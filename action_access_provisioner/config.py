from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class SnowflakeConnectionConfig(BaseModel):
    """Snowflake connection parameters.

    Uses snowflake-connector-python directly — no SQLAlchemy required.
    """

    account_id: str = Field(description="Snowflake account identifier (e.g. xy12345.us-east-1)")
    username: str = Field(description="Snowflake username")
    password: str | None = Field(
        default=None, description="Snowflake password (username/password auth)"
    )
    warehouse: str | None = Field(default=None, description="Default warehouse to use")
    role: str | None = Field(
        default=None,
        description="Snowflake role — must have GRANT OPTION on target objects",
    )
    authentication_type: str = Field(
        default="DEFAULT_AUTHENTICATOR",
        description="Snowflake authentication type (DEFAULT_AUTHENTICATOR or KEY_PAIR_AUTHENTICATOR)",
    )
    private_key: str | None = Field(
        default=None,
        description="PEM-encoded RSA private key for key-pair authentication",
    )
    private_key_password: str | None = Field(
        default=None,
        description="Passphrase for the encrypted private key (if applicable)",
    )

    def get_native_connection(self):
        """Return a live snowflake.connector connection."""
        import snowflake.connector  # lazy import — not needed at config-parse time

        kwargs: dict = {
            "account": self.account_id,
            "user": self.username,
        }
        if self.role:
            kwargs["role"] = self.role
        if self.warehouse:
            kwargs["warehouse"] = self.warehouse

        if self.authentication_type == "KEY_PAIR_AUTHENTICATOR":
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            passphrase = self.private_key_password.encode() if self.private_key_password else None
            pem = (self.private_key or "").encode()
            p_key = serialization.load_pem_private_key(
                pem, password=passphrase, backend=default_backend()
            )
            kwargs["private_key"] = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        elif self.password:
            kwargs["password"] = self.password

        return snowflake.connector.connect(**kwargs)


class SmtpConfig(BaseModel):
    """SMTP configuration for sending email notifications.

    Defaults target Resend (https://resend.com); override host/username/port for
    any other SMTP provider.
    """

    host: str = Field(default="smtp.resend.com", description="SMTP server hostname")
    port: int = Field(default=587, description="SMTP server port (587 for TLS, 465 for SSL)")
    username: str = Field(
        default="resend",
        description="SMTP username — Resend uses the literal string 'resend'",
    )
    password: str = Field(description="SMTP password — for Resend this is your API key (re_...)")
    from_address: str | None = Field(
        default=None,
        description=(
            "Sender address. Required for Resend (must be a verified domain sender, e.g. "
            "'DataHub <noreply@yourdomain.com>'); for providers where the username is the "
            "sender email it defaults to the username."
        ),
    )
    use_tls: bool = Field(
        default=True,
        description="Use STARTTLS (port 587). Set False only when using implicit SSL (port 465).",
    )

    @model_validator(mode="after")
    def _require_from_address_for_non_email_username(self) -> "SmtpConfig":
        # Resend's username is the literal "resend", so the From cannot fall back
        # to it — a verified sender address must be supplied explicitly.
        if self.from_address is None and "@" not in self.username:
            raise ValueError(
                f"from_address is required when username ({self.username!r}) is not an email "
                "address — set it to a verified sender (e.g. 'noreply@yourdomain.com')"
            )
        return self

    def get_from_address(self) -> str:
        return self.from_address or self.username


class StateConfig(BaseModel):
    """
    Snowflake tables used to persist provisioning state across runs.

    Because the DataHub executor kills the action after ~5 minutes of idle time,
    all state that must survive across scheduled invocations is stored here rather
    than in memory.

    Defaults to DATAHUB.ACCESS_PROVISIONER — override if the schema does not exist
    or you prefer a different location.
    """

    database: str = Field(
        default="DATAHUB",
        description="Snowflake database that holds the state tables",
    )
    schema_name: str = Field(
        default="ACCESS_PROVISIONER",
        description="Snowflake schema that holds the state tables",
        alias="schema",
    )
    grants_table: str = Field(
        default="ACCESS_PROVISIONER_GRANTS",
        description="Table tracking every provisioned grant (used for idempotency and expiry)",
    )
    sla_table: str = Field(
        default="ACCESS_PROVISIONER_SLA_NOTIFICATIONS",
        description="Table tracking sent SLA notifications (prevents duplicate emails across runs)",
    )
    errors_table: str = Field(
        default="ACCESS_PROVISIONER_ERRORS",
        description=(
            "Table recording permanently-failed provisioning attempts (e.g. role does not exist). "
            "Requests recorded here are skipped on future catchup passes to prevent infinite retries."
        ),
    )

    @property
    def qualified_grants_table(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.grants_table}"

    @property
    def qualified_sla_table(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.sla_table}"

    @property
    def qualified_errors_table(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.errors_table}"


class SlaConfig(BaseModel):
    """SLA monitoring configuration for open access requests."""

    warning_after_hours: int = Field(
        default=24,
        description="Send a reminder to approvers when a request has been pending this many hours",
    )
    escalation_after_hours: int = Field(
        default=72,
        description="Send an escalation email when a request has been pending this many hours",
    )
    escalation_recipients: list[str] = Field(
        default_factory=list,
        description="Email addresses to CC on escalation alerts (e.g. team leads)",
    )


class ExpiryConfig(BaseModel):
    """Access expiry / auto-revocation configuration."""

    enabled: bool = Field(
        default=True,
        description="Whether to auto-revoke Snowflake access when the declared access duration expires",
    )
    revocation_notification: bool = Field(
        default=True,
        description="Send an email to the original requestor when their access is auto-revoked",
    )


class SnowflakeProvisioningConfig(BaseModel):
    """Controls how Snowflake GRANT statements are constructed."""

    default_warehouse: str | None = Field(
        default=None,
        description="Warehouse to grant USAGE on when granting database/schema access",
    )
    dry_run: bool = Field(
        default=False,
        description="Log GRANT/REVOKE statements but do not execute them — useful for testing",
    )
    requestor_username_format: str = Field(
        default="urn_id",
        description=(
            "How to derive a Snowflake username from the DataHub requestor URN when the "
            "snowflake_role form field is absent. "
            "'urn_id' (default): use the identity segment of the URN as-is "
            "(e.g. 'john.doe@company.com' from 'urn:li:corpuser:john.doe@company.com'). "
            "'email_local_part': strip the @domain suffix "
            "(e.g. 'john.doe' from 'urn:li:corpuser:john.doe@company.com'). "
            "The provisioner then calls DESCRIBE USER to find that user's DEFAULT_ROLE."
        ),
    )


class AccessProvisionerConfig(BaseModel):
    """Top-level configuration for the Access Provisioner Action."""

    snowflake_connection: SnowflakeConnectionConfig = Field(
        description="Snowflake connection used to execute GRANT/REVOKE statements"
    )
    state: StateConfig = Field(
        default_factory=StateConfig,
        description=(
            "Snowflake database/schema/table names used to persist grant state and SLA "
            "notifications across scheduled runs. Defaults to DATAHUB.ACCESS_PROVISIONER."
        ),
    )
    smtp: SmtpConfig = Field(description="SMTP configuration for email notifications")
    sla: SlaConfig = Field(
        default_factory=SlaConfig,
        description="SLA monitoring and reminder settings",
    )
    expiry: ExpiryConfig = Field(
        default_factory=ExpiryConfig,
        description="Access expiry / auto-revocation settings",
    )
    lookback_days: int = Field(
        default=90,
        description=(
            "How many days back to scan DataHub for approved requests on each startup catchup pass. "
            "Requests outside this window are assumed to have been handled by a previous run."
        ),
    )
    provisioning: SnowflakeProvisioningConfig = Field(
        default_factory=SnowflakeProvisioningConfig,
        description="Options controlling how Snowflake grants are executed",
    )

    # Form field IDs — these must match the field IDs defined in the DataHub workflow form
    field_snowflake_database: str = Field(
        default="snowflake_database",
        description="Workflow form field ID that holds the target Snowflake database",
    )
    field_snowflake_schema: str = Field(
        default="snowflake_schema",
        description="Workflow form field ID that holds the target Snowflake schema (optional)",
    )
    field_snowflake_role: str = Field(
        default="snowflake_role",
        description="Workflow form field ID that holds the Snowflake role to be granted",
    )
    field_access_duration_days: str = Field(
        default="access_duration_days",
        description="Workflow form field ID that holds the requested access duration in days",
    )
    field_requestor_email: str = Field(
        default="requestor_email",
        description="Workflow form field ID that holds the requestor's email address",
    )
    field_justification: str = Field(
        default="justification",
        description="Workflow form field ID that holds the business justification",
    )


# ===========================================================================
# Databricks backend
# ===========================================================================


class DatabricksConnectionConfig(BaseModel):
    """Databricks workspace connection parameters.

    Supports two auth methods:
      - Personal access token (PAT): set ``token``.
      - OAuth machine-to-machine (service principal): set ``client_id`` +
        ``client_secret``.

    A SQL warehouse (``http_path``) is always required: state/log tables are
    Delta tables, and (when ``grant_method: sql``) GRANT/REVOKE statements run
    through the warehouse too.
    """

    host: str = Field(
        description="Workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com",
    )
    http_path: str = Field(
        description=(
            "SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/abc123). Required for the "
            "Delta state/log tables and for SQL-based GRANT/REVOKE."
        ),
    )
    token: str | None = Field(
        default=None,
        description="Personal access token (PAT auth)",
    )
    client_id: str | None = Field(
        default=None,
        description="OAuth service-principal client ID (M2M auth)",
    )
    client_secret: str | None = Field(
        default=None,
        description="OAuth service-principal client secret (M2M auth)",
    )

    @model_validator(mode="after")
    def _require_auth(self) -> "DatabricksConnectionConfig":
        has_pat = bool(self.token)
        has_oauth = bool(self.client_id and self.client_secret)
        if not (has_pat or has_oauth):
            raise ValueError(
                "Databricks connection requires either 'token' (PAT) or "
                "'client_id' + 'client_secret' (OAuth service principal)."
            )
        return self

    @property
    def server_hostname(self) -> str:
        """Bare hostname (no scheme / trailing slash) for the SQL connector."""
        return self.host.replace("https://", "").replace("http://", "").rstrip("/")

    def get_sql_connection(self):
        """Return a live databricks-sql-connector connection."""
        from databricks import sql  # lazy import — only needed at runtime

        # The DataHub Cloud executor pins databricks-sql-connector==2.9.6, which
        # only supports pyformat (%(name)s) parameter markers — it has no native
        # ":name" binding, so those markers reach the server unbound. Force
        # pyformat so our state-table queries bind correctly here and on newer
        # connectors (3.x/4.x default to the "named" paramstyle).
        sql.paramstyle = "pyformat"

        if self.token:
            return sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.token,
            )

        # OAuth M2M (service principal) — mint a credentials provider via the SDK.
        from databricks.sdk.core import Config, oauth_service_principal

        def _credentials_provider():
            return oauth_service_principal(
                Config(
                    host=f"https://{self.server_hostname}",
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                )
            )

        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            credentials_provider=_credentials_provider,
        )

    def get_workspace_client(self):
        """Return a databricks-sdk WorkspaceClient (used when grant_method='sdk')."""
        from databricks.sdk import WorkspaceClient

        if self.token:
            return WorkspaceClient(host=f"https://{self.server_hostname}", token=self.token)
        return WorkspaceClient(
            host=f"https://{self.server_hostname}",
            client_id=self.client_id,
            client_secret=self.client_secret,
        )


class DatabricksStateConfig(BaseModel):
    """Unity Catalog location for the Delta state/log tables.

    Defaults to ``datahub.access_provisioner``. The catalog and schema must
    already exist (the provisioner role needs CREATE TABLE on the schema); the
    tables are created on first run.
    """

    catalog: str = Field(
        default="datahub",
        description="Unity Catalog catalog that holds the state/log tables",
    )
    schema_name: str = Field(
        default="access_provisioner",
        description="Schema that holds the state/log tables",
        alias="schema",
    )
    grants_table: str = Field(
        default="access_provisioner_grants",
        description="Delta table tracking every provisioned grant (idempotency + expiry)",
    )
    sla_table: str = Field(
        default="access_provisioner_sla_notifications",
        description="Delta table tracking sent SLA notifications (dedup across runs)",
    )
    errors_table: str = Field(
        default="access_provisioner_errors",
        description="Delta table recording permanently-failed provisioning attempts",
    )
    memberships_table: str = Field(
        default="access_provisioner_group_memberships",
        description="Delta table tracking group-membership grants (membership access model)",
    )

    @property
    def qualified_grants_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema_name}`.`{self.grants_table}`"

    @property
    def qualified_sla_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema_name}`.`{self.sla_table}`"

    @property
    def qualified_errors_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema_name}`.`{self.errors_table}`"

    @property
    def qualified_memberships_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema_name}`.`{self.memberships_table}`"


class TicketProvider(str, Enum):
    JIRA = "jira"
    SERVICENOW = "servicenow"


class TicketingMode(str, Enum):
    AUGMENT = "augment"
    REPLACE = "replace"


class TicketingConfig(BaseModel):
    """Optional Jira / ServiceNow ticketing target.

    Some organisations fulfil access through their ITSM tool rather than (or in
    addition to) a direct grant. When configured, the action opens a ticket on
    approval:

      - ``mode: augment`` — grant access *and* file a ticket (audit / hand-off).
      - ``mode: replace`` — do not grant; only file a ticket for a human to fulfil.
    """

    provider: TicketProvider = Field(
        description="Ticketing system to open access tickets in",
    )
    mode: TicketingMode = Field(
        default=TicketingMode.AUGMENT,
        description=(
            "'augment': grant access and also open a ticket. "
            "'replace': skip the grant and only open a ticket for manual fulfilment."
        ),
    )
    base_url: str = Field(
        description=(
            "Base URL of the ticketing instance, e.g. https://yourco.atlassian.net "
            "(Jira) or https://yourco.service-now.com (ServiceNow)."
        ),
    )
    username: str = Field(
        description="Auth user — the account email for Jira, the username for ServiceNow",
    )
    api_token: str = Field(
        description="API token (Jira) or password (ServiceNow) for basic auth",
    )
    jira_project_key: str | None = Field(
        default=None,
        description="Jira project key the issue is created under (required for provider='jira')",
    )
    jira_issue_type: str = Field(
        default="Task",
        description="Jira issue type name to create",
    )
    servicenow_table: str = Field(
        default="incident",
        description="ServiceNow table to insert the record into (e.g. 'incident', 'sc_request')",
    )
    dry_run: bool = Field(
        default=False,
        description="Log the ticket payload but do not call the ticketing API",
    )

    @model_validator(mode="after")
    def _require_provider_fields(self) -> "TicketingConfig":
        if self.provider == TicketProvider.JIRA and not self.jira_project_key:
            raise ValueError("jira_project_key is required when provider='jira'")
        return self

    @property
    def base_url_clean(self) -> str:
        return self.base_url.rstrip("/")


class GroupAccessMode(str, Enum):
    GRANT = "grant"
    MEMBERSHIP = "membership"


class DatabricksProvisioningConfig(BaseModel):
    """Controls how Databricks GRANT/REVOKE statements are executed."""

    grant_method: Literal["sql", "sdk"] = Field(
        default="sql",
        description=(
            "How to apply grants: 'sql' runs GRANT/REVOKE through the SQL warehouse; "
            "'sdk' uses the Unity Catalog grants API (databricks-sdk)."
        ),
    )
    group_access_mode: GroupAccessMode = Field(
        default=GroupAccessMode.GRANT,
        description=(
            "How a requested group is honoured. 'grant': GRANT the object to the group. "
            "'membership': add the requestor as a member of the group (the group already "
            "holds the grants) — added/removed via the SDK and reconcilable with the IdP. "
            "Only applies when the request supplies a group; otherwise access is granted to "
            "the requestor's own identity."
        ),
    )
    dry_run: bool = Field(
        default=False,
        description="Log GRANT/REVOKE without executing them — useful for testing",
    )


class DatahubSyncConfig(BaseModel):
    """Mirror granted Databricks access into DataHub for auditing.

    When enabled, every group grant / membership change the action makes is also
    written back to DataHub as ``role`` + ``actors`` + dataset ``access`` aspects,
    giving a queryable "who has access" view. This is a strictly read-only mirror of
    Databricks — editing DataHub never mutates Unity Catalog.
    """

    enabled: bool = Field(
        default=False,
        description=(
            "Mirror group grants and memberships into DataHub as role/actors/access "
            "aspects. Off by default; turn on once you want the audit view."
        ),
    )
    role_urn_prefix: str = Field(
        default="databricks",
        description=(
            "Prefix for minted role URNs. A Databricks group 'analytics' becomes "
            "urn:li:role:<prefix>.analytics."
        ),
    )
    platform: str = Field(
        default="databricks",
        description="Data platform used when building dataset URNs for access associations",
    )
    env: str = Field(
        default="PROD",
        description="Fabric/env used when building dataset URNs (must match how datasets were ingested)",
    )
    request_url: str | None = Field(
        default=None,
        description="Optional link surfaced on roleProperties.requestUrl (e.g. the access-request workflow)",
    )


class DatabricksAccessProvisionerConfig(BaseModel):
    """Top-level configuration for the Databricks Access Provisioner Action."""

    databricks_connection: DatabricksConnectionConfig = Field(
        description="Databricks workspace connection used to execute grants and store state"
    )
    state: DatabricksStateConfig = Field(
        default_factory=DatabricksStateConfig,
        description="Unity Catalog location for the Delta state/log tables",
    )
    smtp: SmtpConfig = Field(description="SMTP configuration for email notifications")
    sla: SlaConfig = Field(
        default_factory=SlaConfig,
        description="SLA monitoring and reminder settings",
    )
    expiry: ExpiryConfig = Field(
        default_factory=ExpiryConfig,
        description="Access expiry / auto-revocation settings",
    )
    lookback_days: int = Field(
        default=90,
        description="How many days back to scan DataHub for approved requests on each startup pass",
    )
    provisioning: DatabricksProvisioningConfig = Field(
        default_factory=DatabricksProvisioningConfig,
        description="Options controlling how Databricks grants are executed",
    )
    ticketing: TicketingConfig | None = Field(
        default=None,
        description=(
            "Optional Jira/ServiceNow ticketing target. When set, a ticket is opened on "
            "approval (in addition to, or instead of, the grant — see ticketing.mode)."
        ),
    )
    datahub_sync: DatahubSyncConfig = Field(
        default_factory=DatahubSyncConfig,
        description=(
            "Mirror granted access back into DataHub as role/actors/access aspects for "
            "auditing. Off by default."
        ),
    )

    # Form field IDs — must match the field IDs defined in the DataHub workflow form.
    # The grant target (catalog.schema.table) is derived from the dataset entity the
    # request is raised on, so it is intentionally NOT a form field.
    field_access_duration_days: str = Field(
        default="access_duration_days",
        description="Workflow form field ID that holds the requested access duration in days",
    )
    field_justification: str = Field(
        default="justification",
        description="Workflow form field ID that holds the business justification",
    )
    field_databricks_group: str = Field(
        default="databricks_group",
        description=(
            "Workflow form field ID that holds an optional Databricks group name. When the "
            "form supplies a value, access is granted to that group (group-based access) "
            "rather than to the requestor's individual identity."
        ),
    )
