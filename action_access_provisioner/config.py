from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

# Snowflake application tag (shows up in QUERY_HISTORY, mirrors the ingestion connector).
_SNOWFLAKE_APPLICATION_NAME = "acryl_datahub_access_provisioner"

# The authentication types the Snowflake connector accepts, matching the DataHub
# Snowflake ingestion connector (metadata-ingestion/.../snowflake_connection.py).
_SNOWFLAKE_VALID_AUTH_TYPES = {
    "DEFAULT_AUTHENTICATOR",
    "EXTERNAL_BROWSER_AUTHENTICATOR",
    "KEY_PAIR_AUTHENTICATOR",
    "OAUTH_AUTHENTICATOR",
    "OAUTH_AUTHENTICATOR_TOKEN",
}


class OAuthIdentityProvider(str, Enum):
    """OAuth identity providers supported for ``OAUTH_AUTHENTICATOR`` (mirrors ingestion)."""

    MICROSOFT = "microsoft"
    OKTA = "okta"


class OAuthConfiguration(BaseModel):
    """OAuth settings for Snowflake ``OAUTH_AUTHENTICATOR``.

    Mirrors the ingestion connector's ``OAuthConfiguration`` so the same recipe
    values work here. DataHub fetches a token from the IdP (Microsoft/Okta) and
    connects to Snowflake with ``authenticator=oauth``.
    """

    provider: OAuthIdentityProvider = Field(
        description="OAuth identity provider — 'microsoft' or 'okta'"
    )
    authority_url: str = Field(description="OAuth token endpoint / authority URL")
    client_id: str = Field(description="OAuth client (application) ID")
    scopes: list[str] = Field(description="OAuth scopes to request")
    use_certificate: bool = Field(
        default=False,
        description="Use a client certificate instead of a secret (Microsoft only)",
    )
    client_secret: str | None = Field(
        default=None,
        description="OAuth client secret (required unless use_certificate=True)",
    )
    encoded_oauth_public_key: str | None = Field(
        default=None,
        description="Base64-encoded public certificate (required when use_certificate=True)",
    )
    encoded_oauth_private_key: str | None = Field(
        default=None,
        description="Base64-encoded private key (required when use_certificate=True)",
    )

    @model_validator(mode="after")
    def _validate(self) -> "OAuthConfiguration":
        if self.use_certificate:
            if self.provider != OAuthIdentityProvider.MICROSOFT:
                raise ValueError("Certificate authentication is only supported for Microsoft")
            if not self.encoded_oauth_public_key or not self.encoded_oauth_private_key:
                raise ValueError(
                    "encoded_oauth_public_key and encoded_oauth_private_key are required "
                    "when use_certificate=True"
                )
        elif not self.client_secret:
            raise ValueError("client_secret is required when use_certificate=False")
        return self


class SnowflakeConnectionConfig(BaseModel):
    """Snowflake connection parameters.

    Uses snowflake-connector-python directly — no SQLAlchemy required. Supports the
    same authentication mechanisms as the DataHub Snowflake ingestion connector:
    ``DEFAULT_AUTHENTICATOR`` (user/password), ``KEY_PAIR_AUTHENTICATOR``,
    ``OAUTH_AUTHENTICATOR`` (Microsoft/Okta), ``OAUTH_AUTHENTICATOR_TOKEN`` (a
    pre-supplied token) and ``EXTERNAL_BROWSER_AUTHENTICATOR`` (interactive; local/dev
    only — it cannot complete on the headless remote executor).
    """

    account_id: str = Field(description="Snowflake account identifier (e.g. xy12345.us-east-1)")
    username: str | None = Field(
        default=None,
        description="Snowflake username (required for every auth type except a raw OAuth token)",
    )
    password: str | None = Field(
        default=None,
        description="Snowflake password (DEFAULT/external-browser/Okta password grant)",
    )
    warehouse: str | None = Field(default=None, description="Default warehouse to use")
    role: str | None = Field(
        default=None,
        description="Snowflake role — must have GRANT OPTION on target objects",
    )
    authentication_type: str = Field(
        default="DEFAULT_AUTHENTICATOR",
        description=(
            "Authenticator to use: DEFAULT_AUTHENTICATOR, KEY_PAIR_AUTHENTICATOR, "
            "OAUTH_AUTHENTICATOR, OAUTH_AUTHENTICATOR_TOKEN or EXTERNAL_BROWSER_AUTHENTICATOR."
        ),
    )
    private_key: str | None = Field(
        default=None,
        description="PEM-encoded RSA private key for key-pair authentication (inline)",
    )
    private_key_path: str | None = Field(
        default=None,
        description="Path to a PEM private key file (alternative to private_key)",
    )
    private_key_password: str | None = Field(
        default=None,
        description="Passphrase for the encrypted private key (if applicable)",
    )
    oauth_config: OAuthConfiguration | None = Field(
        default=None,
        description="OAuth settings, required when authentication_type=OAUTH_AUTHENTICATOR",
    )
    token: str | None = Field(
        default=None,
        description="Pre-supplied OAuth token, used only with OAUTH_AUTHENTICATOR_TOKEN",
    )
    snowflake_domain: str = Field(
        default="snowflakecomputing.com",
        description="Snowflake domain suffix (use 'snowflakecomputing.cn' for China regions)",
    )
    connect_args: dict[str, Any] | None = Field(
        default=None,
        description="Extra keyword arguments passed verbatim to snowflake.connector.connect",
    )

    @model_validator(mode="after")
    def _validate_auth(self) -> "SnowflakeConnectionConfig":
        auth = self.authentication_type
        if auth not in _SNOWFLAKE_VALID_AUTH_TYPES:
            raise ValueError(
                f"Unsupported authentication_type {auth!r}. "
                f"Supported: {sorted(_SNOWFLAKE_VALID_AUTH_TYPES)}"
            )
        has_key = bool(self.private_key or self.private_key_path)
        if has_key and auth != "KEY_PAIR_AUTHENTICATOR":
            raise ValueError(
                "private_key / private_key_path are only valid with KEY_PAIR_AUTHENTICATOR"
            )
        if auth == "KEY_PAIR_AUTHENTICATOR" and not has_key:
            raise ValueError(
                "KEY_PAIR_AUTHENTICATOR requires private_key or private_key_path to be set"
            )
        if auth == "OAUTH_AUTHENTICATOR" and self.oauth_config is None:
            raise ValueError("OAUTH_AUTHENTICATOR requires oauth_config to be set")
        if self.token and auth != "OAUTH_AUTHENTICATOR_TOKEN":
            raise ValueError("token is only valid with OAUTH_AUTHENTICATOR_TOKEN")
        if auth == "OAUTH_AUTHENTICATOR_TOKEN" and not self.token:
            raise ValueError("OAUTH_AUTHENTICATOR_TOKEN requires token to be set")
        return self

    def _load_private_key_der(self) -> bytes:
        """Load the PEM private key (inline or from file) and return DER/PKCS8 bytes."""
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        if self.private_key:
            pem = self.private_key
            # DataHub ${ENV} injection often flattens PEM newlines to literal "\n";
            # restore them so the key parses. ponytail: only when no real newline present.
            if "\\n" in pem and "\n" not in pem:
                pem = pem.replace("\\n", "\n")
            pem_bytes = pem.encode()
        elif self.private_key_path:
            with open(self.private_key_path, "rb") as fh:
                pem_bytes = fh.read()
        else:
            raise ValueError("No private key configured for KEY_PAIR_AUTHENTICATOR")

        passphrase = self.private_key_password.encode() if self.private_key_password else None
        p_key = serialization.load_pem_private_key(
            pem_bytes, password=passphrase, backend=default_backend()
        )
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def get_native_connection(self):
        """Return a live snowflake.connector connection for the configured auth type."""
        import snowflake.connector  # lazy import — not needed at config-parse time

        base: dict[str, Any] = {
            "user": self.username,
            "account": self.account_id,
            "host": f"{self.account_id}.{self.snowflake_domain}",
            "application": _SNOWFLAKE_APPLICATION_NAME,
        }
        if self.role:
            base["role"] = self.role
        if self.warehouse:
            base["warehouse"] = self.warehouse
        extra = dict(self.connect_args or {})
        auth = self.authentication_type

        if auth == "DEFAULT_AUTHENTICATOR":
            return snowflake.connector.connect(password=self.password, **base, **extra)
        if auth == "EXTERNAL_BROWSER_AUTHENTICATOR":
            return snowflake.connector.connect(
                authenticator="externalbrowser", password=self.password, **base, **extra
            )
        if auth == "KEY_PAIR_AUTHENTICATOR":
            return snowflake.connector.connect(
                private_key=self._load_private_key_der(), **base, **extra
            )
        if auth == "OAUTH_AUTHENTICATOR_TOKEN":
            return snowflake.connector.connect(
                authenticator="oauth", token=self.token, **base, **extra
            )
        # OAUTH_AUTHENTICATOR — fetch a token from the IdP, then connect with it.
        from action_access_provisioner.snowflake_oauth import generate_oauth_token

        assert self.oauth_config is not None  # guaranteed by _validate_auth
        token = generate_oauth_token(
            self.oauth_config, username=self.username, password=self.password
        )
        return snowflake.connector.connect(authenticator="oauth", token=token, **base, **extra)


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
    ledger_table: str = Field(
        default="ACCESS_PROVISIONER_LEDGER",
        description=(
            "Processing ledger keyed by (request URN, stage). Guarantees exactly-once "
            "side effects — a stage is claimed before its notification is sent so duplicate "
            "or replayed events never send a second approval/denial/revocation email."
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

    @property
    def qualified_ledger_table(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.ledger_table}"


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


class ReconcileConfig(BaseModel):
    """Background reconciliation loop settings.

    The action processes live ``actionRequestStatus`` events as they arrive, but
    event delivery is best-effort: an approval that lands while the process is
    down, mid-restart, or during a consumer rebalance can be missed. The startup
    catchup pass only runs once per process, so a long-lived daemon would not
    re-scan until it restarts — which is how approvals end up delayed by days.

    This loop re-runs the full catchup/reconcile pass on a fixed interval so the
    worst-case delay for a missed event is bounded by ``interval_seconds`` rather
    than by the next process restart. Every pass is idempotent (state tables +
    the processing ledger), so re-scanning never re-grants or re-notifies.
    """

    enabled: bool = Field(
        default=True,
        description=(
            "Run a background reconciliation loop that periodically re-scans for approved, "
            "expired, and pending requests. Strongly recommended for long-lived daemon "
            "deployments so missed live events are still processed promptly."
        ),
    )
    interval_seconds: int = Field(
        default=300,
        ge=30,
        description=(
            "How often the background reconciliation loop runs, in seconds (minimum 30). "
            "Bounds the worst-case delay for a missed live event."
        ),
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
    smtp: SmtpConfig | None = Field(
        default=None,
        description=(
            "SMTP configuration for email notifications. Optional — omit to disable all "
            "email notifications (approvals, denials, SLA reminders, revocations)."
        ),
    )
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
    reconcile: ReconcileConfig = Field(
        default_factory=ReconcileConfig,
        description="Background reconciliation loop settings (bounds delay for missed live events)",
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


class AzureAuthConfig(BaseModel):
    """Azure AD service-principal auth for Azure Databricks (mirrors the ingestion connector)."""

    client_id: str = Field(description="Azure AD application (client) ID")
    tenant_id: str = Field(description="Azure AD tenant ID")
    client_secret: str = Field(description="Azure AD client secret")


class DatabricksConnectionConfig(BaseModel):
    """Databricks workspace connection parameters.

    Supports the same authentication mechanisms as the DataHub Unity Catalog
    ingestion connector, mutually exclusive:

      - **PAT** — set ``token``.
      - **OAuth M2M (service principal)** — set ``client_id`` + ``client_secret``.
      - **Azure AD service principal** — set ``azure_auth`` (Azure Databricks).
      - **Unified auth** — set none of the above; the Databricks SDK resolves
        credentials from its default chain (env vars / config profile).

    A SQL warehouse (``http_path`` or ``warehouse_id``) is always required: the
    Delta state/log tables live there, and (with ``grant_method: sql``) GRANT/REVOKE
    run through it too. All auth flows through the SDK ``WorkspaceClient`` credential
    chain, so both the SQL warehouse and the Unity Catalog grants API share one
    resolved credential.
    """

    host: str = Field(
        description="Workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com",
    )
    http_path: str | None = Field(
        default=None,
        description=(
            "SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/abc123). Required unless "
            "warehouse_id is set. Used for the Delta state/log tables and SQL-based GRANT/REVOKE."
        ),
    )
    warehouse_id: str | None = Field(
        default=None,
        description="SQL warehouse ID (http_path is derived from it when http_path is unset)",
    )
    token: str | None = Field(
        default=None,
        description="Personal access token (PAT auth)",
    )
    client_id: str | None = Field(
        default=None,
        description="OAuth service-principal client ID (Databricks M2M auth)",
    )
    client_secret: str | None = Field(
        default=None,
        description="OAuth service-principal client secret (Databricks M2M auth)",
    )
    azure_auth: AzureAuthConfig | None = Field(
        default=None,
        description="Azure AD service-principal auth (for Azure Databricks workspaces)",
    )

    @model_validator(mode="after")
    def _validate_auth(self) -> "DatabricksConnectionConfig":
        methods = sum(
            [
                bool(self.token),
                bool(self.azure_auth),
                bool(self.client_id or self.client_secret),
            ]
        )
        if methods > 1:
            raise ValueError(
                "Provide only one Databricks auth method: 'token' (PAT), 'azure_auth', "
                "or 'client_id'/'client_secret' (OAuth M2M). Leave all unset for unified auth."
            )
        if bool(self.client_id) != bool(self.client_secret):
            raise ValueError("Databricks OAuth M2M requires both 'client_id' and 'client_secret'.")
        if not self.http_path and not self.warehouse_id:
            raise ValueError("Databricks connection requires either 'http_path' or 'warehouse_id'.")
        return self

    @property
    def server_hostname(self) -> str:
        """Bare hostname (no scheme / trailing slash) for the SQL connector."""
        return self.host.replace("https://", "").replace("http://", "").rstrip("/")

    @property
    def resolved_http_path(self) -> str:
        """The SQL-warehouse HTTP path, derived from warehouse_id when not given explicitly."""
        if self.http_path:
            return self.http_path
        return f"/sql/1.0/warehouses/{self.warehouse_id}"

    def get_workspace_client(self):
        """Return a databricks-sdk WorkspaceClient for the configured auth method.

        The SDK's credential chain covers PAT, Databricks OAuth M2M, Azure AD service
        principal, and unified auth (env/profile) — so passing whichever fields are set
        (or none) selects the right mechanism automatically.
        """
        from databricks.sdk import WorkspaceClient

        kwargs: dict[str, Any] = {"host": f"https://{self.server_hostname}"}
        if self.token:
            kwargs["token"] = self.token
        if self.client_id:
            kwargs["client_id"] = self.client_id
        if self.client_secret:
            kwargs["client_secret"] = self.client_secret
        if self.azure_auth:
            kwargs["azure_tenant_id"] = self.azure_auth.tenant_id
            kwargs["azure_client_id"] = self.azure_auth.client_id
            kwargs["azure_client_secret"] = self.azure_auth.client_secret

        client = WorkspaceClient(**kwargs)
        if self.warehouse_id:
            client.config.warehouse_id = self.warehouse_id
        return client

    def get_sql_connection(self):
        """Return a live databricks-sql-connector connection.

        Auth is delegated to the WorkspaceClient credential chain (the same object used
        for the grants API and SCIM), so every supported auth method — PAT, OAuth M2M,
        Azure AD, unified — works for the SQL warehouse too. Mirrors the ingestion
        connector's ``get_sql_connection_params``.
        """
        from databricks import sql  # lazy import — only needed at runtime

        # The DataHub Cloud executor pins databricks-sql-connector==2.9.6, which
        # only supports pyformat (%(name)s) parameter markers — it has no native
        # ":name" binding, so those markers reach the server unbound. Force
        # pyformat so our state-table queries bind correctly here and on newer
        # connectors (3.x/4.x default to the "named" paramstyle).
        sql.paramstyle = "pyformat"

        workspace_client = self.get_workspace_client()
        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.resolved_http_path,
            credentials_provider=lambda: workspace_client.config.authenticate,
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
    ledger_table: str = Field(
        default="access_provisioner_ledger",
        description=(
            "Processing ledger keyed by (request URN, stage). Guarantees exactly-once "
            "side effects — a stage is claimed before its notification is sent so duplicate "
            "or replayed events never send a second approval/denial/revocation email."
        ),
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

    @property
    def qualified_ledger_table(self) -> str:
        return f"`{self.catalog}`.`{self.schema_name}`.`{self.ledger_table}`"


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


class DatabricksIdentityConfig(BaseModel):
    """How a DataHub requestor is mapped to a Databricks principal (user email).

    By default the requestor's corpuser URN id is used when it is itself an email
    (``urn:li:corpuser:jane@corp.com`` -> ``jane@corp.com``). When DataHub usernames
    are not emails (common with SSO), the requestor's email is looked up from their
    DataHub corpuser profile, and an explicit override map takes precedence over both.
    """

    principal_overrides: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Explicit map from a DataHub corpuser (full URN or bare id) to a Databricks "
            "principal (email). Checked first, before any automatic resolution."
        ),
    )
    resolve_email_from_datahub: bool = Field(
        default=True,
        description=(
            "When the corpuser URN id is not itself an email, look up the user's email "
            "from their DataHub corpuser profile (corpUserInfo / editableInfo)."
        ),
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
    smtp: SmtpConfig | None = Field(
        default=None,
        description=(
            "SMTP configuration for email notifications. Optional — omit to disable all "
            "email notifications (approvals, denials, SLA reminders, revocations)."
        ),
    )
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
    reconcile: ReconcileConfig = Field(
        default_factory=ReconcileConfig,
        description="Background reconciliation loop settings (bounds delay for missed live events)",
    )
    provisioning: DatabricksProvisioningConfig = Field(
        default_factory=DatabricksProvisioningConfig,
        description="Options controlling how Databricks grants are executed",
    )
    identity: DatabricksIdentityConfig = Field(
        default_factory=DatabricksIdentityConfig,
        description="How DataHub requestors are mapped to Databricks principals",
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
