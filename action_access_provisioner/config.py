"""Configuration models for the access provisioner action."""

from typing import Optional

from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from pydantic import BaseModel, Field


class SmtpConfig(BaseModel):
    """Gmail SMTP configuration for sending email notifications."""

    host: str = Field(default="smtp.gmail.com", description="SMTP server hostname")
    port: int = Field(default=587, description="SMTP server port (587 for TLS, 465 for SSL)")
    username: str = Field(description="Gmail address used to send emails")
    password: str = Field(
        description="Gmail App Password (generate at myaccount.google.com/apppasswords)"
    )
    from_address: Optional[str] = Field(
        default=None,
        description="Sender display address — defaults to username if not set",
    )
    use_tls: bool = Field(
        default=True,
        description="Use STARTTLS (port 587). Set False only when using implicit SSL (port 465).",
    )

    def get_from_address(self) -> str:
        return self.from_address or self.username


class StateConfig(BaseModel):
    """
    Snowflake tables used to persist provisioning state across runs.

    Because the DataHub executor kills the action after ~5 minutes of idle time,
    all state that must survive across scheduled invocations is stored here rather
    than in memory.
    """

    database: str = Field(description="Snowflake database that holds the state tables")
    schema_name: str = Field(
        description="Snowflake schema that holds the state tables", alias="schema"
    )
    grants_table: str = Field(
        default="ACCESS_PROVISIONER_GRANTS",
        description="Table tracking every provisioned grant (used for idempotency and expiry)",
    )
    sla_table: str = Field(
        default="ACCESS_PROVISIONER_SLA_NOTIFICATIONS",
        description="Table tracking sent SLA notifications (prevents duplicate emails across runs)",
    )

    @property
    def qualified_grants_table(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.grants_table}"

    @property
    def qualified_sla_table(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.sla_table}"


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

    default_warehouse: Optional[str] = Field(
        default=None,
        description="Warehouse to grant USAGE on when granting database/schema access",
    )
    dry_run: bool = Field(
        default=False,
        description="Log GRANT/REVOKE statements but do not execute them — useful for testing",
    )


class AccessProvisionerConfig(BaseModel):
    """Top-level configuration for the Access Provisioner Action."""

    snowflake_connection: SnowflakeConnectionConfig = Field(
        description="Snowflake connection used to execute GRANT/REVOKE statements"
    )
    state: StateConfig = Field(
        description=(
            "Snowflake database/schema/table names used to persist grant state and SLA "
            "notifications across scheduled runs"
        )
    )
    smtp: SmtpConfig = Field(description="Gmail SMTP configuration for email notifications")
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
