"""Data models for access request state."""

from dataclasses import dataclass, field

# Status values emitted by DataHub on actionRequestStatus
REQUEST_STATUS_PENDING = "PENDING"
REQUEST_STATUS_COMPLETED = "COMPLETED"

# Result values emitted by DataHub on actionRequestStatus
REQUEST_RESULT_APPROVED = "ACCEPTED"
REQUEST_RESULT_DENIED = "REJECTED"

# The actionRequestInfo.type value for workflow-form-based requests
ACTION_REQUEST_TYPE_WORKFLOW = "WORKFLOW_FORM_REQUEST"


@dataclass
class FormFieldValues:
    """Extracted values from an ActionWorkflowFormRequest's fields list."""

    snowflake_database: str | None = None
    snowflake_schema: str | None = None
    snowflake_role: str | None = None
    access_duration_days: int | None = None
    requestor_email: str | None = None
    justification: str | None = None

    # Raw field map in case callers want non-standard fields
    raw: dict[str, str] = field(default_factory=dict)


@dataclass
class AccessRequest:
    """Parsed representation of a DataHub ActionRequest for access provisioning."""

    urn: str
    status: str
    result: str | None
    note: str | None

    # From actionRequestInfo
    request_type: str
    resource: str | None
    requestor_urn: str | None
    created_ms: int | None
    due_date_ms: int | None

    # Parsed form fields
    form_fields: FormFieldValues

    @property
    def is_approved(self) -> bool:
        return self.status == REQUEST_STATUS_COMPLETED and self.result == REQUEST_RESULT_APPROVED

    @property
    def is_denied(self) -> bool:
        return self.status == REQUEST_STATUS_COMPLETED and self.result == REQUEST_RESULT_DENIED

    @property
    def is_pending(self) -> bool:
        return self.status == REQUEST_STATUS_PENDING


@dataclass
class GrantRecord:
    """Tracks a Snowflake grant that was executed so it can be revoked later."""

    action_request_urn: str
    snowflake_role: str
    snowflake_database: str
    snowflake_schema: str | None
    requestor_email: str | None
    granted_at_ms: int
    expires_at_ms: int | None

    @property
    def has_expiry(self) -> bool:
        return self.expires_at_ms is not None


@dataclass
class PendingRequestSummary:
    """Summary of a pending request returned from the DataHub GraphQL search."""

    urn: str
    created_ms: int
    requestor_urn: str | None
    requestor_email: str | None
    resource: str | None
    assigned_users: list[str] = field(default_factory=list)
    assigned_groups: list[str] = field(default_factory=list)
