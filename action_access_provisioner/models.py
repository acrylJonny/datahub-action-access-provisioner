from pydantic import BaseModel, Field

# Status values emitted by DataHub on actionRequestStatus
REQUEST_STATUS_PENDING = "PENDING"
REQUEST_STATUS_COMPLETED = "COMPLETED"

# Result values emitted by DataHub on actionRequestStatus
REQUEST_RESULT_APPROVED = "ACCEPTED"
REQUEST_RESULT_DENIED = "REJECTED"

# The actionRequestInfo.type value for workflow-form-based requests
ACTION_REQUEST_TYPE_WORKFLOW = "WORKFLOW_FORM_REQUEST"

# Processing-ledger stages — each is claimed exactly once per request URN so that
# a replayed or duplicate event never repeats the associated one-shot side effect.
LEDGER_STAGE_PROVISIONED = "provisioned"
LEDGER_STAGE_APPROVAL_NOTIFIED = "approval_notified"
LEDGER_STAGE_DENIAL_NOTIFIED = "denial_notified"
LEDGER_STAGE_REVOCATION_NOTIFIED = "revocation_notified"
LEDGER_STAGE_MEMBERSHIP_NOTIFIED = "membership_notified"
LEDGER_STAGE_MEMBERSHIP_REMOVAL_NOTIFIED = "membership_removal_notified"

_CORPUSER_PREFIX = "urn:li:corpuser:"


def corpuser_email_from_urn(requestor_urn: str | None) -> str | None:
    """Return the email encoded in a corpuser URN, when the id is itself an email."""
    if not requestor_urn or not requestor_urn.startswith(_CORPUSER_PREFIX):
        return None
    urn_id = requestor_urn[len(_CORPUSER_PREFIX) :]
    return urn_id if "@" in urn_id else None


class FormFieldValues(BaseModel):
    """Extracted values from an ActionWorkflowFormRequest's fields list.

    Carries both the Snowflake and Databricks target slots; only the subset that
    matches the configured backend's form-field IDs will be populated for any
    given request.
    """

    snowflake_database: str | None = None
    snowflake_schema: str | None = None
    snowflake_role: str | None = None
    # Note: the Databricks backend derives its catalog.schema.table target from the
    # dataset entity (not form fields), so no Databricks target lives here. The
    # optional group routes the grant to a Databricks group (group-based access)
    # instead of the requestor's individual identity.
    databricks_group: str | None = None
    access_duration_days: int | None = None
    requestor_email: str | None = None
    # Who the access is for, when that is not the person who raised the request
    # (delegated requests, and service accounts that cannot raise their own).
    requested_for: str | None = None
    justification: str | None = None

    # Raw field map in case callers want non-standard fields
    raw: dict[str, str] = Field(default_factory=dict)


class AccessRequest(BaseModel):
    """Parsed representation of a DataHub ActionRequest for access provisioning."""

    urn: str
    status: str
    result: str | None
    note: str | None

    # From actionRequestInfo
    request_type: str
    # Which workflow definition raised this request. A deployment typically runs
    # many workflows, most of which are not access requests, so the provisioner
    # must be able to tell them apart before acting (see WorkflowFilterConfig).
    workflow_urn: str | None = None
    workflow_name: str | None = None
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

    @property
    def has_access_fields(self) -> bool:
        """True when the form carries a field only an access request would set.

        Used as a content-based guard so that a metadata or revocation workflow
        raised on the same dataset is never mistaken for an access request.
        """
        f = self.form_fields
        return any(
            v is not None
            for v in (
                f.access_duration_days,
                f.databricks_group,
                f.snowflake_role,
                f.snowflake_database,
            )
        )


class GrantRecord(BaseModel):
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


class DatabricksGrantRecord(BaseModel):
    """Tracks a Databricks Unity Catalog grant so it can be revoked later.

    The grantee is a Databricks *principal* (the requestor's email / username),
    not a role — Unity Catalog grants privileges directly to users, groups, or
    service principals.
    """

    action_request_urn: str
    principal: str
    catalog: str
    # 'schema_name' rather than 'schema' to avoid shadowing pydantic BaseModel.schema.
    schema_name: str | None
    table: str | None
    requestor_email: str | None
    granted_at_ms: int
    expires_at_ms: int | None

    @property
    def has_expiry(self) -> bool:
        return self.expires_at_ms is not None


class DatabricksGroupMembershipRecord(BaseModel):
    """Tracks a Databricks group membership grant so it can be removed on expiry.

    Used by the 'membership' group-access model: instead of granting an object to
    a principal, the requestor (``user_email``) is added to ``group_name`` — the
    group already holds the relevant Unity Catalog grants.
    """

    action_request_urn: str
    user_email: str
    group_name: str
    added_at_ms: int
    expires_at_ms: int | None

    @property
    def has_expiry(self) -> bool:
        return self.expires_at_ms is not None


class PendingRequestSummary(BaseModel):
    """Summary of a pending request returned from the DataHub GraphQL search."""

    urn: str
    created_ms: int
    requestor_urn: str | None
    requestor_email: str | None
    resource: str | None
    assigned_users: list[str] = Field(default_factory=list)
    assigned_groups: list[str] = Field(default_factory=list)
