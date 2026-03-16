"""Pydantic models representing DataHub GraphQL response shapes.

These models mirror the exact JSON structure returned by the GraphQL API so that
callers work with typed attribute access rather than raw dict.get() chains.

Conversion methods (to_access_request, to_pending_summary) translate the API
response into the domain models defined in models.py.
"""

import logging

from pydantic import BaseModel, Field

from action_access_provisioner.models import AccessRequest, FormFieldValues, PendingRequestSummary

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Primitive / shared shapes
# ---------------------------------------------------------------------------


class GqlActor(BaseModel):
    """A resolved user reference returned inside audit stamps."""

    urn: str


class GqlAuditStamp(BaseModel):
    """Timestamp + actor for created / lastModified fields."""

    time: int
    actor: GqlActor | None = None


class GqlEntity(BaseModel):
    """Minimal entity reference (only the URN is selected in our queries)."""

    urn: str


# ---------------------------------------------------------------------------
# Form field value union
# ---------------------------------------------------------------------------


class GqlFieldValue(BaseModel):
    """Union of StringValue / NumberValue from the ActionWorkflowFormRequestField.

    GraphQL returns one of:
      {"stringValue": "PROD"}
      {"numberValue": 30.0}

    Both are modelled as optional fields on the same Pydantic type so that
    model_validate works without a discriminator.
    """

    stringValue: str | None = None
    numberValue: float | None = None

    def as_str(self) -> str | None:
        """Return the value as a string, preferring stringValue over numberValue.

        Whole-number floats (e.g. 30.0) are rendered without the decimal suffix ("30").
        """
        if self.stringValue is not None:
            return self.stringValue
        if self.numberValue is not None:
            n = self.numberValue
            return str(int(n)) if n == int(n) else str(n)
        return None


class GqlFormField(BaseModel):
    """A single field from an ActionWorkflowFormRequest."""

    id: str
    values: list[GqlFieldValue] = Field(default_factory=list)

    def first_value(self) -> str | None:
        """Return the string representation of the first non-null value."""
        for v in self.values:
            s = v.as_str()
            if s is not None:
                return s
        return None


# ---------------------------------------------------------------------------
# Workflow form request
# ---------------------------------------------------------------------------


class GqlAccessWorkflowRequest(BaseModel):
    expiresAt: int | None = None


class GqlWorkflowFormRequest(BaseModel):
    fields: list[GqlFormField] = Field(default_factory=list)
    access: GqlAccessWorkflowRequest | None = None

    def to_form_field_values(self, config_field_ids: dict[str, str]) -> FormFieldValues:
        """Map form fields to a typed FormFieldValues dataclass.

        config_field_ids maps config keys (e.g. "field_snowflake_database") to the
        actual field IDs used in the workflow form (e.g. "snowflake_database").
        """
        raw: dict[str, str] = {
            f.id: val for f in self.fields if (val := f.first_value()) is not None
        }

        def _get(config_key: str) -> str | None:
            field_id = config_field_ids.get(config_key)
            return raw.get(field_id) if field_id else None

        duration_str = _get("field_access_duration_days")
        duration_int: int | None = None
        if duration_str is not None:
            try:
                duration_int = int(float(duration_str))
            except ValueError:
                logger.warning(f"Could not parse access_duration_days value: '{duration_str}'")

        return FormFieldValues(
            snowflake_database=_get("field_snowflake_database"),
            snowflake_schema=_get("field_snowflake_schema"),
            snowflake_role=_get("field_snowflake_role"),
            access_duration_days=duration_int,
            requestor_email=_get("field_requestor_email"),
            justification=_get("field_justification"),
            raw=raw,
        )


class GqlActionRequestParams(BaseModel):
    workflowFormRequest: GqlWorkflowFormRequest | None = None


# ---------------------------------------------------------------------------
# ActionRequest node
# ---------------------------------------------------------------------------


class GqlActionRequest(BaseModel):
    """An ActionRequest as returned by actionRequest() or listActionRequests().

    All fields are top-level — there is no actionRequestInfo / actionRequestStatus wrapper.
    assignedUsers / assignedGroups are plain string lists ([String!]) in this schema.
    """

    urn: str
    type: str  # ActionRequestType enum value
    status: str  # ActionRequestStatus enum value
    result: str | None = None  # ActionRequestResult enum value (ACCEPTED / REJECTED)
    resultNote: str | None = None
    entity: GqlEntity | None = None
    assignedUsers: list[str] = Field(default_factory=list)
    assignedGroups: list[str] = Field(default_factory=list)
    created: GqlAuditStamp
    dueDate: int | None = None
    params: GqlActionRequestParams | None = None

    def _form_fields(self, config_field_ids: dict[str, str]) -> FormFieldValues:
        wf = self.params.workflowFormRequest if self.params else None
        if wf:
            return wf.to_form_field_values(config_field_ids)
        return FormFieldValues()

    def to_access_request(self, config_field_ids: dict[str, str]) -> AccessRequest:
        """Convert to the domain AccessRequest dataclass."""
        return AccessRequest(
            urn=self.urn,
            status=self.status,
            result=self.result,
            note=self.resultNote,
            request_type=self.type,
            resource=self.entity.urn if self.entity else None,
            requestor_urn=self.created.actor.urn if self.created.actor else None,
            created_ms=self.created.time,
            due_date_ms=self.dueDate,
            form_fields=self._form_fields(config_field_ids),
        )

    def to_pending_summary(self, config_field_ids: dict[str, str]) -> PendingRequestSummary:
        """Convert to the domain PendingRequestSummary dataclass."""
        form_fields = self._form_fields(config_field_ids)
        return PendingRequestSummary(
            urn=self.urn,
            created_ms=self.created.time,
            requestor_urn=self.created.actor.urn if self.created.actor else None,
            requestor_email=form_fields.requestor_email,
            resource=self.entity.urn if self.entity else None,
            assigned_users=self.assignedUsers,
            assigned_groups=self.assignedGroups,
        )


# ---------------------------------------------------------------------------
# Top-level response wrappers (match the data key returned by execute_graphql)
# ---------------------------------------------------------------------------


class GqlListActionRequestsResult(BaseModel):
    total: int = 0
    actionRequests: list[GqlActionRequest] = Field(default_factory=list)


class GqlFetchActionRequestData(BaseModel):
    """Wraps the response from the actionRequest(urn) query."""

    actionRequest: GqlActionRequest | None = None


class GqlListActionRequestsData(BaseModel):
    """Wraps the response from the listActionRequests query."""

    listActionRequests: GqlListActionRequestsResult = Field(
        default_factory=GqlListActionRequestsResult
    )
