"""DataHub GraphQL helpers for querying access requests."""

import logging
from typing import Any, Optional

from .models import (
    ACTION_REQUEST_TYPE_WORKFLOW,
    REQUEST_STATUS_PENDING,
    AccessRequest,
    FormFieldValues,
    PendingRequestSummary,
)

logger = logging.getLogger(__name__)

# GraphQL query to fetch a single action request by URN
_FETCH_ACTION_REQUEST_QUERY = """
query fetchActionRequest($urn: String!) {
  actionRequest(urn: $urn) {
    urn
    actionRequestInfo {
      type
      resource
      resourceType
      assignedUsers { urn }
      assignedGroups { urn }
      created
      createdBy { urn }
      dueDate
      params {
        workflowFormRequest {
          fields {
            id
            values {
              ... on StringValue { stringValue }
              ... on NumberValue { numberValue }
            }
          }
          access { expiresAt }
        }
      }
    }
    actionRequestStatus {
      status
      result
      note
    }
  }
}
"""

# GraphQL search query for PENDING access requests
_SEARCH_PENDING_REQUESTS_QUERY = """
query searchPendingRequests($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
    searchResults {
      entity {
        urn
        ... on ActionRequest {
          actionRequestInfo {
            type
            resource
            created
            createdBy { urn }
            assignedUsers { urn }
            assignedGroups { urn }
            params {
              workflowFormRequest {
                fields {
                  id
                  values {
                    ... on StringValue { stringValue }
                    ... on NumberValue { numberValue }
                  }
                }
              }
            }
          }
          actionRequestStatus { status result }
        }
      }
    }
  }
}
"""


def _extract_field_value(values: list[dict[str, Any]]) -> Optional[str]:
    """Return the first primitive value from a form field's values list."""
    for v in values:
        if "stringValue" in v:
            return v["stringValue"]
        if "numberValue" in v:
            return str(v["numberValue"])
    return None


def _parse_form_fields(
    raw_fields: list[dict[str, Any]],
    config_field_ids: dict[str, str],
) -> FormFieldValues:
    """Map raw GraphQL field list into a FormFieldValues dataclass."""
    raw: dict[str, str] = {}
    for f in raw_fields:
        field_id = f.get("id", "")
        val = _extract_field_value(f.get("values", []))
        if val is not None:
            raw[field_id] = val

    def _get(config_key: str) -> Optional[str]:
        form_field_id = config_field_ids.get(config_key)
        return raw.get(form_field_id) if form_field_id else None

    duration_str = _get("field_access_duration_days")
    duration_int: Optional[int] = None
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


def fetch_action_request(
    graph,
    urn: str,
    config_field_ids: dict[str, str],
) -> Optional[AccessRequest]:
    """Fetch and parse a single ActionRequest by URN using the DataHub graph client."""
    try:
        result = graph.execute_graphql(
            _FETCH_ACTION_REQUEST_QUERY,
            variables={"urn": urn},
        )
    except Exception as exc:
        logger.error(f"GraphQL error fetching action request {urn}: {exc}")
        return None

    node = (result or {}).get("actionRequest")
    if not node:
        logger.warning(f"No actionRequest found for URN {urn}")
        return None

    return _parse_action_request_node(node, config_field_ids)


def fetch_pending_action_requests(
    graph,
    config_field_ids: dict[str, str],
    batch_size: int = 100,
) -> list[PendingRequestSummary]:
    """Search DataHub for all PENDING workflow access requests."""
    variables = {
        "input": {
            "types": ["ACTION_REQUEST"],
            "query": "*",
            "start": 0,
            "count": batch_size,
            "filters": [
                {"field": "status", "value": REQUEST_STATUS_PENDING},
                {"field": "type", "value": ACTION_REQUEST_TYPE_WORKFLOW},
            ],
        }
    }

    try:
        result = graph.execute_graphql(_SEARCH_PENDING_REQUESTS_QUERY, variables=variables)
    except Exception as exc:
        logger.error(f"GraphQL error searching pending requests: {exc}")
        return []

    search_results = (result or {}).get("searchAcrossEntities", {}).get("searchResults", [])

    pending: list[PendingRequestSummary] = []
    for item in search_results:
        entity = item.get("entity", {})
        info = entity.get("actionRequestInfo", {})
        if not info:
            continue

        fields_raw = (info.get("params") or {}).get("workflowFormRequest", {}).get("fields", [])
        form_fields = _parse_form_fields(fields_raw, config_field_ids)

        pending.append(
            PendingRequestSummary(
                urn=entity.get("urn", ""),
                created_ms=info.get("created") or 0,
                requestor_urn=(info.get("createdBy") or {}).get("urn"),
                requestor_email=form_fields.requestor_email,
                resource=info.get("resource"),
                assigned_users=[u.get("urn", "") for u in (info.get("assignedUsers") or [])],
                assigned_groups=[g.get("urn", "") for g in (info.get("assignedGroups") or [])],
            )
        )

    return pending


def _parse_action_request_node(
    node: dict[str, Any],
    config_field_ids: dict[str, str],
) -> AccessRequest:
    info = node.get("actionRequestInfo", {})
    status_obj = node.get("actionRequestStatus", {})

    fields_raw = (info.get("params") or {}).get("workflowFormRequest", {}).get("fields", [])
    form_fields = _parse_form_fields(fields_raw, config_field_ids)

    return AccessRequest(
        urn=node.get("urn", ""),
        status=status_obj.get("status", ""),
        result=status_obj.get("result"),
        note=status_obj.get("note"),
        request_type=info.get("type", ""),
        resource=info.get("resource"),
        requestor_urn=(info.get("createdBy") or {}).get("urn"),
        created_ms=info.get("created"),
        due_date_ms=info.get("dueDate"),
        form_fields=form_fields,
    )
