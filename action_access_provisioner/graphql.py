"""DataHub GraphQL helpers for querying access requests."""

import logging
import time
from typing import Any, Optional

from .models import (
    ACTION_REQUEST_TYPE_WORKFLOW,
    REQUEST_RESULT_APPROVED,
    REQUEST_STATUS_COMPLETED,
    REQUEST_STATUS_PENDING,
    AccessRequest,
    FormFieldValues,
    PendingRequestSummary,
)

logger = logging.getLogger(__name__)


def _execute_graphql(graph, query: str, variables: dict) -> dict:
    """Execute a GraphQL query against DataHub.

    Works with both DataHubGraph (which has execute_graphql directly) and
    AcrylDataHubGraph (which wraps a DataHubGraph in .graph).
    """
    if hasattr(graph, "execute_graphql"):
        return graph.execute_graphql(query, variables=variables)
    # AcrylDataHubGraph — delegate to the inner DataHubGraph
    if hasattr(graph, "graph") and hasattr(graph.graph, "execute_graphql"):
        return graph.graph.execute_graphql(query, variables=variables)
    raise AttributeError(
        f"Graph object {type(graph)} has no execute_graphql method. Cannot execute GraphQL query."
    )


# Note: ActionRequest does NOT implement Entity, so searchAcrossEntities cannot be used.
# The listActionRequests query (ListActionRequestsInput) is the correct API.
# All fields are top-level — there is no actionRequestInfo / actionRequestStatus wrapper.

# Fetch a single ActionRequest by URN
_FETCH_ACTION_REQUEST_QUERY = """
query fetchActionRequest($urn: String!) {
  actionRequest(urn: $urn) {
    urn
    type
    status
    result
    resultNote
    entity { urn }
    assignedUsers
    assignedGroups
    created { time actor { urn } }
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
}
"""

# List ActionRequests with server-side type/status filtering
_LIST_ACTION_REQUESTS_QUERY = """
query listActionRequests($input: ListActionRequestsInput!) {
  listActionRequests(input: $input) {
    total
    actionRequests {
      urn
      type
      status
      result
      resultNote
      entity { urn }
      assignedUsers
      assignedGroups
      created { time actor { urn } }
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


def _parse_action_request_node(
    node: dict[str, Any],
    config_field_ids: dict[str, str],
) -> AccessRequest:
    """Parse an ActionRequest node returned by listActionRequests or actionRequest queries."""
    created = node.get("created") or {}
    fields_raw = (node.get("params") or {}).get("workflowFormRequest", {}).get("fields", [])
    form_fields = _parse_form_fields(fields_raw, config_field_ids)

    return AccessRequest(
        urn=node.get("urn", ""),
        status=node.get("status", ""),
        result=node.get("result"),
        note=node.get("resultNote"),
        request_type=node.get("type", ""),
        resource=(node.get("entity") or {}).get("urn"),
        requestor_urn=(created.get("actor") or {}).get("urn"),
        created_ms=created.get("time"),
        due_date_ms=node.get("dueDate"),
        form_fields=form_fields,
    )


def fetch_action_request(
    graph,
    urn: str,
    config_field_ids: dict[str, str],
) -> Optional[AccessRequest]:
    """Fetch and parse a single ActionRequest by URN using the DataHub graph client."""
    try:
        result = _execute_graphql(
            graph,
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
    """List all PENDING workflow access requests from DataHub."""
    variables = {
        "input": {
            "start": 0,
            "count": batch_size,
            "type": ACTION_REQUEST_TYPE_WORKFLOW,
            "status": REQUEST_STATUS_PENDING,
            "allActionRequests": True,
        }
    }

    try:
        result = _execute_graphql(graph, _LIST_ACTION_REQUESTS_QUERY, variables=variables)
    except Exception as exc:
        logger.error(f"GraphQL error fetching pending requests: {exc}")
        return []

    action_requests = (result or {}).get("listActionRequests", {}).get("actionRequests", [])

    pending: list[PendingRequestSummary] = []
    for node in action_requests:
        created = node.get("created") or {}
        fields_raw = (node.get("params") or {}).get("workflowFormRequest", {}).get("fields", [])
        form_fields = _parse_form_fields(fields_raw, config_field_ids)

        pending.append(
            PendingRequestSummary(
                urn=node.get("urn", ""),
                created_ms=created.get("time") or 0,
                requestor_urn=(created.get("actor") or {}).get("urn"),
                requestor_email=form_fields.requestor_email,
                resource=(node.get("entity") or {}).get("urn"),
                # assignedUsers / assignedGroups are [String!] scalars in this schema version
                assigned_users=node.get("assignedUsers") or [],
                assigned_groups=node.get("assignedGroups") or [],
            )
        )

    return pending


def fetch_all_approved_requests(
    graph,
    config_field_ids: dict[str, str],
    lookback_days: int = 90,
    batch_size: int = 100,
) -> list[AccessRequest]:
    """
    Return all COMPLETED/ACCEPTED workflow access requests created within the lookback window.

    Uses listActionRequests with status=COMPLETED and allActionRequests=True, then
    post-filters by result=ACCEPTED and the time window in Python. This avoids relying on
    the result field being indexed for server-side filtering.
    """
    since_ms = int((time.time() - lookback_days * 86_400) * 1000)
    approved: list[AccessRequest] = []
    start = 0

    while True:
        variables = {
            "input": {
                "start": start,
                "count": batch_size,
                "type": ACTION_REQUEST_TYPE_WORKFLOW,
                "status": REQUEST_STATUS_COMPLETED,
                "allActionRequests": True,
            }
        }

        try:
            result = _execute_graphql(graph, _LIST_ACTION_REQUESTS_QUERY, variables=variables)
        except Exception as exc:
            logger.error(f"GraphQL error fetching approved requests (start={start}): {exc}")
            break

        data = (result or {}).get("listActionRequests", {})
        action_requests = data.get("actionRequests", [])
        total = data.get("total", 0)

        for node in action_requests:
            ar = _parse_action_request_node(node, config_field_ids)
            # Post-filter: only ACCEPTED results within the lookback window
            if ar.result == REQUEST_RESULT_APPROVED and (
                ar.created_ms is None or ar.created_ms >= since_ms
            ):
                approved.append(ar)

        start += len(action_requests)
        if start >= total or not action_requests:
            break

    logger.info(f"[GraphQL] Found {len(approved)} approved requests in last {lookback_days} days")
    return approved
