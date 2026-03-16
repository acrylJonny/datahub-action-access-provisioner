"""DataHub GraphQL helpers for querying access requests."""

import logging
import time

from pydantic import ValidationError

from action_access_provisioner.gql_types import (
    GqlFetchActionRequestData,
    GqlListActionRequestsData,
)
from action_access_provisioner.models import (
    ACTION_REQUEST_TYPE_WORKFLOW,
    REQUEST_RESULT_APPROVED,
    REQUEST_STATUS_COMPLETED,
    REQUEST_STATUS_PENDING,
    AccessRequest,
    PendingRequestSummary,
)

logger = logging.getLogger(__name__)


def _execute_graphql(graph: object, query: str, variables: dict) -> dict:
    """Execute a GraphQL query against DataHub.

    Works with both DataHubGraph (which has execute_graphql directly) and
    AcrylDataHubGraph (which wraps a DataHubGraph in .graph).
    """
    if hasattr(graph, "execute_graphql"):
        return graph.execute_graphql(query, variables=variables)  # type: ignore[union-attr]
    # AcrylDataHubGraph — delegate to the inner DataHubGraph
    if hasattr(graph, "graph") and hasattr(graph.graph, "execute_graphql"):
        return graph.graph.execute_graphql(query, variables=variables)  # type: ignore[union-attr]
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


def fetch_action_request(
    graph: object,
    urn: str,
    config_field_ids: dict[str, str],
) -> AccessRequest | None:
    """Fetch and parse a single ActionRequest by URN."""
    try:
        raw = _execute_graphql(graph, _FETCH_ACTION_REQUEST_QUERY, variables={"urn": urn})
    except Exception as exc:
        logger.error(f"GraphQL error fetching action request {urn}: {exc}")
        return None

    try:
        data = GqlFetchActionRequestData.model_validate(raw or {})
    except ValidationError as exc:
        logger.error(f"Failed to parse actionRequest response for {urn}: {exc}")
        return None

    if not data.actionRequest:
        logger.warning(f"No actionRequest found for URN {urn}")
        return None

    return data.actionRequest.to_access_request(config_field_ids)


def fetch_pending_action_requests(
    graph: object,
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
        raw = _execute_graphql(graph, _LIST_ACTION_REQUESTS_QUERY, variables=variables)
    except Exception as exc:
        logger.error(f"GraphQL error fetching pending requests: {exc}")
        return []

    try:
        data = GqlListActionRequestsData.model_validate(raw or {})
    except ValidationError as exc:
        logger.error(f"Failed to parse listActionRequests response: {exc}")
        return []

    return [
        ar.to_pending_summary(config_field_ids) for ar in data.listActionRequests.actionRequests
    ]


def fetch_all_approved_requests(
    graph: object,
    config_field_ids: dict[str, str],
    lookback_days: int = 90,
    batch_size: int = 100,
) -> list[AccessRequest]:
    """Return all COMPLETED/ACCEPTED workflow access requests within the lookback window.

    Uses listActionRequests with status=COMPLETED and allActionRequests=True, then
    post-filters by result=ACCEPTED and the time window in Python. This avoids relying
    on the result field being indexed for server-side filtering.
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
            raw = _execute_graphql(graph, _LIST_ACTION_REQUESTS_QUERY, variables=variables)
        except Exception as exc:
            logger.error(f"GraphQL error fetching approved requests (start={start}): {exc}")
            break

        try:
            data = GqlListActionRequestsData.model_validate(raw or {})
        except ValidationError as exc:
            logger.error(f"Failed to parse listActionRequests response (start={start}): {exc}")
            break

        result = data.listActionRequests
        for ar in result.actionRequests:
            # Post-filter: only ACCEPTED results within the lookback window
            if ar.result == REQUEST_RESULT_APPROVED and (
                ar.created.time is None or ar.created.time >= since_ms
            ):
                approved.append(ar.to_access_request(config_field_ids))

        start += len(result.actionRequests)
        if start >= result.total or not result.actionRequests:
            break

    logger.info(f"[GraphQL] Found {len(approved)} approved requests in last {lookback_days} days")
    return approved
