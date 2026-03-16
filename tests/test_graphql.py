"""Unit tests for DataHub GraphQL helpers and GQL Pydantic response models."""

from unittest.mock import MagicMock

import pytest

from action_access_provisioner.gql_types import (
    GqlActionRequest,
    GqlFetchActionRequestData,
    GqlFieldValue,
    GqlFormField,
    GqlListActionRequestsData,
    GqlWorkflowFormRequest,
)
from action_access_provisioner.graphql import fetch_action_request
from action_access_provisioner.models import REQUEST_RESULT_APPROVED, REQUEST_STATUS_COMPLETED

# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


@pytest.fixture
def field_id_map() -> dict[str, str]:
    return {
        "field_snowflake_database": "snowflake_database",
        "field_snowflake_schema": "snowflake_schema",
        "field_snowflake_role": "snowflake_role",
        "field_access_duration_days": "access_duration_days",
        "field_requestor_email": "requestor_email",
        "field_justification": "justification",
    }


def _str_field(field_id: str, value: str) -> dict:
    return {"id": field_id, "values": [{"stringValue": value}]}


def _num_field(field_id: str, value: float) -> dict:
    return {"id": field_id, "values": [{"numberValue": value}]}


def _make_action_request_dict(
    status: str = REQUEST_STATUS_COMPLETED, result: str = REQUEST_RESULT_APPROVED
) -> dict:
    """Build a raw dict matching the GraphQL ActionRequest shape."""
    return {
        "urn": "urn:li:actionRequest:001",
        "type": "WORKFLOW_FORM_REQUEST",
        "status": status,
        "result": result,
        "resultNote": None,
        "entity": {"urn": "urn:li:dataset:foo"},
        "assignedUsers": ["urn:li:corpuser:approver"],
        "assignedGroups": [],
        "created": {"time": 1700000000000, "actor": {"urn": "urn:li:corpuser:requester"}},
        "dueDate": None,
        "params": {
            "workflowFormRequest": {
                "fields": [
                    _str_field("snowflake_database", "PROD"),
                    _str_field("snowflake_role", "ANALYST"),
                    _str_field("requestor_email", "req@example.com"),
                ],
                "access": None,
            }
        },
    }


# ---------------------------------------------------------------------------
# GqlFieldValue
# ---------------------------------------------------------------------------


def test_field_value_returns_string():
    fv = GqlFieldValue(stringValue="hello")
    assert fv.as_str() == "hello"


def test_field_value_returns_number_as_string():
    fv = GqlFieldValue(numberValue=30)
    assert fv.as_str() == "30"


def test_field_value_prefers_string_over_number():
    fv = GqlFieldValue(stringValue="text", numberValue=99)
    assert fv.as_str() == "text"


def test_field_value_empty_returns_none():
    fv = GqlFieldValue()
    assert fv.as_str() is None


# ---------------------------------------------------------------------------
# GqlFormField
# ---------------------------------------------------------------------------


def test_form_field_first_value_string():
    f = GqlFormField(id="db", values=[GqlFieldValue(stringValue="PROD")])
    assert f.first_value() == "PROD"


def test_form_field_first_value_number():
    f = GqlFormField(id="days", values=[GqlFieldValue(numberValue=14)])
    assert f.first_value() == "14"


def test_form_field_first_value_empty():
    f = GqlFormField(id="x", values=[])
    assert f.first_value() is None


# ---------------------------------------------------------------------------
# GqlWorkflowFormRequest.to_form_field_values
# ---------------------------------------------------------------------------


def test_to_form_field_values_maps_all_known_fields(field_id_map):
    wf = GqlWorkflowFormRequest.model_validate(
        {
            "fields": [
                _str_field("snowflake_database", "PROD"),
                _str_field("snowflake_schema", "SALES"),
                _str_field("snowflake_role", "ANALYST_ROLE"),
                _num_field("access_duration_days", 30),
                _str_field("requestor_email", "alice@example.com"),
                _str_field("justification", "Q3 analysis"),
            ]
        }
    )
    ff = wf.to_form_field_values(field_id_map)
    assert ff.snowflake_database == "PROD"
    assert ff.snowflake_schema == "SALES"
    assert ff.snowflake_role == "ANALYST_ROLE"
    assert ff.access_duration_days == 30
    assert ff.requestor_email == "alice@example.com"
    assert ff.justification == "Q3 analysis"


def test_to_form_field_values_handles_missing_optional(field_id_map):
    wf = GqlWorkflowFormRequest.model_validate(
        {
            "fields": [
                _str_field("snowflake_database", "DEV"),
                _str_field("snowflake_role", "DEV_ROLE"),
            ]
        }
    )
    ff = wf.to_form_field_values(field_id_map)
    assert ff.snowflake_database == "DEV"
    assert ff.snowflake_schema is None
    assert ff.access_duration_days is None


def test_to_form_field_values_invalid_duration_falls_back_to_none(field_id_map):
    wf = GqlWorkflowFormRequest.model_validate(
        {"fields": [_str_field("access_duration_days", "not-a-number")]}
    )
    ff = wf.to_form_field_values(field_id_map)
    assert ff.access_duration_days is None


# ---------------------------------------------------------------------------
# GqlActionRequest.to_access_request
# ---------------------------------------------------------------------------


def test_to_access_request_populates_all_fields(field_id_map):
    ar = GqlActionRequest.model_validate(_make_action_request_dict())
    req = ar.to_access_request(field_id_map)

    assert req.urn == "urn:li:actionRequest:001"
    assert req.status == REQUEST_STATUS_COMPLETED
    assert req.result == REQUEST_RESULT_APPROVED
    assert req.request_type == "WORKFLOW_FORM_REQUEST"
    assert req.resource == "urn:li:dataset:foo"
    assert req.requestor_urn == "urn:li:corpuser:requester"
    assert req.created_ms == 1700000000000
    assert req.form_fields.snowflake_database == "PROD"
    assert req.form_fields.requestor_email == "req@example.com"
    assert req.is_approved


def test_to_access_request_no_entity(field_id_map):
    raw = _make_action_request_dict()
    raw["entity"] = None
    ar = GqlActionRequest.model_validate(raw)
    req = ar.to_access_request(field_id_map)
    assert req.resource is None


def test_to_access_request_no_actor(field_id_map):
    raw = _make_action_request_dict()
    raw["created"] = {"time": 1700000000000, "actor": None}
    ar = GqlActionRequest.model_validate(raw)
    req = ar.to_access_request(field_id_map)
    assert req.requestor_urn is None


# ---------------------------------------------------------------------------
# GqlActionRequest.to_pending_summary
# ---------------------------------------------------------------------------


def test_to_pending_summary(field_id_map):
    raw = _make_action_request_dict(status="PENDING", result=None)
    raw["assignedUsers"] = ["urn:li:corpuser:alice", "urn:li:corpuser:bob"]
    raw["assignedGroups"] = ["urn:li:corpGroup:data-stewards"]
    ar = GqlActionRequest.model_validate(raw)
    summary = ar.to_pending_summary(field_id_map)

    assert summary.urn == "urn:li:actionRequest:001"
    assert summary.created_ms == 1700000000000
    assert summary.requestor_urn == "urn:li:corpuser:requester"
    assert summary.requestor_email == "req@example.com"
    assert summary.resource == "urn:li:dataset:foo"
    assert summary.assigned_users == ["urn:li:corpuser:alice", "urn:li:corpuser:bob"]
    assert summary.assigned_groups == ["urn:li:corpGroup:data-stewards"]


# ---------------------------------------------------------------------------
# GqlFetchActionRequestData / GqlListActionRequestsData round-trip parsing
# ---------------------------------------------------------------------------


def test_fetch_response_parses_correctly(field_id_map):
    raw = {"actionRequest": _make_action_request_dict()}
    data = GqlFetchActionRequestData.model_validate(raw)
    assert data.actionRequest is not None
    assert data.actionRequest.urn == "urn:li:actionRequest:001"


def test_fetch_response_none_actionRequest():
    data = GqlFetchActionRequestData.model_validate({})
    assert data.actionRequest is None


def test_list_response_parses_correctly(field_id_map):
    raw = {
        "listActionRequests": {
            "total": 1,
            "actionRequests": [_make_action_request_dict()],
        }
    }
    data = GqlListActionRequestsData.model_validate(raw)
    assert data.listActionRequests.total == 1
    assert len(data.listActionRequests.actionRequests) == 1


def test_list_response_empty():
    data = GqlListActionRequestsData.model_validate({})
    assert data.listActionRequests.total == 0
    assert data.listActionRequests.actionRequests == []


# ---------------------------------------------------------------------------
# fetch_action_request (mocked graph — integration of the full public function)
# ---------------------------------------------------------------------------


def test_fetch_action_request_returns_parsed_object(field_id_map):
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {"actionRequest": _make_action_request_dict()}

    req = fetch_action_request(mock_graph, "urn:li:actionRequest:001", field_id_map)

    assert req is not None
    assert req.is_approved
    assert req.form_fields.snowflake_database == "PROD"
    assert req.form_fields.requestor_email == "req@example.com"


def test_fetch_action_request_returns_none_on_empty_response(field_id_map):
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = {}

    req = fetch_action_request(mock_graph, "urn:li:actionRequest:999", field_id_map)
    assert req is None


def test_fetch_action_request_returns_none_on_exception(field_id_map):
    mock_graph = MagicMock()
    mock_graph.execute_graphql.side_effect = RuntimeError("network error")

    req = fetch_action_request(mock_graph, "urn:li:actionRequest:999", field_id_map)
    assert req is None
