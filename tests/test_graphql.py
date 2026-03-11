"""Unit tests for DataHub GraphQL helpers."""

from unittest.mock import MagicMock

import pytest

from action_access_provisioner.graphql import (
    _extract_field_value,
    _parse_form_fields,
    fetch_action_request,
)
from action_access_provisioner.models import REQUEST_RESULT_APPROVED, REQUEST_STATUS_COMPLETED


@pytest.fixture
def field_id_map():
    return {
        "field_snowflake_database": "snowflake_database",
        "field_snowflake_schema": "snowflake_schema",
        "field_snowflake_role": "snowflake_role",
        "field_access_duration_days": "access_duration_days",
        "field_requestor_email": "requestor_email",
        "field_justification": "justification",
    }


def _make_field(field_id: str, string_value: str):
    return {"id": field_id, "values": [{"stringValue": string_value}]}


def _make_number_field(field_id: str, number_value: float):
    return {"id": field_id, "values": [{"numberValue": number_value}]}


# ---------------------------------------------------------------------------
# _extract_field_value
# ---------------------------------------------------------------------------


def test_extract_string_value():
    assert _extract_field_value([{"stringValue": "hello"}]) == "hello"


def test_extract_number_value():
    assert _extract_field_value([{"numberValue": 30}]) == "30"


def test_extract_prefers_first_value():
    assert _extract_field_value([{"stringValue": "first"}, {"stringValue": "second"}]) == "first"


def test_extract_empty_list_returns_none():
    assert _extract_field_value([]) is None


# ---------------------------------------------------------------------------
# _parse_form_fields
# ---------------------------------------------------------------------------


def test_parse_form_fields_maps_all_known_fields(field_id_map):
    raw_fields = [
        _make_field("snowflake_database", "PROD"),
        _make_field("snowflake_schema", "SALES"),
        _make_field("snowflake_role", "ANALYST_ROLE"),
        _make_number_field("access_duration_days", 30),
        _make_field("requestor_email", "alice@example.com"),
        _make_field("justification", "Q3 analysis"),
    ]
    ff = _parse_form_fields(raw_fields, field_id_map)
    assert ff.snowflake_database == "PROD"
    assert ff.snowflake_schema == "SALES"
    assert ff.snowflake_role == "ANALYST_ROLE"
    assert ff.access_duration_days == 30
    assert ff.requestor_email == "alice@example.com"
    assert ff.justification == "Q3 analysis"


def test_parse_form_fields_handles_missing_optional(field_id_map):
    raw_fields = [
        _make_field("snowflake_database", "DEV"),
        _make_field("snowflake_role", "DEV_ROLE"),
    ]
    ff = _parse_form_fields(raw_fields, field_id_map)
    assert ff.snowflake_database == "DEV"
    assert ff.snowflake_schema is None
    assert ff.access_duration_days is None


def test_parse_form_fields_invalid_duration_falls_back_to_none(field_id_map):
    raw_fields = [_make_field("access_duration_days", "not-a-number")]
    ff = _parse_form_fields(raw_fields, field_id_map)
    assert ff.access_duration_days is None


# ---------------------------------------------------------------------------
# fetch_action_request (mocked graph)
# ---------------------------------------------------------------------------


def _make_graph_response(status: str, result: str):
    return {
        "actionRequest": {
            "urn": "urn:li:actionRequest:001",
            "actionRequestInfo": {
                "type": "WORKFLOW_FORM_REQUEST",
                "resource": "urn:li:dataset:foo",
                "assignedUsers": [{"urn": "urn:li:corpuser:approver"}],
                "assignedGroups": [],
                "created": 1700000000000,
                "createdBy": {"urn": "urn:li:corpuser:requester"},
                "dueDate": None,
                "params": {
                    "workflowFormRequest": {
                        "fields": [
                            _make_field("snowflake_database", "PROD"),
                            _make_field("snowflake_role", "ANALYST"),
                            _make_field("requestor_email", "req@example.com"),
                        ],
                        "access": None,
                    }
                },
            },
            "actionRequestStatus": {"status": status, "result": result, "note": None},
        }
    }


def test_fetch_action_request_returns_parsed_object(field_id_map):
    mock_graph = MagicMock()
    mock_graph.execute_graphql.return_value = _make_graph_response(
        REQUEST_STATUS_COMPLETED, REQUEST_RESULT_APPROVED
    )

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
