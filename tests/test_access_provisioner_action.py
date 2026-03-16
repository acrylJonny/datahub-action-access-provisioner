"""Unit tests for the AccessProvisionerAction."""

import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub_actions.pipeline.pipeline_context import PipelineContext

from action_access_provisioner.models import (
    AccessRequest,
    FormFieldValues,
    GrantRecord,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mcl_event(entity_type: str, aspect_name: str, entity_urn: str):
    from datahub_actions.event.event_envelope import EventEnvelope

    mcl = MagicMock()
    mcl.entityType = entity_type
    mcl.aspectName = aspect_name
    mcl.entityUrn = entity_urn

    return EventEnvelope(event_type="MetadataChangeLogEvent_v1", event=mcl, meta={})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_pipeline_context():
    ctx = Mock(spec=PipelineContext)
    ctx.graph = MagicMock()
    return ctx


@pytest.fixture
def base_config_dict():
    return {
        "snowflake_connection": {
            "account_id": "test-account",
            "username": "test-user",
            "password": "test-password",
            "warehouse": "TEST_WH",
            "role": "SYSADMIN",
            "authentication_type": "DEFAULT_AUTHENTICATOR",
        },
        "state": {
            "database": "TEST_DB",
            "schema": "AP_STATE",
        },
        "smtp": {
            "username": "sender@gmail.com",
            "password": "app-password",
        },
        "provisioning": {
            "dry_run": True,
        },
    }


@pytest.fixture
def approved_request():
    return AccessRequest(
        urn="urn:li:actionRequest:approved-001",
        status="COMPLETED",
        result="ACCEPTED",
        note="Looks good",
        request_type="WORKFLOW_FORM_REQUEST",
        resource="urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD.SALES.ORDERS,PROD)",
        requestor_urn="urn:li:corpuser:alice",
        created_ms=int(time.time() * 1000) - 3_600_000,
        due_date_ms=None,
        form_fields=FormFieldValues(
            snowflake_database="PROD",
            snowflake_schema="SALES",
            snowflake_role="ANALYST_ROLE",
            access_duration_days=30,
            requestor_email="alice@example.com",
            justification="Q3 analysis",
        ),
    )


@pytest.fixture
def denied_request():
    return AccessRequest(
        urn="urn:li:actionRequest:denied-001",
        status="COMPLETED",
        result="REJECTED",
        note="Policy does not permit this",
        request_type="WORKFLOW_FORM_REQUEST",
        resource="urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD.HR.EMPLOYEES,PROD)",
        requestor_urn="urn:li:corpuser:bob",
        created_ms=int(time.time() * 1000) - 7_200_000,
        due_date_ms=None,
        form_fields=FormFieldValues(
            snowflake_database="PROD",
            snowflake_schema="HR",
            snowflake_role="HR_READ_ROLE",
            requestor_email="bob@example.com",
        ),
    )


# ---------------------------------------------------------------------------
# Factory / catchup
# ---------------------------------------------------------------------------


def _create_action(base_config_dict, mock_pipeline_context, catchup_patch=True):
    """Helper to create action with catchup patched out by default."""
    from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

    if catchup_patch:
        with patch.object(AccessProvisionerAction, "_startup_catchup"):
            action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        return action
    return AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)


def test_create_calls_startup_catchup(base_config_dict, mock_pipeline_context):
    from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

    with patch.object(AccessProvisionerAction, "_startup_catchup") as mock_catchup:
        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        mock_catchup.assert_called_once()
        action.close()


def test_catchup_approved_skips_already_provisioned(
    base_config_dict, mock_pipeline_context, approved_request
):
    from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

    with patch.object(AccessProvisionerAction, "_startup_catchup"):
        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.fetch_all_approved_requests",
            return_value=[approved_request],
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.is_already_provisioned",
            return_value=True,
        ),
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch("action_access_provisioner.access_provisioner_action.ensure_state_tables"),
        patch.object(action, "_provision") as mock_provision,
    ):
        action._catchup_approved_requests()
        mock_provision.assert_not_called()

    action.close()


def test_catchup_approved_provisions_new_requests(
    base_config_dict, mock_pipeline_context, approved_request
):
    from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

    with patch.object(AccessProvisionerAction, "_startup_catchup"):
        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.fetch_all_approved_requests",
            return_value=[approved_request],
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.is_already_provisioned",
            return_value=False,
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.is_provisioning_failed",
            return_value=False,
        ),
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch.object(action, "_provision") as mock_provision,
    ):
        action._catchup_approved_requests()
        mock_provision.assert_called_once_with(approved_request)

    action.close()


# ---------------------------------------------------------------------------
# Live event routing
# ---------------------------------------------------------------------------


def test_act_ignores_non_mcl_events(base_config_dict, mock_pipeline_context):
    from datahub_actions.event.event_envelope import EventEnvelope

    action = _create_action(base_config_dict, mock_pipeline_context)
    action._handle_status_change = MagicMock()

    other = EventEnvelope(event_type="EntityChangeEvent_v1", event=MagicMock(), meta={})
    action.act(other)

    action._handle_status_change.assert_not_called()
    action.close()


def test_act_ignores_wrong_aspect(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    action._handle_status_change = MagicMock()

    env = _make_mcl_event("actionRequest", "actionRequestInfo", "urn:li:actionRequest:1")
    action.act(env)

    action._handle_status_change.assert_not_called()
    action.close()


def test_act_triggers_handle_on_status_change(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    action._handle_status_change = MagicMock()

    env = _make_mcl_event("actionRequest", "actionRequestStatus", "urn:li:actionRequest:001")
    action.act(env)

    action._handle_status_change.assert_called_once_with("urn:li:actionRequest:001")
    action.close()


# ---------------------------------------------------------------------------
# _handle_status_change — idempotency guard
# ---------------------------------------------------------------------------


def test_handle_status_change_skips_duplicate_approval(
    base_config_dict, mock_pipeline_context, approved_request
):
    action = _create_action(base_config_dict, mock_pipeline_context)

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.fetch_action_request",
            return_value=approved_request,
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.is_already_provisioned",
            return_value=True,
        ),
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch.object(action, "_provision") as mock_provision,
    ):
        action._handle_status_change(approved_request.urn)
        mock_provision.assert_not_called()

    action.close()


def test_handle_status_change_provisions_new_approval(
    base_config_dict, mock_pipeline_context, approved_request
):
    action = _create_action(base_config_dict, mock_pipeline_context)

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.fetch_action_request",
            return_value=approved_request,
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.is_already_provisioned",
            return_value=False,
        ),
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch.object(action, "_provision") as mock_provision,
    ):
        action._handle_status_change(approved_request.urn)
        mock_provision.assert_called_once()

    action.close()


def test_handle_status_change_denied_sends_email(
    base_config_dict, mock_pipeline_context, denied_request
):
    action = _create_action(base_config_dict, mock_pipeline_context)

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.fetch_action_request",
            return_value=denied_request,
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.send_denial_notification"
        ) as mock_email,
    ):
        action._handle_status_change(denied_request.urn)
        mock_email.assert_called_once()

    action.close()


# ---------------------------------------------------------------------------
# Provisioning
# ---------------------------------------------------------------------------


def test_provision_records_grant_state(base_config_dict, mock_pipeline_context, approved_request):
    action = _create_action(base_config_dict, mock_pipeline_context)

    with (
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch(
            "action_access_provisioner.access_provisioner_action.provision_access",
            return_value=["GRANT USAGE ON DATABASE PROD TO ROLE ANALYST_ROLE"],
        ),
        patch("action_access_provisioner.access_provisioner_action.record_grant") as mock_record,
        patch("action_access_provisioner.access_provisioner_action.send_approval_notification"),
    ):
        action._provision(approved_request)
        mock_record.assert_called_once()
        grant_arg = mock_record.call_args[0][1]
        assert grant_arg.snowflake_role == "ANALYST_ROLE"
        assert grant_arg.expires_at_ms is not None

    action.close()


def test_provision_skips_when_missing_role(
    base_config_dict, mock_pipeline_context, approved_request
):
    approved_request.form_fields.snowflake_role = None
    action = _create_action(base_config_dict, mock_pipeline_context)

    with (
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch(
            "action_access_provisioner.access_provisioner_action.get_user_default_role",
            return_value=None,
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.provision_access"
        ) as mock_provision,
    ):
        action._provision(approved_request)
        mock_provision.assert_not_called()

    action.close()


# ---------------------------------------------------------------------------
# SLA evaluation — idempotency via Snowflake state
# ---------------------------------------------------------------------------


def test_sla_warning_not_sent_if_already_notified(base_config_dict, mock_pipeline_context):
    from action_access_provisioner.models import PendingRequestSummary

    action = _create_action(base_config_dict, mock_pipeline_context)
    now_ms = int(time.time() * 1000)
    req = PendingRequestSummary(
        urn="urn:li:actionRequest:sla-001",
        created_ms=now_ms - 30 * 3_600_000,  # 30 hours ago
        requestor_urn=None,
        requestor_email="charlie@example.com",
        resource=None,
    )
    mock_conn = MagicMock()

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.is_sla_notified",
            return_value=True,
        ),
        patch("action_access_provisioner.access_provisioner_action.send_sla_warning") as mock_warn,
    ):
        action._evaluate_sla(req, now_ms, mock_conn)
        mock_warn.assert_not_called()

    action.close()


def test_sla_warning_sent_and_recorded(base_config_dict, mock_pipeline_context):
    from action_access_provisioner.models import PendingRequestSummary

    action = _create_action(base_config_dict, mock_pipeline_context)
    now_ms = int(time.time() * 1000)
    req = PendingRequestSummary(
        urn="urn:li:actionRequest:sla-002",
        created_ms=now_ms - 30 * 3_600_000,
        requestor_urn=None,
        requestor_email="dave@example.com",
        resource=None,
    )
    mock_conn = MagicMock()

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.is_sla_notified",
            return_value=False,
        ),
        patch("action_access_provisioner.access_provisioner_action.send_sla_warning") as mock_warn,
        patch(
            "action_access_provisioner.access_provisioner_action.record_sla_notification"
        ) as mock_record,
    ):
        action._evaluate_sla(req, now_ms, mock_conn)
        mock_warn.assert_called_once()
        mock_record.assert_called_once()

    action.close()


def test_sla_escalation_sent_for_old_request(base_config_dict, mock_pipeline_context):
    from action_access_provisioner.models import PendingRequestSummary

    action = _create_action(base_config_dict, mock_pipeline_context)
    now_ms = int(time.time() * 1000)
    req = PendingRequestSummary(
        urn="urn:li:actionRequest:sla-003",
        created_ms=now_ms - 80 * 3_600_000,  # 80 hours
        requestor_urn=None,
        requestor_email="eve@example.com",
        resource=None,
    )
    mock_conn = MagicMock()

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.is_sla_notified",
            return_value=False,
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.send_escalation_alert"
        ) as mock_escalate,
        patch("action_access_provisioner.access_provisioner_action.send_sla_warning") as mock_warn,
        patch("action_access_provisioner.access_provisioner_action.record_sla_notification"),
    ):
        action._evaluate_sla(req, now_ms, mock_conn)
        mock_escalate.assert_called_once()
        mock_warn.assert_not_called()

    action.close()


# ---------------------------------------------------------------------------
# Role resolution helpers
# ---------------------------------------------------------------------------


def test_extract_snowflake_username_urn_id(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    assert (
        action._extract_snowflake_username("urn:li:corpuser:john.doe@company.com")
        == "john.doe@company.com"
    )
    action.close()


def test_extract_snowflake_username_email_local_part(base_config_dict, mock_pipeline_context):
    cfg = {
        **base_config_dict,
        "provisioning": {"dry_run": True, "requestor_username_format": "email_local_part"},
    }
    action = _create_action(cfg, mock_pipeline_context)
    assert action._extract_snowflake_username("urn:li:corpuser:john.doe@company.com") == "john.doe"
    action.close()


def test_extract_snowflake_username_email_local_part_no_at(base_config_dict, mock_pipeline_context):
    cfg = {
        **base_config_dict,
        "provisioning": {"dry_run": True, "requestor_username_format": "email_local_part"},
    }
    action = _create_action(cfg, mock_pipeline_context)
    # No @ in urn_id — falls back to returning the urn_id as-is
    assert action._extract_snowflake_username("urn:li:corpuser:johndoe") == "johndoe"
    action.close()


def test_extract_snowflake_username_unknown_urn_format(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    assert action._extract_snowflake_username("urn:li:dataset:something") is None
    action.close()


def test_extract_requestor_email_from_email_urn(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    assert (
        action._extract_requestor_email("urn:li:corpuser:alice@example.com") == "alice@example.com"
    )
    action.close()


def test_extract_requestor_email_non_email_urn(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    assert action._extract_requestor_email("urn:li:corpuser:alice") is None
    action.close()


def test_extract_requestor_email_none_urn(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    assert action._extract_requestor_email(None) is None
    action.close()


def test_resolve_snowflake_role_uses_form_field(base_config_dict, mock_pipeline_context):
    """When the form field is populated, it should be used directly without Snowflake lookup."""
    action = _create_action(base_config_dict, mock_pipeline_context)
    request = AccessRequest(
        urn="urn:li:actionRequest:001",
        status="COMPLETED",
        result="ACCEPTED",
        note=None,
        request_type="WORKFLOW_FORM_REQUEST",
        resource=None,
        requestor_urn="urn:li:corpuser:alice@example.com",
        created_ms=0,
        due_date_ms=None,
        form_fields=FormFieldValues(snowflake_role="EXPLICIT_ROLE", snowflake_database="DB"),
    )
    mock_conn = MagicMock()
    with patch(
        "action_access_provisioner.access_provisioner_action.get_user_default_role"
    ) as mock_lookup:
        role = action._resolve_snowflake_role(request, mock_conn)
    assert role == "EXPLICIT_ROLE"
    mock_lookup.assert_not_called()
    action.close()


def test_resolve_snowflake_role_falls_back_to_snowflake_lookup(
    base_config_dict, mock_pipeline_context
):
    """When snowflake_role is absent, the DEFAULT_ROLE is fetched from Snowflake."""
    action = _create_action(base_config_dict, mock_pipeline_context)
    request = AccessRequest(
        urn="urn:li:actionRequest:002",
        status="COMPLETED",
        result="ACCEPTED",
        note=None,
        request_type="WORKFLOW_FORM_REQUEST",
        resource=None,
        requestor_urn="urn:li:corpuser:bob@example.com",
        created_ms=0,
        due_date_ms=None,
        form_fields=FormFieldValues(snowflake_database="DB"),  # no snowflake_role
    )
    mock_conn = MagicMock()
    with patch(
        "action_access_provisioner.access_provisioner_action.get_user_default_role",
        return_value="BOB_DEFAULT_ROLE",
    ) as mock_lookup:
        role = action._resolve_snowflake_role(request, mock_conn)
    assert role == "BOB_DEFAULT_ROLE"
    mock_lookup.assert_called_once_with(mock_conn, "bob@example.com")
    action.close()


def test_resolve_snowflake_role_no_urn_and_no_form_field(base_config_dict, mock_pipeline_context):
    """When neither form field nor requestor URN is available, returns None."""
    action = _create_action(base_config_dict, mock_pipeline_context)
    request = AccessRequest(
        urn="urn:li:actionRequest:003",
        status="COMPLETED",
        result="ACCEPTED",
        note=None,
        request_type="WORKFLOW_FORM_REQUEST",
        resource=None,
        requestor_urn=None,
        created_ms=0,
        due_date_ms=None,
        form_fields=FormFieldValues(snowflake_database="DB"),
    )
    mock_conn = MagicMock()
    role = action._resolve_snowflake_role(request, mock_conn)
    assert role is None
    action.close()


# ---------------------------------------------------------------------------
# Expiry catchup
# ---------------------------------------------------------------------------


def test_expiry_catchup_revokes_and_notifies(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)

    expired = GrantRecord(
        action_request_urn="urn:li:actionRequest:expired-001",
        snowflake_role="OLD_ROLE",
        snowflake_database="PROD",
        snowflake_schema="SALES",
        requestor_email="eve@example.com",
        granted_at_ms=int(time.time() * 1000) - 31 * 86_400_000,
        expires_at_ms=int(time.time() * 1000) - 1000,
    )

    with (
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch(
            "action_access_provisioner.access_provisioner_action.get_expired_grants",
            return_value=[expired],
        ),
        patch("action_access_provisioner.access_provisioner_action.revoke_access") as mock_revoke,
        patch(
            "action_access_provisioner.access_provisioner_action.record_revocation"
        ) as mock_record_rev,
        patch(
            "action_access_provisioner.access_provisioner_action.send_revocation_notification"
        ) as mock_notify,
    ):
        action._catchup_expiry()

        mock_revoke.assert_called_once()
        # record_revocation is now called with the full GrantRecord, not just the URN
        mock_record_rev.assert_called_once_with(
            mock_revoke.call_args[0][0],  # conn
            expired,  # GrantRecord
            action.config.state,
        )
        mock_notify.assert_called_once()

    action.close()
