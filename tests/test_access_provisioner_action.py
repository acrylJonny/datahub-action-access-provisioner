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
            "authentication_type": "DEFAULT_AUTHENTICATOR",
        },
        "smtp": {
            "username": "sender@gmail.com",
            "password": "app-password",
        },
        "sla": {
            "warning_after_hours": 24,
            "escalation_after_hours": 72,
            "check_interval_seconds": 3600,
        },
        "expiry": {
            "enabled": True,
            "check_interval_seconds": 3600,
        },
        "provisioning": {
            "dry_run": True,
        },
    }


@pytest.fixture
def approved_access_request():
    return AccessRequest(
        urn="urn:li:actionRequest:approved-001",
        status="COMPLETED",
        result="APPROVED",
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
            justification="Needed for quarterly analysis",
        ),
    )


@pytest.fixture
def denied_access_request():
    return AccessRequest(
        urn="urn:li:actionRequest:denied-001",
        status="COMPLETED",
        result="DENIED",
        note="Access policy does not permit this",
        request_type="WORKFLOW_FORM_REQUEST",
        resource="urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD.HR.EMPLOYEES,PROD)",
        requestor_urn="urn:li:corpuser:bob",
        created_ms=int(time.time() * 1000) - 7_200_000,
        due_date_ms=None,
        form_fields=FormFieldValues(
            snowflake_database="PROD",
            snowflake_schema="HR",
            snowflake_role="HR_READ_ROLE",
            access_duration_days=7,
            requestor_email="bob@example.com",
            justification="Investigating payroll discrepancy",
        ),
    )


# ---------------------------------------------------------------------------
# Factory / lifecycle tests
# ---------------------------------------------------------------------------


def test_create_starts_background_threads(base_config_dict, mock_pipeline_context):
    with patch(
        "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
    ) as mock_start:
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        mock_start.assert_called_once()
        action.close()


def test_close_sets_stop_event(base_config_dict, mock_pipeline_context):
    with patch(
        "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        assert not action._stop_event.is_set()
        action.close()
        assert action._stop_event.is_set()


# ---------------------------------------------------------------------------
# Event routing tests
# ---------------------------------------------------------------------------


def _make_mcl_event(entity_type: str, aspect_name: str, entity_urn: str):
    """Build a minimal fake MetadataChangeLogEvent envelope."""
    from datahub_actions.event.event_envelope import EventEnvelope

    mcl = MagicMock()
    mcl.entityType = entity_type
    mcl.aspectName = aspect_name
    mcl.entityUrn = entity_urn

    return EventEnvelope(
        event_type="MetadataChangeLogEvent_v1",
        event=mcl,
        meta={},
    )


def test_act_ignores_non_mcl_events(base_config_dict, mock_pipeline_context):
    with patch(
        "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
    ):
        from datahub_actions.event.event_envelope import EventEnvelope

        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._handle_status_change = MagicMock()

        other_event = EventEnvelope(event_type="EntityChangeEvent_v1", event=MagicMock(), meta={})
        action.act(other_event)

        action._handle_status_change.assert_not_called()
        action.close()


def test_act_ignores_wrong_aspect(base_config_dict, mock_pipeline_context):
    with patch(
        "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._handle_status_change = MagicMock()

        env = _make_mcl_event("actionRequest", "actionRequestInfo", "urn:li:actionRequest:1")
        action.act(env)

        action._handle_status_change.assert_not_called()
        action.close()


def test_act_triggers_handle_on_correct_aspect(base_config_dict, mock_pipeline_context):
    with patch(
        "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._handle_status_change = MagicMock()

        env = _make_mcl_event(
            "actionRequest", "actionRequestStatus", "urn:li:actionRequest:approved-001"
        )
        action.act(env)

        action._handle_status_change.assert_called_once_with("urn:li:actionRequest:approved-001")
        action.close()


# ---------------------------------------------------------------------------
# Provisioning tests
# ---------------------------------------------------------------------------


def test_provision_executes_grants_and_sends_email(
    base_config_dict, mock_pipeline_context, approved_access_request
):
    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch(
            "action_access_provisioner.access_provisioner_action.provision_access"
        ) as mock_provision,
        patch(
            "action_access_provisioner.access_provisioner_action.send_approval_notification"
        ) as mock_email,
    ):
        mock_provision.return_value = ["GRANT USAGE ON DATABASE PROD TO ROLE ANALYST_ROLE"]

        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._provision(approved_access_request)

        mock_provision.assert_called_once()
        mock_email.assert_called_once()

        # Grant should be recorded
        assert "urn:li:actionRequest:approved-001" in action._active_grants
        grant = action._active_grants["urn:li:actionRequest:approved-001"]
        assert grant.snowflake_role == "ANALYST_ROLE"
        assert grant.expires_at_ms is not None

        action.close()


def test_provision_skips_when_missing_role(
    base_config_dict, mock_pipeline_context, approved_access_request
):
    approved_access_request.form_fields.snowflake_role = None

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.provision_access"
        ) as mock_provision,
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._provision(approved_access_request)

        mock_provision.assert_not_called()
        action.close()


def test_handle_status_change_denied(
    base_config_dict, mock_pipeline_context, denied_access_request
):
    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.fetch_action_request"
        ) as mock_fetch,
        patch(
            "action_access_provisioner.access_provisioner_action.send_denial_notification"
        ) as mock_email,
    ):
        mock_fetch.return_value = denied_access_request

        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._handle_status_change(denied_access_request.urn)

        mock_email.assert_called_once()
        action.close()


# ---------------------------------------------------------------------------
# SLA monitoring tests
# ---------------------------------------------------------------------------


def test_sla_warning_sent_once(base_config_dict, mock_pipeline_context):
    """SLA warning should be sent only once per request, not on every poll cycle."""
    from action_access_provisioner.models import PendingRequestSummary

    now_ms = int(time.time() * 1000)
    old_request = PendingRequestSummary(
        urn="urn:li:actionRequest:sla-001",
        created_ms=now_ms - 30 * 3_600_000,  # 30 hours ago
        requestor_urn="urn:li:corpuser:charlie",
        requestor_email="charlie@example.com",
        resource="urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD.SALES.ORDERS,PROD)",
    )

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch("action_access_provisioner.access_provisioner_action.send_sla_warning") as mock_warn,
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)

        # Call twice to verify deduplication
        action._evaluate_sla(old_request, now_ms)
        action._evaluate_sla(old_request, now_ms)

        mock_warn.assert_called_once()
        action.close()


def test_sla_escalation_sent_for_very_old_request(base_config_dict, mock_pipeline_context):
    from action_access_provisioner.models import PendingRequestSummary

    now_ms = int(time.time() * 1000)
    very_old_request = PendingRequestSummary(
        urn="urn:li:actionRequest:sla-002",
        created_ms=now_ms - 80 * 3_600_000,  # 80 hours ago
        requestor_urn="urn:li:corpuser:dave",
        requestor_email="dave@example.com",
        resource=None,
    )

    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch(
            "action_access_provisioner.access_provisioner_action.send_escalation_alert"
        ) as mock_escalate,
        patch("action_access_provisioner.access_provisioner_action.send_sla_warning") as mock_warn,
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        action._evaluate_sla(very_old_request, now_ms)

        mock_escalate.assert_called_once()
        # Should not double-send a warning when escalation fires
        mock_warn.assert_not_called()
        action.close()


# ---------------------------------------------------------------------------
# Expiry / revocation tests
# ---------------------------------------------------------------------------


def test_expired_grant_is_revoked(base_config_dict, mock_pipeline_context):
    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch("action_access_provisioner.access_provisioner_action.get_connection"),
        patch("action_access_provisioner.access_provisioner_action.revoke_access") as mock_revoke,
        patch(
            "action_access_provisioner.access_provisioner_action.send_revocation_notification"
        ) as mock_notify,
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)

        expired_grant = GrantRecord(
            action_request_urn="urn:li:actionRequest:expired-001",
            snowflake_role="OLD_ROLE",
            snowflake_database="PROD",
            snowflake_schema="SALES",
            requestor_email="eve@example.com",
            granted_at_ms=int(time.time() * 1000) - 31 * 86_400_000,
            expires_at_ms=int(time.time() * 1000) - 1000,  # expired 1 second ago
        )
        action._active_grants["urn:li:actionRequest:expired-001"] = expired_grant

        action._check_expiry()

        mock_revoke.assert_called_once()
        mock_notify.assert_called_once()
        # Grant should be removed from the registry
        assert "urn:li:actionRequest:expired-001" not in action._active_grants
        action.close()


def test_non_expired_grant_not_revoked(base_config_dict, mock_pipeline_context):
    with (
        patch(
            "action_access_provisioner.access_provisioner_action.AccessProvisionerAction._start_background_threads"
        ),
        patch("action_access_provisioner.access_provisioner_action.revoke_access") as mock_revoke,
    ):
        from action_access_provisioner.access_provisioner_action import AccessProvisionerAction

        action = AccessProvisionerAction.create(base_config_dict, mock_pipeline_context)

        active_grant = GrantRecord(
            action_request_urn="urn:li:actionRequest:active-001",
            snowflake_role="CURRENT_ROLE",
            snowflake_database="PROD",
            snowflake_schema=None,
            requestor_email="frank@example.com",
            granted_at_ms=int(time.time() * 1000),
            expires_at_ms=int(time.time() * 1000) + 30 * 86_400_000,  # 30 days in future
        )
        action._active_grants["urn:li:actionRequest:active-001"] = active_grant

        action._check_expiry()

        mock_revoke.assert_not_called()
        assert "urn:li:actionRequest:active-001" in action._active_grants
        action.close()
