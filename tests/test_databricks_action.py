import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub_actions.pipeline.pipeline_context import PipelineContext

from action_access_provisioner.models import (
    AccessRequest,
    DatabricksGrantRecord,
    FormFieldValues,
)

_ACTION_MODULE = "action_access_provisioner.databricks_access_provisioner_action"


@pytest.fixture
def mock_pipeline_context():
    ctx = Mock(spec=PipelineContext)
    ctx.graph = MagicMock()
    return ctx


@pytest.fixture
def base_config_dict():
    return {
        "databricks_connection": {
            "host": "https://dbc-test.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc123",
            "token": "dapiXXXX",
        },
        "state": {"catalog": "test_cat", "schema": "ap_state"},
        "smtp": {"username": "sender@gmail.com", "password": "app-password"},
        "provisioning": {"dry_run": True, "grant_method": "sql"},
    }


def _make_request(form_fields, *, resource=None, requestor="urn:li:corpuser:alice@example.com"):
    return AccessRequest(
        urn="urn:li:actionRequest:approved-001",
        status="COMPLETED",
        result="ACCEPTED",
        note="Looks good",
        request_type="WORKFLOW_FORM_REQUEST",
        resource=resource,
        requestor_urn=requestor,
        created_ms=int(time.time() * 1000) - 3_600_000,
        due_date_ms=None,
        form_fields=form_fields,
    )


def _create_action(config_dict, ctx):
    from action_access_provisioner.databricks_access_provisioner_action import (
        DatabricksAccessProvisionerAction,
    )

    with patch.object(DatabricksAccessProvisionerAction, "_startup_catchup"):
        return DatabricksAccessProvisionerAction.create(config_dict, ctx)


def test_create_calls_startup_catchup(base_config_dict, mock_pipeline_context):
    from action_access_provisioner.databricks_access_provisioner_action import (
        DatabricksAccessProvisionerAction,
    )

    with patch.object(DatabricksAccessProvisionerAction, "_startup_catchup") as mock_catchup:
        action = DatabricksAccessProvisionerAction.create(base_config_dict, mock_pipeline_context)
        mock_catchup.assert_called_once()
        action.close()


def test_provision_grants_to_requestor_email_and_records(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    request = _make_request(
        FormFieldValues(
            databricks_catalog="prod",
            databricks_schema="sales",
            requestor_email="alice@example.com",
            access_duration_days=30,
        )
    )

    with (
        patch(f"{_ACTION_MODULE}.dbx.get_sql_connection", return_value=MagicMock()),
        patch(
            f"{_ACTION_MODULE}.dbx.provision_access",
            return_value=["GRANT SELECT ON SCHEMA `prod`.`sales` TO `alice@example.com`"],
        ) as mock_provision,
        patch(f"{_ACTION_MODULE}.dbx.record_grant") as mock_record,
        patch(f"{_ACTION_MODULE}.send_dbx_approval_notification"),
    ):
        action._provision(request)

        # Principal is the requestor email; catalog/schema come from the form.
        kwargs = mock_provision.call_args.kwargs
        assert kwargs["principal"] == "alice@example.com"
        assert kwargs["catalog"] == "prod"
        assert kwargs["schema"] == "sales"

        grant = mock_record.call_args[0][1]
        assert isinstance(grant, DatabricksGrantRecord)
        assert grant.principal == "alice@example.com"
        assert grant.expires_at_ms is not None

    action.close()


def test_provision_ignores_platform_instance_in_resource_urn(
    base_config_dict, mock_pipeline_context
):
    """The target must come from form fields, never parsed from the dataset URN —
    so a platform_instance prefix on the resource has no effect on the grant."""
    action = _create_action(base_config_dict, mock_pipeline_context)
    request = _make_request(
        FormFieldValues(
            databricks_catalog="prod",
            databricks_schema="sales",
            databricks_table="orders",
            requestor_email="alice@example.com",
        ),
        # Dataset URN carries a platform_instance ('myinstance') prefix.
        resource="urn:li:dataset:(urn:li:dataPlatform:databricks,myinstance.prod.sales.orders,PROD)",
    )

    with (
        patch(f"{_ACTION_MODULE}.dbx.get_sql_connection", return_value=MagicMock()),
        patch(f"{_ACTION_MODULE}.dbx.provision_access", return_value=[]) as mock_provision,
        patch(f"{_ACTION_MODULE}.dbx.record_grant"),
        patch(f"{_ACTION_MODULE}.send_dbx_approval_notification"),
    ):
        action._provision(request)
        kwargs = mock_provision.call_args.kwargs
        assert kwargs["catalog"] == "prod"  # not 'myinstance'
        assert kwargs["table"] == "orders"

    action.close()


def test_provision_skips_when_no_catalog(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    request = _make_request(FormFieldValues(requestor_email="alice@example.com"))

    with (
        patch(f"{_ACTION_MODULE}.dbx.get_sql_connection", return_value=MagicMock()),
        patch(f"{_ACTION_MODULE}.dbx.provision_access") as mock_provision,
    ):
        action._provision(request)
        mock_provision.assert_not_called()

    action.close()


def test_provision_principal_falls_back_to_corpuser_email(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    # No requestor_email form field; the corpuser URN identity is an email.
    request = _make_request(
        FormFieldValues(databricks_catalog="prod"),
        requestor="urn:li:corpuser:bob@example.com",
    )

    with (
        patch(f"{_ACTION_MODULE}.dbx.get_sql_connection", return_value=MagicMock()),
        patch(f"{_ACTION_MODULE}.dbx.provision_access", return_value=[]) as mock_provision,
        patch(f"{_ACTION_MODULE}.dbx.record_grant"),
        patch(f"{_ACTION_MODULE}.send_dbx_approval_notification"),
    ):
        action._provision(request)
        assert mock_provision.call_args.kwargs["principal"] == "bob@example.com"

    action.close()


def test_expiry_catchup_revokes_and_notifies(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    expired = DatabricksGrantRecord(
        action_request_urn="urn:li:actionRequest:expired-001",
        principal="eve@example.com",
        catalog="prod",
        schema="sales",
        table=None,
        requestor_email="eve@example.com",
        granted_at_ms=int(time.time() * 1000) - 31 * 86_400_000,
        expires_at_ms=int(time.time() * 1000) - 1000,
    )

    with (
        patch(f"{_ACTION_MODULE}.dbx.get_sql_connection", return_value=MagicMock()),
        patch(f"{_ACTION_MODULE}.dbx.get_expired_grants", return_value=[expired]),
        patch(f"{_ACTION_MODULE}.dbx.revoke_access") as mock_revoke,
        patch(f"{_ACTION_MODULE}.dbx.record_revocation") as mock_record_rev,
        patch(f"{_ACTION_MODULE}.send_dbx_revocation_notification") as mock_notify,
    ):
        action._catchup_expiry()
        mock_revoke.assert_called_once()
        mock_record_rev.assert_called_once()
        mock_notify.assert_called_once()

    action.close()


def test_denied_request_sends_denial_email(base_config_dict, mock_pipeline_context):
    action = _create_action(base_config_dict, mock_pipeline_context)
    denied = AccessRequest(
        urn="urn:li:actionRequest:denied-001",
        status="COMPLETED",
        result="REJECTED",
        note="Policy does not permit this",
        request_type="WORKFLOW_FORM_REQUEST",
        resource="urn:li:dataset:(urn:li:dataPlatform:databricks,prod.hr.employees,PROD)",
        requestor_urn="urn:li:corpuser:bob@example.com",
        created_ms=int(time.time() * 1000),
        due_date_ms=None,
        form_fields=FormFieldValues(databricks_catalog="prod", databricks_schema="hr"),
    )

    with (
        patch(f"{_ACTION_MODULE}.fetch_action_request", return_value=denied),
        patch(f"{_ACTION_MODULE}.send_denial_notification") as mock_email,
    ):
        action._handle_status_change(denied.urn)
        mock_email.assert_called_once()

    action.close()
