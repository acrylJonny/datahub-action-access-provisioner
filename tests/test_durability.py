from unittest.mock import MagicMock, Mock, patch

import pytest
from datahub_actions.pipeline.pipeline_context import PipelineContext

from action_access_provisioner.access_provisioner_action import AccessProvisionerAction
from action_access_provisioner.config import (
    AccessProvisionerConfig,
    DatabricksAccessProvisionerConfig,
)
from action_access_provisioner.databricks_access_provisioner_action import (
    DatabricksAccessProvisionerAction,
)

_SF_MODULE = "action_access_provisioner.access_provisioner_action"
_DBX_MODULE = "action_access_provisioner.databricks_access_provisioner_action"


@pytest.fixture
def ctx():
    c = Mock(spec=PipelineContext)
    c.graph = MagicMock()
    return c


def _sf_config(**overrides) -> AccessProvisionerConfig:
    base = {
        "snowflake_connection": {
            "account_id": "acct",
            "username": "u",
            "password": "p",
            "authentication_type": "DEFAULT_AUTHENTICATOR",
        },
        "state": {"database": "DB", "schema": "AP"},
        "provisioning": {"dry_run": True},
    }
    base.update(overrides)
    return AccessProvisionerConfig.model_validate(base)


def _dbx_config(**overrides) -> DatabricksAccessProvisionerConfig:
    base = {
        "databricks_connection": {
            "host": "https://x.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc",
            "token": "dapiX",
        },
        "state": {"catalog": "cat", "schema": "ap"},
        "provisioning": {"dry_run": True, "grant_method": "sql"},
    }
    base.update(overrides)
    return DatabricksAccessProvisionerConfig.model_validate(base)


# ---------------------------------------------------------------------------
# Per-phase isolation — one failing phase must not abort the others
# ---------------------------------------------------------------------------


def test_sf_reconcile_isolates_phase_failures(ctx):
    action = AccessProvisionerAction(_sf_config(), ctx)
    with (
        patch.object(action, "_get_snowflake_connection", return_value=MagicMock()),
        patch(f"{_SF_MODULE}.ensure_state_tables"),
        patch.object(action, "_catchup_approved_requests", side_effect=RuntimeError("boom")) as p1,
        patch.object(action, "_catchup_expiry") as p2,
        patch.object(action, "_catchup_sla") as p3,
    ):
        # Real bound methods carry __name__ (used in the isolation log line); give the
        # mock one so the failing-phase branch logs cleanly.
        p1.__name__ = "_catchup_approved_requests"
        action._reconcile_once()  # must not raise
    p1.assert_called_once()
    p2.assert_called_once()
    p3.assert_called_once()


def test_sf_reconcile_connect_failure_skips_phases(ctx):
    action = AccessProvisionerAction(_sf_config(), ctx)
    with (
        patch.object(action, "_get_snowflake_connection", side_effect=RuntimeError("no conn")),
        patch.object(action, "_catchup_approved_requests") as p1,
    ):
        action._reconcile_once()  # logged and returns, no phases run
    p1.assert_not_called()


def test_dbx_reconcile_isolates_phase_failures(ctx):
    action = DatabricksAccessProvisionerAction(_dbx_config(), ctx)
    with (
        patch.object(action, "_get_sql_conn", return_value=MagicMock()),
        patch(f"{_DBX_MODULE}.dbx.ensure_state_tables"),
        patch.object(action, "_catchup_approved_requests", side_effect=RuntimeError("boom")) as p1,
        patch.object(action, "_catchup_expiry") as p2,
        patch.object(action, "_catchup_sla") as p3,
    ):
        p1.__name__ = "_catchup_approved_requests"
        action._reconcile_once()
    p1.assert_called_once()
    p2.assert_called_once()
    p3.assert_called_once()


def test_dbx_reconcile_connect_failure_skips_phases(ctx):
    action = DatabricksAccessProvisionerAction(_dbx_config(), ctx)
    with (
        patch.object(action, "_get_sql_conn", side_effect=RuntimeError("no conn")),
        patch.object(action, "_catchup_approved_requests") as p1,
    ):
        action._reconcile_once()
    p1.assert_not_called()


# ---------------------------------------------------------------------------
# Reconciler thread lifecycle — disabled = no thread; enabled = start + clean stop
# ---------------------------------------------------------------------------


def test_sf_reconciler_disabled_starts_no_thread(ctx):
    action = AccessProvisionerAction(_sf_config(reconcile={"enabled": False}), ctx)
    action._start_reconciler()
    assert action._reconcile_thread is None


def test_sf_reconciler_starts_and_close_stops_it(ctx):
    action = AccessProvisionerAction(_sf_config(), ctx)
    action._start_reconciler()
    assert action._reconcile_thread is not None
    assert action._reconcile_thread.is_alive()

    action.close()  # sets stop event; the wait() returns immediately

    assert action._stop.is_set()
    assert action._reconcile_thread is None


def test_dbx_reconciler_disabled_starts_no_thread(ctx):
    action = DatabricksAccessProvisionerAction(_dbx_config(reconcile={"enabled": False}), ctx)
    action._start_reconciler()
    assert action._reconcile_thread is None


def test_dbx_reconciler_starts_and_close_stops_it(ctx):
    action = DatabricksAccessProvisionerAction(_dbx_config(), ctx)
    action._start_reconciler()
    assert action._reconcile_thread is not None
    assert action._reconcile_thread.is_alive()

    action.close()

    assert action._stop.is_set()
    assert action._reconcile_thread is None
