from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import DatabricksProvisioningConfig
from action_access_provisioner.databricks import (
    _principal,
    add_group_member,
    build_grant_statements,
    build_revoke_statements,
    is_permanent_databricks_error,
    parse_databricks_dataset_urn,
    provision_access,
    remove_group_member,
    revoke_access,
)
from action_access_provisioner.models import DatabricksGrantRecord


def test_principal_quotes_group_names_with_spaces():
    # Group display names legitimately contain spaces — they must not be rejected.
    assert _principal("Data Analysts") == "`Data Analysts`"


def test_principal_allows_email_and_service_principal():
    assert _principal("jane.smith@corp.com") == "`jane.smith@corp.com`"
    assert _principal("a1b2c3d4-0000-1111-2222-333344445555") == (
        "`a1b2c3d4-0000-1111-2222-333344445555`"
    )


def test_principal_escapes_embedded_backticks():
    # A backtick is doubled so the value cannot break out of the quoting.
    assert _principal("weird`name") == "`weird``name`"


def test_principal_rejects_control_characters():
    with pytest.raises(ValueError):
        _principal("bad\nname")


@pytest.fixture
def sql_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.close = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ---------------------------------------------------------------------------
# Dataset URN parsing — target derivation + platform_instance stripping
# ---------------------------------------------------------------------------


def test_parse_dataset_urn_three_level():
    urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,prod.sales.orders,PROD)"
    assert parse_databricks_dataset_urn(urn) == ("prod", "sales", "orders")


def test_parse_dataset_urn_strips_platform_instance():
    # A platform_instance prefix becomes a leading 4th segment — it must be dropped.
    urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,myinstance.prod.sales.orders,PROD)"
    assert parse_databricks_dataset_urn(urn) == ("prod", "sales", "orders")


def test_parse_dataset_urn_rejects_non_databricks_platform():
    urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
    assert parse_databricks_dataset_urn(urn) is None


def test_parse_dataset_urn_rejects_non_table_level():
    # Catalog/schema-only names aren't datasets and can't resolve to a UC table grant.
    urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,prod.sales,PROD)"
    assert parse_databricks_dataset_urn(urn) is None


def test_parse_dataset_urn_handles_garbage():
    assert parse_databricks_dataset_urn(None) is None
    assert parse_databricks_dataset_urn("urn:li:corpuser:alice@example.com") is None


# ---------------------------------------------------------------------------
# Statement builders — privilege granularity
# ---------------------------------------------------------------------------


def test_grant_statements_catalog_only():
    stmts = build_grant_statements("alice@example.com", "prod", None, None)
    assert "GRANT USE CATALOG ON CATALOG `prod` TO `alice@example.com`" in stmts
    assert any("GRANT SELECT ON CATALOG `prod`" in s for s in stmts)


def test_grant_statements_schema_level():
    stmts = build_grant_statements("alice@example.com", "prod", "sales", None)
    assert any("USE SCHEMA ON SCHEMA `prod`.`sales`" in s for s in stmts)
    assert any("SELECT ON SCHEMA `prod`.`sales`" in s for s in stmts)


def test_grant_statements_table_level():
    stmts = build_grant_statements("alice@example.com", "prod", "sales", "orders")
    assert any("SELECT ON TABLE `prod`.`sales`.`orders`" in s for s in stmts)
    # A specific table grant should not also blanket-grant SELECT on the schema.
    assert not any("SELECT ON SCHEMA" in s for s in stmts)


def test_grant_statements_reject_injection():
    """Identifiers from form fields must be validated, not blindly quoted."""
    with pytest.raises(ValueError):
        build_grant_statements("alice@example.com", "prod`; DROP TABLE x; --", None, None)


def test_revoke_statements_only_select_at_granted_level():
    grant = DatabricksGrantRecord(
        action_request_urn="urn:li:actionRequest:001",
        principal="alice@example.com",
        catalog="prod",
        schema_name="sales",
        table=None,
        requestor_email="alice@example.com",
        granted_at_ms=0,
        expires_at_ms=None,
    )
    stmts = build_revoke_statements(grant)
    assert stmts == ["REVOKE SELECT ON SCHEMA `prod`.`sales` FROM `alice@example.com`"]


# ---------------------------------------------------------------------------
# Execution paths
# ---------------------------------------------------------------------------


def test_provision_dry_run_does_not_execute(sql_conn):
    conn, cursor = sql_conn
    provisioning = DatabricksProvisioningConfig(dry_run=True, grant_method="sql")
    stmts = provision_access(
        sql_conn=conn,
        workspace_client=None,
        principal="alice@example.com",
        catalog="prod",
        schema="sales",
        table=None,
        provisioning=provisioning,
    )
    assert stmts
    cursor.execute.assert_not_called()


def test_provision_sql_executes_each_statement(sql_conn):
    conn, cursor = sql_conn
    provisioning = DatabricksProvisioningConfig(dry_run=False, grant_method="sql")
    stmts = provision_access(
        sql_conn=conn,
        workspace_client=None,
        principal="alice@example.com",
        catalog="prod",
        schema="sales",
        table=None,
        provisioning=provisioning,
    )
    assert cursor.execute.call_count == len(stmts)


def test_provision_sdk_calls_grants_api():
    provisioning = DatabricksProvisioningConfig(dry_run=False, grant_method="sdk")
    workspace_client = MagicMock()
    pytest.importorskip("databricks.sdk.service.catalog")
    provision_access(
        sql_conn=None,
        workspace_client=workspace_client,
        principal="alice@example.com",
        catalog="prod",
        schema="sales",
        table="orders",
        provisioning=provisioning,
    )
    # catalog USE_CATALOG + schema USE_SCHEMA + table SELECT = 3 updates
    assert workspace_client.grants.update.call_count == 3


def test_revoke_sql_executes(sql_conn):
    conn, cursor = sql_conn
    provisioning = DatabricksProvisioningConfig(dry_run=False, grant_method="sql")
    grant = DatabricksGrantRecord(
        action_request_urn="urn:li:actionRequest:002",
        principal="bob@example.com",
        catalog="prod",
        schema_name=None,
        table=None,
        requestor_email="bob@example.com",
        granted_at_ms=0,
        expires_at_ms=None,
    )
    revoke_access(
        sql_conn=conn,
        workspace_client=None,
        grant=grant,
        provisioning=provisioning,
    )
    cursor.execute.assert_called_once()
    assert "REVOKE SELECT ON CATALOG `prod`" in cursor.execute.call_args[0][0]


# ---------------------------------------------------------------------------
# Group membership (SCIM)
# ---------------------------------------------------------------------------


def _membership_client(group_id="grp-1", user_id="usr-1"):
    wc = MagicMock()
    wc.groups.list.return_value = [MagicMock(id=group_id)]
    wc.users.list.return_value = [MagicMock(id=user_id)]
    return wc


def test_add_group_member_patches_group():
    pytest.importorskip("databricks.sdk.service")
    wc = _membership_client()
    add_group_member(wc, "analytics_team", "alice@example.com", dry_run=False)
    wc.groups.patch.assert_called_once()
    assert wc.groups.patch.call_args.kwargs["id"] == "grp-1"


def test_remove_group_member_patches_group():
    pytest.importorskip("databricks.sdk.service")
    wc = _membership_client()
    remove_group_member(wc, "analytics_team", "alice@example.com", dry_run=False)
    wc.groups.patch.assert_called_once()


def test_add_group_member_dry_run_skips_api():
    wc = MagicMock()
    add_group_member(wc, "analytics_team", "alice@example.com", dry_run=True)
    wc.groups.patch.assert_not_called()


def test_add_group_member_raises_when_group_missing():
    pytest.importorskip("databricks.sdk.service")
    wc = MagicMock()
    wc.groups.list.return_value = []
    with pytest.raises(ValueError):
        add_group_member(wc, "missing_group", "alice@example.com", dry_run=False)


# ---------------------------------------------------------------------------
# Permanent error detection
# ---------------------------------------------------------------------------


def test_permanent_error_for_missing_object():
    assert is_permanent_databricks_error(Exception("Catalog 'prod' does not exist")) is True


def test_permanent_error_for_invalid_identifier():
    assert is_permanent_databricks_error(ValueError("Invalid Databricks identifier")) is True


def test_transient_error_is_not_permanent():
    assert is_permanent_databricks_error(Exception("connection reset by peer")) is False
