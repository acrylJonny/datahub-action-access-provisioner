import re
import time
from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import DatabricksStateConfig
from action_access_provisioner.constants import SCHEMA_ALL, TABLE_ALL
from action_access_provisioner.databricks import (
    ensure_state_tables,
    get_expired_grants,
    is_already_provisioned,
    is_provisioning_failed,
    is_sla_notified,
    record_grant,
    record_provisioning_error,
    record_revocation,
    record_sla_notification,
)
from action_access_provisioner.models import DatabricksGrantRecord


@pytest.fixture
def state_config():
    return DatabricksStateConfig(catalog="test_cat", **{"schema": "ap_state"})


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.close = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


@pytest.fixture
def grant_with_expiry():
    return DatabricksGrantRecord(
        action_request_urn="urn:li:actionRequest:001",
        principal="alice@example.com",
        catalog="prod",
        schema="sales",
        table=None,
        requestor_email="alice@example.com",
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=int(time.time() * 1000) + 30 * 86_400_000,
    )


def test_ensure_state_tables_creates_three_tables(mock_conn, state_config):
    conn, cursor = mock_conn
    ensure_state_tables(conn, state_config)
    stmts = [c[0][0] for c in cursor.execute.call_args_list]
    assert cursor.execute.call_count == 3
    assert any("access_provisioner_grants" in s for s in stmts)
    assert any("access_provisioner_sla_notifications" in s for s in stmts)
    assert any("access_provisioner_errors" in s for s in stmts)
    assert all("USING DELTA" in s for s in stmts)


def test_is_already_provisioned_uses_urn_and_revoked_guard(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (1,)
    assert is_already_provisioned(conn, "urn:li:actionRequest:001", state_config) is True
    sql = cursor.execute.call_args[0][0]
    assert "latest_action_request_urn" in sql
    assert "revoked_at_ms IS NULL" in sql


def test_record_grant_merges_on_natural_key(mock_conn, state_config, grant_with_expiry):
    conn, cursor = mock_conn
    record_grant(conn, grant_with_expiry, state_config)
    sql, params = cursor.execute.call_args[0]
    assert "MERGE INTO" in sql
    for col in ("grantee", "dbx_catalog", "dbx_schema", "dbx_table"):
        assert col in sql
    assert params["expires"] == grant_with_expiry.expires_at_ms


def test_record_grant_no_expiry_inlines_null_and_omits_param(mock_conn, state_config):
    conn, cursor = mock_conn
    grant = DatabricksGrantRecord(
        action_request_urn="urn:li:actionRequest:002",
        principal="alice@example.com",
        catalog="prod",
        schema=None,
        table=None,
        requestor_email=None,
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=None,
    )
    record_grant(conn, grant, state_config)
    sql, params = cursor.execute.call_args[0]
    assert "expires" not in params  # inlined as NULL, not bound
    # None schema/table stored as sentinels so the natural key has no NULLs.
    assert params["schema"] == SCHEMA_ALL
    assert params["tbl"] == TABLE_ALL


def test_get_expired_grants_converts_sentinels_back_to_none(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchall.return_value = [
        (
            "urn:li:actionRequest:001",
            "alice@example.com",
            "prod",
            SCHEMA_ALL,
            TABLE_ALL,
            "alice@example.com",
            1000,
            2000,
        )
    ]
    grants = get_expired_grants(conn, state_config)
    assert len(grants) == 1
    assert grants[0].schema is None
    assert grants[0].table is None


def test_record_revocation_keys_on_natural_combo(mock_conn, state_config, grant_with_expiry):
    conn, cursor = mock_conn
    record_revocation(conn, grant_with_expiry, state_config)
    sql, params = cursor.execute.call_args[0]
    assert "revoked_at_ms" in sql
    assert params["grantee"] == "alice@example.com"
    assert params["catalog"] == "prod"


def test_sla_notification_is_idempotent_merge(mock_conn, state_config):
    conn, cursor = mock_conn
    record_sla_notification(conn, "urn:li:actionRequest:009", "escalation", state_config)
    sql = cursor.execute.call_args[0][0]
    assert "MERGE INTO" in sql
    assert "WHEN NOT MATCHED" in sql


def test_is_sla_notified_false(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (0,)
    assert is_sla_notified(conn, "urn:li:actionRequest:008", "warning", state_config) is False


def test_record_provisioning_error_merge(mock_conn, state_config):
    conn, cursor = mock_conn
    record_provisioning_error(
        conn, "urn:li:actionRequest:012", "NOT_FOUND", "Catalog does not exist", state_config
    )
    sql, params = cursor.execute.call_args[0]
    assert "MERGE INTO" in sql
    assert params["urn"] == "urn:li:actionRequest:012"
    assert params["msg"] == "Catalog does not exist"


def test_parameterized_sql_uses_pyformat_markers(mock_conn, state_config, grant_with_expiry):
    # The DataHub Cloud executor pins databricks-sql-connector 2.9.6, which only
    # binds pyformat (%(name)s) markers — native ":name" markers reach the server
    # unbound (UNBOUND_SQL_PARAMETER). Every bound param must use %(name)s and no
    # bare :name marker may slip back in.
    conn, cursor = mock_conn
    calls = [
        lambda: is_already_provisioned(conn, "urn:li:actionRequest:001", state_config),
        lambda: record_grant(conn, grant_with_expiry, state_config),
        lambda: get_expired_grants(conn, state_config),
        lambda: record_revocation(conn, grant_with_expiry, state_config),
        lambda: is_sla_notified(conn, "urn:li:actionRequest:008", "warning", state_config),
        lambda: record_sla_notification(
            conn, "urn:li:actionRequest:009", "escalation", state_config
        ),
        lambda: is_provisioning_failed(conn, "urn:li:actionRequest:011", state_config),
        lambda: record_provisioning_error(
            conn, "urn:li:actionRequest:012", "X", "boom", state_config
        ),
    ]
    for call in calls:
        cursor.reset_mock()
        cursor.fetchone.return_value = (0,)
        cursor.fetchall.return_value = []
        call()
        sql, params = cursor.execute.call_args[0]
        for key in params:
            assert f"%({key})s" in sql, f"{key} not bound as pyformat marker in: {sql}"
        assert not re.search(r"[\s(]:[a-z_]+\b", sql), f"stray :name marker in: {sql}"
