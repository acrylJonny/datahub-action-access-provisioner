import re
import time
from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import DatabricksStateConfig
from action_access_provisioner.databricks import (
    claim_stage,
    ensure_state_tables,
    get_expired_grants,
    get_expired_memberships,
    is_already_provisioned,
    is_membership_provisioned,
    is_provisioning_failed,
    is_sla_notified,
    is_stage_processed,
    record_grant,
    record_membership,
    record_provisioning_error,
    record_revocation,
    record_sla_notification,
)
from action_access_provisioner.models import (
    DatabricksGrantRecord,
    DatabricksGroupMembershipRecord,
)
from action_access_provisioner.sql.databricks.ddl import SCHEMA_ALL, TABLE_ALL


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
        schema_name="sales",
        table=None,
        requestor_email="alice@example.com",
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=int(time.time() * 1000) + 30 * 86_400_000,
    )


def test_ensure_state_tables_creates_all_tables(mock_conn, state_config):
    conn, cursor = mock_conn
    ensure_state_tables(conn, state_config)
    stmts = [c[0][0] for c in cursor.execute.call_args_list]
    assert cursor.execute.call_count == 5
    assert any("access_provisioner_grants" in s for s in stmts)
    assert any("access_provisioner_sla_notifications" in s for s in stmts)
    assert any("access_provisioner_errors" in s for s in stmts)
    assert any("access_provisioner_group_memberships" in s for s in stmts)
    assert any("access_provisioner_ledger" in s for s in stmts)
    assert all("USING DELTA" in s for s in stmts)


def test_is_stage_processed_true(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = ("1",)
    assert (
        is_stage_processed(conn, "urn:li:actionRequest:001", "approval_notified", state_config)
        is True
    )


def test_claim_stage_dedups_when_already_claimed(mock_conn, state_config):
    """A replayed event whose stage is already in the ledger must not run the MERGE insert."""
    conn, cursor = mock_conn
    cursor.fetchone.return_value = ("1",)  # is_stage_processed -> True
    assert claim_stage(conn, "urn:li:actionRequest:001", "approval_notified", state_config) is False
    # Only the COUNT ran; the MERGE insert was short-circuited.
    assert cursor.execute.call_count == 1
    assert "MERGE" not in cursor.execute.call_args[0][0].upper()


def test_claim_stage_first_time_runs_merge(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = ("0",)  # is_stage_processed -> False
    assert claim_stage(conn, "urn:li:actionRequest:001", "approval_notified", state_config) is True
    # COUNT then MERGE.
    assert cursor.execute.call_count == 2
    assert "MERGE" in cursor.execute.call_args[0][0].upper()


def test_record_membership_merges_on_user_and_group(mock_conn, state_config):
    conn, cursor = mock_conn
    membership = DatabricksGroupMembershipRecord(
        action_request_urn="urn:li:actionRequest:m1",
        user_email="alice@example.com",
        group_name="analytics_team",
        added_at_ms=int(time.time() * 1000),
        expires_at_ms=int(time.time() * 1000) + 14 * 86_400_000,
    )
    record_membership(conn, membership, state_config)
    sql = cursor.execute.call_args_list[0][0][0]
    assert "MERGE INTO" in sql
    assert "user_email" in sql and "group_name" in sql


def test_get_expired_memberships_parses_rows(mock_conn, state_config):
    conn, cursor = mock_conn
    # Connector returns the CAST(... AS STRING) bigint columns as strings.
    cursor.fetchall.return_value = [
        ("urn:li:actionRequest:m1", "alice@example.com", "analytics_team", "1000", "2000")
    ]
    expired = get_expired_memberships(conn, state_config)
    assert len(expired) == 1
    assert expired[0].user_email == "alice@example.com"
    assert expired[0].group_name == "analytics_team"
    assert expired[0].expires_at_ms == 2000


def test_is_membership_provisioned_uses_urn_and_removed_guard(mock_conn, state_config):
    conn, cursor = mock_conn
    # First fetchone answers the ledger probe (miss), second the memberships table.
    cursor.fetchone.side_effect = [("0",), ("1",)]
    assert is_membership_provisioned(conn, "urn:li:actionRequest:m1", state_config) is True
    sql = cursor.execute.call_args[0][0]
    assert "latest_action_request_urn" in sql
    assert "removed_at_ms IS NULL" in sql


def test_is_already_provisioned_uses_urn_and_revoked_guard(mock_conn, state_config):
    conn, cursor = mock_conn
    # The connector returns the CAST(COUNT(*) AS STRING) value as a string.
    # First fetchone answers the ledger probe (miss), second the grants table.
    cursor.fetchone.side_effect = [("0",), ("1",)]
    assert is_already_provisioned(conn, "urn:li:actionRequest:001", state_config) is True
    sql = cursor.execute.call_args[0][0]
    assert "latest_action_request_urn" in sql
    assert "revoked_at_ms IS NULL" in sql


def test_is_already_provisioned_short_circuits_on_ledger(mock_conn, state_config):
    # A superseded request is invisible to the grants table, whose
    # latest_action_request_urn now names a newer request for the same target.
    # The ledger must answer on its own or the grant is re-executed every pass.
    conn, cursor = mock_conn
    cursor.fetchone.return_value = ("1",)
    assert is_already_provisioned(conn, "urn:li:actionRequest:001", state_config) is True
    assert cursor.execute.call_count == 1
    assert "access_provisioner_ledger" in cursor.execute.call_args[0][0]


def test_record_grant_merges_on_natural_key(mock_conn, state_config, grant_with_expiry):
    conn, cursor = mock_conn
    record_grant(conn, grant_with_expiry, state_config)
    sql, params = cursor.execute.call_args_list[0][0]
    assert "MERGE INTO" in sql
    for col in ("grantee", "dbx_catalog", "dbx_schema", "dbx_table"):
        assert col in sql
    assert params["expires"] == grant_with_expiry.expires_at_ms


def test_record_grant_stamps_the_request_ledger(mock_conn, state_config, grant_with_expiry):
    # Without this stamp, idempotency falls back to the grants table's
    # latest_action_request_urn, which cannot recognise a superseded request.
    conn, cursor = mock_conn
    cursor.fetchone.return_value = ("0",)
    record_grant(conn, grant_with_expiry, state_config)
    ledger_calls = [
        c for c in cursor.execute.call_args_list if "access_provisioner_ledger" in c[0][0]
    ]
    assert any(
        c[0][1]["stage"] == "provisioned" and c[0][1]["urn"] == grant_with_expiry.action_request_urn
        for c in ledger_calls
    )


def test_record_grant_no_expiry_inlines_null_and_omits_param(mock_conn, state_config):
    conn, cursor = mock_conn
    grant = DatabricksGrantRecord(
        action_request_urn="urn:li:actionRequest:002",
        principal="alice@example.com",
        catalog="prod",
        schema_name=None,
        table=None,
        requestor_email=None,
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=None,
    )
    record_grant(conn, grant, state_config)
    sql, params = cursor.execute.call_args_list[0][0]
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
            "1000",
            "2000",
        )
    ]
    grants = get_expired_grants(conn, state_config)
    assert len(grants) == 1
    assert grants[0].schema_name is None
    assert grants[0].table is None
    # Timestamps arrive as strings (CAST AS STRING) but are parsed back to int.
    assert grants[0].granted_at_ms == 1000
    assert grants[0].expires_at_ms == 2000


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
    cursor.fetchone.return_value = ("0",)
    assert is_sla_notified(conn, "urn:li:actionRequest:008", "warning", state_config) is False


def test_integer_result_columns_are_cast_to_string(mock_conn, state_config):
    # databricks-sql-connector 2.9.x crashes converting *any* int result column
    # under numpy 2.x (pandas.to_numpy(na_value=None)). Every SELECT that would
    # otherwise return an int must CAST it to STRING; guard against regressions.
    conn, cursor = mock_conn
    cursor.fetchone.return_value = ("0",)
    cursor.fetchall.return_value = []
    int_returning_calls = [
        lambda: is_already_provisioned(conn, "urn:li:actionRequest:001", state_config),
        lambda: is_sla_notified(conn, "urn:li:actionRequest:008", "warning", state_config),
        lambda: is_provisioning_failed(conn, "urn:li:actionRequest:011", state_config),
        lambda: get_expired_grants(conn, state_config),
    ]
    for call in int_returning_calls:
        cursor.reset_mock()
        cursor.fetchone.return_value = ("0",)
        cursor.fetchall.return_value = []
        call()
        sql = cursor.execute.call_args[0][0]
        assert "CAST(" in sql and "AS STRING)" in sql, f"int column not cast in: {sql}"


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
