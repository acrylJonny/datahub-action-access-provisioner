"""Unit tests for Snowflake persistent state table helpers."""

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import StateConfig
from action_access_provisioner.constants import SCHEMA_ALL as _SCHEMA_ALL
from action_access_provisioner.models import GrantRecord
from action_access_provisioner.snowflake import (
    ensure_state_tables,
    get_expired_grants,
    is_already_provisioned,
    is_sla_notified,
    record_grant,
    record_revocation,
    record_sla_notification,
)


@pytest.fixture
def state_config():
    return StateConfig(database="TEST_DB", **{"schema": "AP_STATE"})


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda self: self
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn, cursor


@pytest.fixture
def grant_with_expiry():
    return GrantRecord(
        action_request_urn="urn:li:actionRequest:001",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema="SALES",
        requestor_email="alice@example.com",
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=int(time.time() * 1000) + 30 * 86_400_000,
    )


@pytest.fixture
def grant_no_expiry():
    return GrantRecord(
        action_request_urn="urn:li:actionRequest:002",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema=None,
        requestor_email=None,
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=None,
    )


# ---------------------------------------------------------------------------
# ensure_state_tables
# ---------------------------------------------------------------------------


def test_ensure_state_tables_creates_both_tables(mock_conn, state_config):
    conn, cursor = mock_conn
    ensure_state_tables(conn, state_config)
    assert cursor.execute.call_count == 2
    stmts = [c[0][0] for c in cursor.execute.call_args_list]
    assert any("ACCESS_PROVISIONER_GRANTS" in s for s in stmts)
    assert any("ACCESS_PROVISIONER_SLA_NOTIFICATIONS" in s for s in stmts)


# ---------------------------------------------------------------------------
# is_already_provisioned — checks LATEST_ACTION_REQUEST_URN, not PK
# ---------------------------------------------------------------------------


def test_is_already_provisioned_true(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (1,)
    assert is_already_provisioned(conn, "urn:li:actionRequest:001", state_config) is True
    sql = cursor.execute.call_args[0][0]
    assert "LATEST_ACTION_REQUEST_URN" in sql
    assert "REVOKED_AT IS NULL" in sql


def test_is_already_provisioned_false_for_new_urn(mock_conn, state_config):
    """A new URN (extension/re-request) returns False even if same role/db/schema exists."""
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (0,)
    assert is_already_provisioned(conn, "urn:li:actionRequest:NEW", state_config) is False


# ---------------------------------------------------------------------------
# record_grant — MERGE on natural key (role, db, schema)
# ---------------------------------------------------------------------------


def test_record_grant_merges_on_natural_key(mock_conn, state_config, grant_with_expiry):
    conn, cursor = mock_conn
    record_grant(conn, grant_with_expiry, state_config)
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    # Natural key join condition
    assert "SNOWFLAKE_ROLE" in sql
    assert "SNOWFLAKE_DATABASE" in sql
    assert "SNOWFLAKE_SCHEMA" in sql
    # On match it should update the URN column
    assert "LATEST_ACTION_REQUEST_URN" in sql
    # On match it must clear REVOKED_AT so re-requests regain access
    assert "REVOKED_AT" in sql and "NULL" in sql


def test_record_grant_without_expiry_uses_null(mock_conn, state_config, grant_no_expiry):
    conn, cursor = mock_conn
    record_grant(conn, grant_no_expiry, state_config)
    cursor.execute.assert_called_once()
    sql, params = cursor.execute.call_args[0]
    # EXPIRES_AT expressions should be NULL literals, not a placeholder
    assert "NULL" in sql
    # Param count must equal the number of %s placeholders:
    # 3 (USING) + 3 (MATCHED SET) + 6 (NOT MATCHED VALUES) = 12
    assert sql.count("%s") == len(params), (
        f"Param mismatch: {sql.count('%s')} placeholders but {len(params)} params"
    )


def test_record_grant_with_expiry_param_count(mock_conn, state_config, grant_with_expiry):
    conn, cursor = mock_conn
    record_grant(conn, grant_with_expiry, state_config)
    cursor.execute.assert_called_once()
    sql, params = cursor.execute.call_args[0]
    # With expiry: 3 (USING) + 4 (MATCHED SET incl. expires) + 7 (NOT MATCHED incl. expires) = 14
    assert sql.count("%s") == len(params), (
        f"Param mismatch: {sql.count('%s')} placeholders but {len(params)} params"
    )


def test_record_grant_schema_none_uses_sentinel(mock_conn, state_config, grant_no_expiry):
    """None schema must be stored as _SCHEMA_ALL sentinel so it fits in the composite PK."""
    conn, cursor = mock_conn
    record_grant(conn, grant_no_expiry, state_config)
    params = cursor.execute.call_args[0][1]
    # _SCHEMA_ALL sentinel should appear in params where schema is expected
    assert _SCHEMA_ALL in params


# ---------------------------------------------------------------------------
# Extension scenario: second request for same combo updates the row
# ---------------------------------------------------------------------------


def test_record_grant_extension_updates_expires_at(mock_conn, state_config):
    """A second call for the same role/db/schema (extension) updates EXPIRES_AT in place."""
    conn, cursor = mock_conn

    original = GrantRecord(
        action_request_urn="urn:li:actionRequest:001",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema="SALES",
        requestor_email="alice@example.com",
        granted_at_ms=int(time.time() * 1000) - 86_400_000,
        expires_at_ms=int(time.time() * 1000) + 5 * 86_400_000,  # expires in 5 days
    )
    extension = GrantRecord(
        action_request_urn="urn:li:actionRequest:002",  # new URN
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema="SALES",
        requestor_email="alice@example.com",
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=int(time.time() * 1000) + 60 * 86_400_000,  # extended to 60 days
    )

    record_grant(conn, original, state_config)
    record_grant(conn, extension, state_config)

    # Both calls should use the same MERGE statement structure
    assert cursor.execute.call_count == 2
    for c in cursor.execute.call_args_list:
        sql = c[0][0]
        assert "MERGE INTO" in sql
        # Both should join on the same natural key columns
        assert "SNOWFLAKE_ROLE" in sql


# ---------------------------------------------------------------------------
# get_expired_grants — sentinel conversion
# ---------------------------------------------------------------------------


def test_get_expired_grants_converts_sentinel_schema_to_none(mock_conn, state_config):
    """Rows stored with _SCHEMA_ALL sentinel must come back as schema=None."""
    conn, cursor = mock_conn
    now = datetime.now(tz=timezone.utc)
    expired_dt = datetime.fromtimestamp((int(time.time() * 1000) - 1000) / 1000, tz=timezone.utc)
    cursor.fetchall.return_value = [
        (
            "urn:li:actionRequest:001",
            "ANALYST",
            "PROD",
            _SCHEMA_ALL,
            "bob@example.com",
            now,
            expired_dt,
        )
    ]
    grants = get_expired_grants(conn, state_config)
    assert len(grants) == 1
    assert grants[0].snowflake_schema is None  # sentinel converted back


def test_get_expired_grants_preserves_real_schema(mock_conn, state_config):
    conn, cursor = mock_conn
    now = datetime.now(tz=timezone.utc)
    expired_dt = datetime.fromtimestamp((int(time.time() * 1000) - 1000) / 1000, tz=timezone.utc)
    cursor.fetchall.return_value = [
        ("urn:li:actionRequest:002", "ANALYST", "PROD", "SALES", None, now, expired_dt)
    ]
    grants = get_expired_grants(conn, state_config)
    assert grants[0].snowflake_schema == "SALES"


def test_get_expired_grants_empty(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchall.return_value = []
    assert get_expired_grants(conn, state_config) == []


# ---------------------------------------------------------------------------
# record_revocation — keyed on natural key via GrantRecord
# ---------------------------------------------------------------------------


def test_record_revocation_uses_natural_key(mock_conn, state_config, grant_with_expiry):
    conn, cursor = mock_conn
    record_revocation(conn, grant_with_expiry, state_config)
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    params = cursor.execute.call_args[0][1]
    assert "REVOKED_AT" in sql
    assert "SNOWFLAKE_ROLE" in sql
    assert grant_with_expiry.snowflake_role in params


# ---------------------------------------------------------------------------
# SLA helpers
# ---------------------------------------------------------------------------


def test_is_sla_notified_true(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (1,)
    assert is_sla_notified(conn, "urn:li:actionRequest:007", "warning", state_config) is True


def test_is_sla_notified_false(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (0,)
    assert is_sla_notified(conn, "urn:li:actionRequest:008", "warning", state_config) is False


def test_record_sla_notification_inserts(mock_conn, state_config):
    conn, cursor = mock_conn
    record_sla_notification(conn, "urn:li:actionRequest:009", "escalation", state_config)
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "INSERT INTO" in sql
    assert "ACCESS_PROVISIONER_SLA_NOTIFICATIONS" in sql
