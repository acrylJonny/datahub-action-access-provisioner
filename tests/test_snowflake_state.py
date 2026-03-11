"""Unit tests for Snowflake persistent state table helpers."""

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import StateConfig
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
    return StateConfig(
        database="TEST_DB",
        **{"schema": "AP_STATE"},  # alias field
    )


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda self: self
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn, cursor


def test_ensure_state_tables_creates_both_tables(mock_conn, state_config):
    conn, cursor = mock_conn
    ensure_state_tables(conn, state_config)
    # Two DDL statements should be executed
    assert cursor.execute.call_count == 2
    stmts = [c[0][0] for c in cursor.execute.call_args_list]
    assert any("ACCESS_PROVISIONER_GRANTS" in s for s in stmts)
    assert any("ACCESS_PROVISIONER_SLA_NOTIFICATIONS" in s for s in stmts)


def test_is_already_provisioned_true(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (1,)
    result = is_already_provisioned(conn, "urn:li:actionRequest:001", state_config)
    assert result is True


def test_is_already_provisioned_false(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchone.return_value = (0,)
    result = is_already_provisioned(conn, "urn:li:actionRequest:002", state_config)
    assert result is False


def test_record_grant_calls_merge(mock_conn, state_config):
    conn, cursor = mock_conn
    grant = GrantRecord(
        action_request_urn="urn:li:actionRequest:003",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema="SALES",
        requestor_email="alice@example.com",
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=int(time.time() * 1000) + 30 * 86_400_000,
    )
    record_grant(conn, grant, state_config)
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "MERGE INTO" in sql
    assert "ACCESS_PROVISIONER_GRANTS" in sql


def test_record_grant_without_expiry(mock_conn, state_config):
    conn, cursor = mock_conn
    grant = GrantRecord(
        action_request_urn="urn:li:actionRequest:004",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema=None,
        requestor_email=None,
        granted_at_ms=int(time.time() * 1000),
        expires_at_ms=None,
    )
    record_grant(conn, grant, state_config)
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "NULL" in sql  # expires_at should be NULL


def test_get_expired_grants_parses_rows(mock_conn, state_config):
    conn, cursor = mock_conn
    now = datetime.now(tz=timezone.utc)
    expired_dt = datetime.fromtimestamp((int(time.time() * 1000) - 1000) / 1000, tz=timezone.utc)
    cursor.fetchall.return_value = [
        (
            "urn:li:actionRequest:005",
            "OLD_ROLE",
            "PROD",
            "SALES",
            "frank@example.com",
            now,
            expired_dt,
        )
    ]
    grants = get_expired_grants(conn, state_config)
    assert len(grants) == 1
    assert grants[0].snowflake_role == "OLD_ROLE"
    assert grants[0].expires_at_ms is not None


def test_get_expired_grants_empty(mock_conn, state_config):
    conn, cursor = mock_conn
    cursor.fetchall.return_value = []
    grants = get_expired_grants(conn, state_config)
    assert grants == []


def test_record_revocation_updates_table(mock_conn, state_config):
    conn, cursor = mock_conn
    record_revocation(conn, "urn:li:actionRequest:006", state_config)
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "REVOKED_AT" in sql


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
