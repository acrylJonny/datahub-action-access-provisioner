"""Unit tests for Snowflake GRANT/REVOKE helpers."""

from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import SnowflakeProvisioningConfig
from action_access_provisioner.models import GrantRecord
from action_access_provisioner.snowflake import (
    _execute,
    get_user_default_role,
    provision_access,
    revoke_access,
)


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: self
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture
def live_provisioning():
    return SnowflakeProvisioningConfig(dry_run=False)


@pytest.fixture
def dry_run_provisioning():
    return SnowflakeProvisioningConfig(dry_run=True)


def test_execute_dry_run_does_not_call_cursor(mock_conn, dry_run_provisioning):
    _execute(mock_conn, "GRANT USAGE ON DATABASE FOO TO ROLE BAR", dry_run_provisioning)
    mock_conn.cursor.assert_not_called()


def test_execute_live_calls_cursor(mock_conn, live_provisioning):
    cursor = MagicMock()
    mock_conn.cursor.return_value = cursor
    _execute(mock_conn, "GRANT USAGE ON DATABASE FOO TO ROLE BAR", live_provisioning)
    cursor.execute.assert_called_once_with("GRANT USAGE ON DATABASE FOO TO ROLE BAR")


def test_provision_access_with_schema(mock_conn, dry_run_provisioning):
    statements = provision_access(
        conn=mock_conn,
        role="ANALYST",
        database="PROD",
        schema="SALES",
        warehouse=None,
        provisioning=dry_run_provisioning,
    )
    assert any("DATABASE PROD" in s for s in statements)
    assert any("SCHEMA PROD.SALES" in s for s in statements)


def test_provision_access_without_schema_grants_all_schemas(mock_conn, dry_run_provisioning):
    statements = provision_access(
        conn=mock_conn,
        role="ANALYST",
        database="PROD",
        schema=None,
        warehouse=None,
        provisioning=dry_run_provisioning,
    )
    assert any("DATABASE PROD" in s for s in statements)


def test_provision_access_with_warehouse(mock_conn, dry_run_provisioning):
    statements = provision_access(
        conn=mock_conn,
        role="ANALYST",
        database="PROD",
        schema=None,
        warehouse="COMPUTE_WH",
        provisioning=dry_run_provisioning,
    )
    assert any("WAREHOUSE COMPUTE_WH" in s for s in statements)


def test_revoke_access_with_schema(mock_conn, dry_run_provisioning):
    grant = GrantRecord(
        action_request_urn="urn:li:actionRequest:001",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema="SALES",
        requestor_email=None,
        granted_at_ms=0,
        expires_at_ms=None,
    )
    statements = revoke_access(mock_conn, grant, dry_run_provisioning)
    assert any("REVOKE" in s for s in statements)
    assert any("DATABASE PROD" in s for s in statements)


def test_revoke_access_without_schema(mock_conn, dry_run_provisioning):
    grant = GrantRecord(
        action_request_urn="urn:li:actionRequest:002",
        snowflake_role="ANALYST",
        snowflake_database="PROD",
        snowflake_schema=None,
        requestor_email=None,
        granted_at_ms=0,
        expires_at_ms=None,
    )
    statements = revoke_access(mock_conn, grant, dry_run_provisioning)
    assert any("REVOKE USAGE ON DATABASE PROD" in s for s in statements)


# ---------------------------------------------------------------------------
# get_user_default_role
# ---------------------------------------------------------------------------


def _make_describe_conn(rows: list[tuple]) -> MagicMock:
    """Build a mock connection whose cursor().fetchall() returns *rows*.

    _cursor() in snowflake.py calls conn.cursor() directly (not as a context
    manager), so we only need conn.cursor.return_value = cursor.
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_get_user_default_role_returns_role():
    rows = [
        ("NAME", "JOHN.DOE", "null"),
        ("LOGIN_NAME", "JOHN.DOE@COMPANY.COM", "null"),
        ("DEFAULT_ROLE", "ANALYST_ROLE", "null"),
        ("DEFAULT_WAREHOUSE", "COMPUTE_WH", "null"),
    ]
    conn = _make_describe_conn(rows)
    assert get_user_default_role(conn, "john.doe") == "ANALYST_ROLE"


def test_get_user_default_role_missing_row_returns_none():
    rows = [
        ("NAME", "JOHN.DOE", "null"),
        ("DEFAULT_WAREHOUSE", "COMPUTE_WH", "null"),
    ]
    conn = _make_describe_conn(rows)
    assert get_user_default_role(conn, "john.doe") is None


def test_get_user_default_role_empty_value_returns_none():
    rows = [("DEFAULT_ROLE", "", "null")]
    conn = _make_describe_conn(rows)
    assert get_user_default_role(conn, "john.doe") is None


def test_get_user_default_role_snowflake_error_returns_none():
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("002003: user not found")
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    assert get_user_default_role(conn, "unknown_user") is None
