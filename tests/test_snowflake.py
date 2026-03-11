"""Unit tests for Snowflake GRANT/REVOKE helpers."""

from unittest.mock import MagicMock

import pytest

from action_access_provisioner.config import SnowflakeProvisioningConfig
from action_access_provisioner.models import GrantRecord
from action_access_provisioner.snowflake import (
    _execute,
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
