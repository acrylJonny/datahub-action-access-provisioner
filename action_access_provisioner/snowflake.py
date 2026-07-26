import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from action_access_provisioner.config import (
    SnowflakeConnectionConfig,
    SnowflakeProvisioningConfig,
    StateConfig,
)
from action_access_provisioner.models import GrantRecord
from action_access_provisioner.sql.snowflake import dcl, ddl, dml
from action_access_provisioner.sql.snowflake.ddl import SCHEMA_ALL

logger = logging.getLogger(__name__)


@contextmanager
def _cursor(conn) -> Generator[Any, None, None]:
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def get_connection(connection_config: SnowflakeConnectionConfig):
    """Create and return a Snowflake connection from the provided config."""
    return connection_config.get_native_connection()


def get_user_default_role(conn, username: str) -> str | None:
    """Look up the DEFAULT_ROLE of a Snowflake user.

    Executes ``DESCRIBE USER "<username>"`` and returns the value of the
    DEFAULT_ROLE property. Returns None if the user does not exist, has no
    default role set, or the caller lacks permission to describe the user.

    Requires the provisioner role to have MANAGE GRANTS (or SECURITYADMIN+).
    """
    with _cursor(conn) as cur:
        try:
            cur.execute(dml.DESCRIBE_USER.format(username=username))
            for row in cur.fetchall():
                # DESCRIBE USER returns rows of (property, value, default)
                if row[0] == "DEFAULT_ROLE" and row[1]:
                    role = str(row[1]).strip()
                    return role if role else None
        except Exception as exc:
            logger.warning(f"[Snowflake] Could not describe user '{username}': {exc}")
    return None


def grant_role_to_role(
    conn,
    target_role: str,
    grantee_role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT <target_role> TO ROLE <grantee_role>."""
    statement = dcl.GRANT_ROLE_TO_ROLE.format(target_role=target_role, grantee_role=grantee_role)
    _execute(conn, statement, provisioning)


def grant_database_usage(
    conn,
    database: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT USAGE ON DATABASE <database> TO ROLE <role>."""
    statement = dcl.GRANT_DATABASE_USAGE.format(database=database, role=role)
    _execute(conn, statement, provisioning)


def grant_schema_usage(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT USAGE ON SCHEMA <database>.<schema> TO ROLE <role>."""
    statement = dcl.GRANT_SCHEMA_USAGE.format(database=database, schema=schema, role=role)
    _execute(conn, statement, provisioning)


def grant_schema_select(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT SELECT ON ALL TABLES IN SCHEMA <database>.<schema> TO ROLE <role>."""
    statement = dcl.GRANT_SCHEMA_SELECT_ALL.format(database=database, schema=schema, role=role)
    _execute(conn, statement, provisioning)
    future_statement = dcl.GRANT_SCHEMA_SELECT_FUTURE.format(
        database=database, schema=schema, role=role
    )
    _execute(conn, future_statement, provisioning)


def grant_warehouse_usage(
    conn,
    warehouse: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>."""
    statement = dcl.GRANT_WAREHOUSE_USAGE.format(warehouse=warehouse, role=role)
    _execute(conn, statement, provisioning)


def revoke_database_usage(
    conn,
    database: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """REVOKE USAGE ON DATABASE <database> FROM ROLE <role>."""
    statement = dcl.REVOKE_DATABASE_USAGE.format(database=database, role=role)
    _execute(conn, statement, provisioning)


def revoke_schema_usage(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """REVOKE USAGE ON SCHEMA <database>.<schema> FROM ROLE <role>."""
    statement = dcl.REVOKE_SCHEMA_USAGE.format(database=database, schema=schema, role=role)
    _execute(conn, statement, provisioning)


def revoke_schema_select(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """REVOKE SELECT ON ALL TABLES IN SCHEMA from role."""
    statement = dcl.REVOKE_SCHEMA_SELECT_ALL.format(database=database, schema=schema, role=role)
    _execute(conn, statement, provisioning)
    future_statement = dcl.REVOKE_SCHEMA_SELECT_FUTURE.format(
        database=database, schema=schema, role=role
    )
    _execute(conn, future_statement, provisioning)


def provision_access(
    conn,
    role: str,
    database: str,
    schema: str | None,
    warehouse: str | None,
    provisioning: SnowflakeProvisioningConfig,
) -> list[str]:
    """
    Execute the full set of GRANT statements required to provision read access.

    Returns the list of SQL statements that were (or would be in dry_run mode) executed.
    """
    statements: list[str] = []

    grant_database_usage(conn, database, role, provisioning)
    statements.append(dcl.GRANT_DATABASE_USAGE.format(database=database, role=role))

    if schema:
        grant_schema_usage(conn, database, schema, role, provisioning)
        statements.append(
            dcl.GRANT_SCHEMA_USAGE.format(database=database, schema=schema, role=role)
        )
        grant_schema_select(conn, database, schema, role, provisioning)
        statements.append(
            dcl.GRANT_SCHEMA_SELECT_SUMMARY.format(database=database, schema=schema, role=role)
        )
    else:
        # No schema specified — grant at the database level
        all_schemas = dcl.GRANT_ALL_SCHEMAS_USAGE.format(database=database, role=role)
        _execute(conn, all_schemas, provisioning)
        statements.append(all_schemas)
        future_schemas = dcl.GRANT_FUTURE_SCHEMAS_USAGE.format(database=database, role=role)
        _execute(conn, future_schemas, provisioning)
        statements.append(future_schemas)

    if warehouse:
        grant_warehouse_usage(conn, warehouse, role, provisioning)
        statements.append(dcl.GRANT_WAREHOUSE_USAGE.format(warehouse=warehouse, role=role))

    return statements


def revoke_access(
    conn,
    grant: GrantRecord,
    provisioning: SnowflakeProvisioningConfig,
) -> list[str]:
    """
    Execute the REVOKE statements that mirror the original GRANT.

    Returns the list of SQL statements that were (or would be) executed.
    """
    statements: list[str] = []

    if grant.snowflake_schema:
        revoke_schema_select(
            conn,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.snowflake_role,
            provisioning,
        )
        statements.append(
            dcl.REVOKE_SCHEMA_SELECT_SUMMARY.format(
                database=grant.snowflake_database,
                schema=grant.snowflake_schema,
                role=grant.snowflake_role,
            )
        )
        revoke_schema_usage(
            conn,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.snowflake_role,
            provisioning,
        )
        statements.append(
            dcl.REVOKE_SCHEMA_USAGE.format(
                database=grant.snowflake_database,
                schema=grant.snowflake_schema,
                role=grant.snowflake_role,
            )
        )

    revoke_database_usage(conn, grant.snowflake_database, grant.snowflake_role, provisioning)
    statements.append(
        dcl.REVOKE_DATABASE_USAGE.format(
            database=grant.snowflake_database, role=grant.snowflake_role
        )
    )

    return statements


def _execute(
    conn,
    statement: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    if provisioning.dry_run:
        logger.info(f"[DRY RUN] {statement}")
        return

    logger.info(f"Executing: {statement}")
    with _cursor(conn) as cur:
        cur.execute(statement)


# ---------------------------------------------------------------------------
# Persistent state tables
# ---------------------------------------------------------------------------
# These tables survive across scheduled runs so that:
#   - We never re-grant access that was already provisioned (idempotency)
#   - We never re-send SLA emails that were already dispatched
#   - We can revoke grants whose expiry_at has passed even on a fresh invocation
#
# Grant table primary key design — natural key (ROLE, DATABASE, SCHEMA)
# -----------------------------------------------------------------------
# We intentionally key the grants table on the access combo
# (SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA) rather than on
# ACTION_REQUEST_URN.  This prevents a subtle bug in the extension and
# re-request flows:
#
#   Request A approved (expires in 30 days) → row inserted.
#   User submits Request B for the same access (extension, expires in 60 days).
#   → MERGE on natural key: LATEST_ACTION_REQUEST_URN = B, EXPIRES_AT = 60 days.
#   → Row is updated in place; there is still only ONE active grant row.
#   → Expiry monitor uses 60-day timer. The 30-day timer is gone. ✓
#
# If we had used ACTION_REQUEST_URN as PK, Request B would insert a second
# row, and the expiry monitor would fire on Request A's row after 30 days,
# revoking access that should be valid for another 30 days.
#


def ensure_state_tables(conn, state: StateConfig) -> None:
    """Create the grants, SLA-notification, errors, and ledger tables if they don't already exist."""
    with _cursor(conn) as cur:
        cur.execute(ddl.GRANTS_TABLE.format(table=state.qualified_grants_table))
        cur.execute(ddl.SLA_TABLE.format(table=state.qualified_sla_table))
        cur.execute(ddl.ERRORS_TABLE.format(table=state.qualified_errors_table))
        cur.execute(ddl.LEDGER_TABLE.format(table=state.qualified_ledger_table))
    logger.info(
        f"[State] State tables ready: {state.qualified_grants_table}, "
        f"{state.qualified_sla_table}, {state.qualified_errors_table}, "
        f"{state.qualified_ledger_table}"
    )


def is_stage_processed(conn, action_request_urn: str, stage: str, state: StateConfig) -> bool:
    """Return True if this (request, stage) has already been claimed in the ledger."""
    sql = dml.COUNT_LEDGER_STAGE.format(table=state.qualified_ledger_table)
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn, stage))
        row = cur.fetchone()
        return bool(row and row[0] > 0)


def claim_stage(conn, action_request_urn: str, stage: str, state: StateConfig) -> bool:
    """Atomically claim a processing stage for a request.

    Returns True if this call won the claim (the caller should now perform the
    stage's side effect exactly once), or False if it was already claimed by a
    previous run or a duplicate event. The claim is written *before* the side
    effect so a replayed event never triggers a second notification.
    """
    if is_stage_processed(conn, action_request_urn, stage, state):
        return False
    sql = dml.CLAIM_LEDGER_STAGE.format(table=state.qualified_ledger_table)
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn, stage, action_request_urn, stage))
        # rowcount is 1 when this INSERT won the claim, 0 if another writer beat us.
        claimed = getattr(cur, "rowcount", 1) != 0
    return claimed


def is_already_provisioned(conn, action_request_urn: str, state: StateConfig) -> bool:
    """
    Return True if this exact request URN has already been provisioned and the grant
    is still active (not revoked).

    A new request for the same access combo (extension / re-request) will have a
    different URN and will therefore return False — which is the correct behaviour
    since the MERGE in record_grant() will update the existing row in place.
    """
    sql = dml.COUNT_ACTIVE_GRANT.format(table=state.qualified_grants_table)
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn,))
        row = cur.fetchone()
        return bool(row and row[0] > 0)


def record_grant(conn, grant: GrantRecord, state: StateConfig) -> None:
    """
    Upsert a grant record into the state table, keyed on (ROLE, DATABASE, SCHEMA).

    Behaviour by scenario:
      - New grant:      inserts a fresh row.
      - Extension:      MERGE matches the existing active row and updates
                        LATEST_ACTION_REQUEST_URN, EXPIRES_AT, and clears REVOKED_AT.
      - Re-request after revocation: same MERGE path — REVOKED_AT is cleared and
                        the new expiry timer starts.
    """
    schema_key = grant.snowflake_schema or SCHEMA_ALL
    expires_str = (
        datetime.fromtimestamp(grant.expires_at_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if grant.expires_at_ms
        else None
    )
    granted_str = datetime.fromtimestamp(grant.granted_at_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    expires_expr = "%s::TIMESTAMP_NTZ" if expires_str else "NULL"
    sql = dml.MERGE_GRANT.format(table=state.qualified_grants_table, expires_expr=expires_expr)
    # WHEN MATCHED SET params: URN, email, granted_at (+ optional expires).
    # role/db/schema are NOT included here — they belong only in the USING clause.
    common = [
        grant.action_request_urn,
        grant.requestor_email,
        granted_str,
    ]
    if expires_str:
        common.append(expires_str)

    insert_vals = [
        grant.snowflake_role,
        grant.snowflake_database,
        schema_key,
        grant.action_request_urn,
        grant.requestor_email,
        granted_str,
    ]
    if expires_str:
        insert_vals.append(expires_str)

    params = tuple(
        [grant.snowflake_role, grant.snowflake_database, schema_key]  # USING source
        + common  # WHEN MATCHED
        + insert_vals  # WHEN NOT MATCHED
    )
    with _cursor(conn) as cur:
        cur.execute(sql, params)
    action = "updated (extension/re-request)" if expires_str else "recorded"
    logger.debug(
        f"[State] Grant {action} for {grant.action_request_urn} ({grant.snowflake_role}/{grant.snowflake_database}/{schema_key})"
    )


def get_expired_grants(conn, state: StateConfig) -> list[GrantRecord]:
    """Return all grants whose EXPIRES_AT is in the past and have not yet been revoked."""
    sql = dml.SELECT_EXPIRED_GRANTS.format(table=state.qualified_grants_table)
    grants: list[GrantRecord] = []
    with _cursor(conn) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            urn, role, db, schema_key, email, granted_at, expires_at = row
            grants.append(
                GrantRecord(
                    action_request_urn=urn,
                    snowflake_role=role,
                    snowflake_database=db,
                    # Convert sentinel back to None so REVOKE logic works correctly
                    snowflake_schema=schema_key if schema_key != SCHEMA_ALL else None,
                    requestor_email=email,
                    granted_at_ms=int(granted_at.timestamp() * 1000),
                    expires_at_ms=int(expires_at.timestamp() * 1000) if expires_at else None,
                )
            )
    return grants


def record_revocation(conn, grant: GrantRecord, state: StateConfig) -> None:
    """Mark the grant row as revoked, keyed on the natural access combo."""
    schema_key = grant.snowflake_schema or SCHEMA_ALL
    sql = dml.UPDATE_REVOKE_GRANT.format(table=state.qualified_grants_table)
    with _cursor(conn) as cur:
        cur.execute(sql, (grant.snowflake_role, grant.snowflake_database, schema_key))
    logger.debug(
        f"[State] Marked {grant.snowflake_role}/{grant.snowflake_database}/{schema_key} as revoked"
    )


def is_sla_notified(
    conn, action_request_urn: str, notification_type: str, state: StateConfig
) -> bool:
    """Return True if this SLA notification has already been sent."""
    sql = dml.COUNT_SLA_NOTIFIED.format(table=state.qualified_sla_table)
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn, notification_type))
        row = cur.fetchone()
        return bool(row and row[0] > 0)


def record_sla_notification(
    conn, action_request_urn: str, notification_type: str, state: StateConfig
) -> None:
    """Record that an SLA notification has been sent."""
    sql = dml.INSERT_SLA_NOTIFICATION.format(table=state.qualified_sla_table)
    with _cursor(conn) as cur:
        cur.execute(
            sql, (action_request_urn, notification_type, action_request_urn, notification_type)
        )
    logger.debug(f"[State] Recorded SLA notification {notification_type} for {action_request_urn}")


# ---------------------------------------------------------------------------
# Permanent provisioning failure tracking
# ---------------------------------------------------------------------------

# Snowflake ProgrammingError errno values that indicate a permanent failure
# (i.e. retrying will never succeed without a human fix):
#   002003 — SQL compilation error: object does not exist (role, database, schema, …)
#   002001 — Object does not exist (legacy code used in older Snowflake versions)
_PERMANENT_SNOWFLAKE_ERRNOS = {2001, 2003}


def is_permanent_snowflake_error(exc: Exception) -> bool:
    """Return True if the exception is a Snowflake error that will never succeed on retry."""
    try:
        from snowflake.connector.errors import ProgrammingError

        if isinstance(exc, ProgrammingError):
            return exc.errno in _PERMANENT_SNOWFLAKE_ERRNOS
    except ImportError:
        pass
    return False


def is_provisioning_failed(conn, action_request_urn: str, state: StateConfig) -> bool:
    """Return True if this request URN has a previously-recorded permanent failure."""
    sql = f"SELECT COUNT(*) FROM {state.qualified_errors_table} WHERE ACTION_REQUEST_URN = %s"
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn,))
        row = cur.fetchone()
        return bool(row and row[0] > 0)


def record_provisioning_error(
    conn,
    action_request_urn: str,
    error_code: str | None,
    error_message: str,
    state: StateConfig,
) -> None:
    """Record a permanent provisioning failure so the request is not retried."""
    sql = (
        f"INSERT INTO {state.qualified_errors_table} "
        f"(ACTION_REQUEST_URN, SNOWFLAKE_ERROR_CODE, ERROR_MESSAGE, FAILED_AT) "
        f"SELECT %s, %s, %s, CURRENT_TIMESTAMP() "
        f"WHERE NOT EXISTS ("
        f"  SELECT 1 FROM {state.qualified_errors_table} "
        f"  WHERE ACTION_REQUEST_URN = %s"
        f")"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn, error_code, error_message, action_request_urn))
    logger.warning(
        f"[State] Recorded permanent provisioning failure for {action_request_urn}: {error_message}"
    )
