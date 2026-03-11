"""Snowflake GRANT, REVOKE, and persistent state table logic."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from .config import SnowflakeConnectionConfig, SnowflakeProvisioningConfig, StateConfig
from .models import GrantRecord

logger = logging.getLogger(__name__)


@contextmanager
def _cursor(conn) -> Generator:  # type: ignore[type-arg]
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def get_connection(connection_config: SnowflakeConnectionConfig):  # type: ignore[return]
    """Create and return a Snowflake connection from the provided config."""
    return connection_config.get_native_connection()


def grant_role_to_role(
    conn,
    target_role: str,
    grantee_role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT <target_role> TO ROLE <grantee_role>."""
    statement = f"GRANT ROLE {target_role} TO ROLE {grantee_role}"
    _execute(conn, statement, provisioning)


def grant_database_usage(
    conn,
    database: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT USAGE ON DATABASE <database> TO ROLE <role>."""
    statement = f"GRANT USAGE ON DATABASE {database} TO ROLE {role}"
    _execute(conn, statement, provisioning)


def grant_schema_usage(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT USAGE ON SCHEMA <database>.<schema> TO ROLE <role>."""
    statement = f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role}"
    _execute(conn, statement, provisioning)


def grant_schema_select(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT SELECT ON ALL TABLES IN SCHEMA <database>.<schema> TO ROLE <role>."""
    statement = f"GRANT SELECT ON ALL TABLES IN SCHEMA {database}.{schema} TO ROLE {role}"
    _execute(conn, statement, provisioning)
    future_statement = f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {database}.{schema} TO ROLE {role}"
    _execute(conn, future_statement, provisioning)


def grant_warehouse_usage(
    conn,
    warehouse: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>."""
    statement = f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role}"
    _execute(conn, statement, provisioning)


def revoke_database_usage(
    conn,
    database: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """REVOKE USAGE ON DATABASE <database> FROM ROLE <role>."""
    statement = f"REVOKE USAGE ON DATABASE {database} FROM ROLE {role}"
    _execute(conn, statement, provisioning)


def revoke_schema_usage(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """REVOKE USAGE ON SCHEMA <database>.<schema> FROM ROLE <role>."""
    statement = f"REVOKE USAGE ON SCHEMA {database}.{schema} FROM ROLE {role}"
    _execute(conn, statement, provisioning)


def revoke_schema_select(
    conn,
    database: str,
    schema: str,
    role: str,
    provisioning: SnowflakeProvisioningConfig,
) -> None:
    """REVOKE SELECT ON ALL TABLES IN SCHEMA from role."""
    statement = f"REVOKE SELECT ON ALL TABLES IN SCHEMA {database}.{schema} FROM ROLE {role}"
    _execute(conn, statement, provisioning)
    future_statement = (
        f"REVOKE SELECT ON FUTURE TABLES IN SCHEMA {database}.{schema} FROM ROLE {role}"
    )
    _execute(conn, future_statement, provisioning)


def provision_access(
    conn,
    role: str,
    database: str,
    schema: Optional[str],
    warehouse: Optional[str],
    provisioning: SnowflakeProvisioningConfig,
) -> list[str]:
    """
    Execute the full set of GRANT statements required to provision read access.

    Returns the list of SQL statements that were (or would be in dry_run mode) executed.
    """
    statements: list[str] = []

    grant_database_usage(conn, database, role, provisioning)
    statements.append(f"GRANT USAGE ON DATABASE {database} TO ROLE {role}")

    if schema:
        grant_schema_usage(conn, database, schema, role, provisioning)
        statements.append(f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role}")
        grant_schema_select(conn, database, schema, role, provisioning)
        statements.append(
            f"GRANT SELECT ON ALL/FUTURE TABLES IN SCHEMA {database}.{schema} TO ROLE {role}"
        )
    else:
        # No schema specified — grant at the database level
        all_schemas = f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {database} TO ROLE {role}"
        _execute(conn, all_schemas, provisioning)
        statements.append(all_schemas)
        future_schemas = f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {database} TO ROLE {role}"
        _execute(conn, future_schemas, provisioning)
        statements.append(future_schemas)

    if warehouse:
        grant_warehouse_usage(conn, warehouse, role, provisioning)
        statements.append(f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role}")

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
            f"REVOKE SELECT ON ALL/FUTURE TABLES IN SCHEMA "
            f"{grant.snowflake_database}.{grant.snowflake_schema} FROM ROLE {grant.snowflake_role}"
        )
        revoke_schema_usage(
            conn,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.snowflake_role,
            provisioning,
        )
        statements.append(
            f"REVOKE USAGE ON SCHEMA {grant.snowflake_database}.{grant.snowflake_schema} "
            f"FROM ROLE {grant.snowflake_role}"
        )

    revoke_database_usage(conn, grant.snowflake_database, grant.snowflake_role, provisioning)
    statements.append(
        f"REVOKE USAGE ON DATABASE {grant.snowflake_database} FROM ROLE {grant.snowflake_role}"
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
# SNOWFLAKE_SCHEMA uses an empty string '' as a sentinel for "all schemas"
# because Snowflake composite PKs do not allow NULL components.

_SCHEMA_ALL = ""  # sentinel stored when no schema is specified


_DDL_GRANTS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    SNOWFLAKE_ROLE              VARCHAR       NOT NULL,
    SNOWFLAKE_DATABASE          VARCHAR       NOT NULL,
    SNOWFLAKE_SCHEMA            VARCHAR       NOT NULL DEFAULT '',
    LATEST_ACTION_REQUEST_URN   VARCHAR       NOT NULL,
    REQUESTOR_EMAIL             VARCHAR,
    GRANTED_AT                  TIMESTAMP_NTZ NOT NULL,
    EXPIRES_AT                  TIMESTAMP_NTZ,
    REVOKED_AT                  TIMESTAMP_NTZ,
    PRIMARY KEY (SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)
)
"""

_DDL_SLA_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    ACTION_REQUEST_URN    VARCHAR      NOT NULL,
    NOTIFICATION_TYPE     VARCHAR      NOT NULL,
    SENT_AT               TIMESTAMP_NTZ NOT NULL,
    PRIMARY KEY (ACTION_REQUEST_URN, NOTIFICATION_TYPE)
)
"""


def ensure_state_tables(conn, state: StateConfig) -> None:
    """Create the grants and SLA-notification tracking tables if they don't already exist."""
    with _cursor(conn) as cur:
        cur.execute(_DDL_GRANTS_TABLE.format(table=state.qualified_grants_table))
        cur.execute(_DDL_SLA_TABLE.format(table=state.qualified_sla_table))
    logger.info(
        f"[State] State tables ready: {state.qualified_grants_table}, {state.qualified_sla_table}"
    )


def is_already_provisioned(conn, action_request_urn: str, state: StateConfig) -> bool:
    """
    Return True if this exact request URN has already been provisioned and the grant
    is still active (not revoked).

    A new request for the same access combo (extension / re-request) will have a
    different URN and will therefore return False — which is the correct behaviour
    since the MERGE in record_grant() will update the existing row in place.
    """
    sql = (
        f"SELECT COUNT(*) FROM {state.qualified_grants_table} "
        f"WHERE LATEST_ACTION_REQUEST_URN = %s AND REVOKED_AT IS NULL"
    )
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
    schema_key = grant.snowflake_schema or _SCHEMA_ALL
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
    sql = f"""
        MERGE INTO {state.qualified_grants_table} AS target
        USING (SELECT %s AS role, %s AS db, %s AS schema) AS source
            ON  target.SNOWFLAKE_ROLE      = source.role
            AND target.SNOWFLAKE_DATABASE  = source.db
            AND target.SNOWFLAKE_SCHEMA    = source.schema
        WHEN MATCHED THEN UPDATE SET
            LATEST_ACTION_REQUEST_URN = %s,
            REQUESTOR_EMAIL           = %s,
            GRANTED_AT                = %s::TIMESTAMP_NTZ,
            EXPIRES_AT                = {expires_expr},
            REVOKED_AT                = NULL
        WHEN NOT MATCHED THEN INSERT
            (SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
             LATEST_ACTION_REQUEST_URN, REQUESTOR_EMAIL, GRANTED_AT, EXPIRES_AT)
        VALUES (%s, %s, %s, %s, %s, %s::TIMESTAMP_NTZ, {expires_expr})
    """
    common = [
        grant.snowflake_role,
        grant.snowflake_database,
        schema_key,
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
    sql = (
        f"SELECT LATEST_ACTION_REQUEST_URN, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, "
        f"SNOWFLAKE_SCHEMA, REQUESTOR_EMAIL, GRANTED_AT, EXPIRES_AT "
        f"FROM {state.qualified_grants_table} "
        f"WHERE EXPIRES_AT <= CURRENT_TIMESTAMP() AND REVOKED_AT IS NULL"
    )
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
                    snowflake_schema=schema_key if schema_key != _SCHEMA_ALL else None,
                    requestor_email=email,
                    granted_at_ms=int(granted_at.timestamp() * 1000),
                    expires_at_ms=int(expires_at.timestamp() * 1000) if expires_at else None,
                )
            )
    return grants


def record_revocation(conn, grant: GrantRecord, state: StateConfig) -> None:
    """Mark the grant row as revoked, keyed on the natural access combo."""
    schema_key = grant.snowflake_schema or _SCHEMA_ALL
    sql = (
        f"UPDATE {state.qualified_grants_table} "
        f"SET REVOKED_AT = CURRENT_TIMESTAMP() "
        f"WHERE SNOWFLAKE_ROLE = %s AND SNOWFLAKE_DATABASE = %s AND SNOWFLAKE_SCHEMA = %s"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, (grant.snowflake_role, grant.snowflake_database, schema_key))
    logger.debug(
        f"[State] Marked {grant.snowflake_role}/{grant.snowflake_database}/{schema_key} as revoked"
    )


def is_sla_notified(
    conn, action_request_urn: str, notification_type: str, state: StateConfig
) -> bool:
    """Return True if this SLA notification has already been sent."""
    sql = (
        f"SELECT COUNT(*) FROM {state.qualified_sla_table} "
        f"WHERE ACTION_REQUEST_URN = %s AND NOTIFICATION_TYPE = %s"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn, notification_type))
        row = cur.fetchone()
        return bool(row and row[0] > 0)


def record_sla_notification(
    conn, action_request_urn: str, notification_type: str, state: StateConfig
) -> None:
    """Record that an SLA notification has been sent."""
    sql = (
        f"INSERT INTO {state.qualified_sla_table} "
        f"(ACTION_REQUEST_URN, NOTIFICATION_TYPE, SENT_AT) "
        f"SELECT %s, %s, CURRENT_TIMESTAMP() "
        f"WHERE NOT EXISTS ("
        f"  SELECT 1 FROM {state.qualified_sla_table} "
        f"  WHERE ACTION_REQUEST_URN = %s AND NOTIFICATION_TYPE = %s"
        f")"
    )
    with _cursor(conn) as cur:
        cur.execute(
            sql, (action_request_urn, notification_type, action_request_urn, notification_type)
        )
    logger.debug(f"[State] Recorded SLA notification {notification_type} for {action_request_urn}")
