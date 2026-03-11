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


_DDL_GRANTS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    ACTION_REQUEST_URN    VARCHAR      NOT NULL,
    SNOWFLAKE_ROLE        VARCHAR      NOT NULL,
    SNOWFLAKE_DATABASE    VARCHAR      NOT NULL,
    SNOWFLAKE_SCHEMA      VARCHAR,
    REQUESTOR_EMAIL       VARCHAR,
    GRANTED_AT            TIMESTAMP_NTZ NOT NULL,
    EXPIRES_AT            TIMESTAMP_NTZ,
    REVOKED_AT            TIMESTAMP_NTZ,
    PRIMARY KEY (ACTION_REQUEST_URN)
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
    """Return True if this request has already been provisioned (and not since revoked)."""
    sql = (
        f"SELECT COUNT(*) FROM {state.qualified_grants_table} "
        f"WHERE ACTION_REQUEST_URN = %s AND REVOKED_AT IS NULL"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn,))
        row = cur.fetchone()
        return bool(row and row[0] > 0)


def record_grant(conn, grant: GrantRecord, state: StateConfig) -> None:
    """Upsert a grant record into the state table."""
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
    sql = f"""
        MERGE INTO {state.qualified_grants_table} AS target
        USING (SELECT %s AS urn) AS source ON target.ACTION_REQUEST_URN = source.urn
        WHEN MATCHED THEN UPDATE SET
            SNOWFLAKE_ROLE     = %s,
            SNOWFLAKE_DATABASE = %s,
            SNOWFLAKE_SCHEMA   = %s,
            REQUESTOR_EMAIL    = %s,
            GRANTED_AT         = %s::TIMESTAMP_NTZ,
            EXPIRES_AT         = {"%s::TIMESTAMP_NTZ" if expires_str else "NULL"},
            REVOKED_AT         = NULL
        WHEN NOT MATCHED THEN INSERT
            (ACTION_REQUEST_URN, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
             REQUESTOR_EMAIL, GRANTED_AT, EXPIRES_AT)
        VALUES (%s, %s, %s, %s, %s, %s::TIMESTAMP_NTZ, {"%s::TIMESTAMP_NTZ" if expires_str else "NULL"})
    """
    if expires_str:
        params = (
            grant.action_request_urn,
            grant.snowflake_role,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.requestor_email,
            granted_str,
            expires_str,
            grant.action_request_urn,
            grant.snowflake_role,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.requestor_email,
            granted_str,
            expires_str,
        )
    else:
        params = (
            grant.action_request_urn,
            grant.snowflake_role,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.requestor_email,
            granted_str,
            grant.action_request_urn,
            grant.snowflake_role,
            grant.snowflake_database,
            grant.snowflake_schema,
            grant.requestor_email,
            granted_str,
        )
    with _cursor(conn) as cur:
        cur.execute(sql, params)
    logger.debug(f"[State] Recorded grant for {grant.action_request_urn}")


def get_expired_grants(conn, state: StateConfig) -> list[GrantRecord]:
    """Return all grants whose EXPIRES_AT is in the past and have not yet been revoked."""
    sql = (
        f"SELECT ACTION_REQUEST_URN, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, "
        f"REQUESTOR_EMAIL, GRANTED_AT, EXPIRES_AT "
        f"FROM {state.qualified_grants_table} "
        f"WHERE EXPIRES_AT <= CURRENT_TIMESTAMP() AND REVOKED_AT IS NULL"
    )
    grants: list[GrantRecord] = []
    with _cursor(conn) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            urn, role, db, schema, email, granted_at, expires_at = row
            grants.append(
                GrantRecord(
                    action_request_urn=urn,
                    snowflake_role=role,
                    snowflake_database=db,
                    snowflake_schema=schema,
                    requestor_email=email,
                    granted_at_ms=int(granted_at.timestamp() * 1000),
                    expires_at_ms=int(expires_at.timestamp() * 1000) if expires_at else None,
                )
            )
    return grants


def record_revocation(conn, action_request_urn: str, state: StateConfig) -> None:
    """Mark a grant as revoked in the state table."""
    sql = (
        f"UPDATE {state.qualified_grants_table} "
        f"SET REVOKED_AT = CURRENT_TIMESTAMP() "
        f"WHERE ACTION_REQUEST_URN = %s"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, (action_request_urn,))
    logger.debug(f"[State] Marked {action_request_urn} as revoked")


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
