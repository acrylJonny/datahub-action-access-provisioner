"""Snowflake GRANT and REVOKE execution logic."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional

from .config import SnowflakeConnectionConfig, SnowflakeProvisioningConfig
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
