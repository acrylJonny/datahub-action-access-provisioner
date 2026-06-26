import logging
import re
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from action_access_provisioner.config import (
    DatabricksProvisioningConfig,
    DatabricksStateConfig,
)
from action_access_provisioner.constants import (
    DDL_DBX_ERRORS_TABLE,
    DDL_DBX_GRANTS_TABLE,
    DDL_DBX_SLA_TABLE,
    SCHEMA_ALL,
    TABLE_ALL,
)
from action_access_provisioner.models import DatabricksGrantRecord

logger = logging.getLogger(__name__)

# Unity Catalog object identifiers (catalog/schema/table) — strict allow-list so
# the values, which originate from user-submitted form fields, can be safely
# inlined into GRANT statements (identifiers cannot be passed as bind params).
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
# Principals are emails / usernames / group names.
_PRINCIPAL_RE = re.compile(r"^[A-Za-z0-9_.@+\-]+$")


def _ident(name: str) -> str:
    """Validate and backtick-quote a Unity Catalog identifier."""
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid Databricks identifier: {name!r}")
    return f"`{name}`"


# Dataset URN: urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<env>)
_DATASET_URN_RE = re.compile(
    r"^urn:li:dataset:\(urn:li:dataPlatform:(?P<platform>[^,]+),(?P<name>.+),(?P<env>[^,)]+)\)$"
)


def parse_databricks_dataset_urn(urn: str | None) -> tuple[str, str, str] | None:
    """Extract ``(catalog, schema, table)`` from a Databricks dataset URN.

    The Unity Catalog dataset name is ``catalog.schema.table``. When a
    ``platform_instance`` is configured it is prepended as a leading segment
    (``<instance>.<catalog>.<schema>.<table>``) — we always take the trailing
    three segments so the instance is stripped and never affects the grant.

    Returns ``None`` for non-Databricks datasets or names that aren't at least
    three-level (e.g. catalog- or schema-only containers, which aren't datasets).

    ponytail: splits the name on ``.`` — a Unity Catalog identifier that itself
    contains a literal dot (only possible when backtick-quoted) would mis-split.
    Such names are not emitted by DataHub's UC ingestion, so this is acceptable.
    """
    m = _DATASET_URN_RE.match(urn or "")
    if not m or m.group("platform") != "databricks":
        return None
    parts = m.group("name").split(".")
    if len(parts) < 3:
        return None
    return parts[-3], parts[-2], parts[-1]


def _principal(name: str) -> str:
    """Validate and backtick-quote a Databricks principal (user/group/SP)."""
    if not name or not _PRINCIPAL_RE.match(name):
        raise ValueError(f"Invalid Databricks principal: {name!r}")
    return f"`{name}`"


@contextmanager
def _cursor(conn) -> Generator[Any, None, None]:
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def get_sql_connection(connection_config) -> Any:
    """Open a databricks-sql-connector connection from the provided config."""
    return connection_config.get_sql_connection()


# ---------------------------------------------------------------------------
# GRANT / REVOKE statement builders
# ---------------------------------------------------------------------------


def build_grant_statements(
    principal: str, catalog: str, schema: str | None, table: str | None
) -> list[str]:
    """Build the GRANT statements needed to provision read access.

    Unity Catalog requires USE CATALOG + USE SCHEMA for navigation plus SELECT on
    the data object. Privileges are granted at the most specific level requested.
    """
    p = _principal(principal)
    c = _ident(catalog)
    statements = [f"GRANT USE CATALOG ON CATALOG {c} TO {p}"]

    if schema:
        s = _ident(schema)
        statements.append(f"GRANT USE SCHEMA ON SCHEMA {c}.{s} TO {p}")
        if table:
            t = _ident(table)
            statements.append(f"GRANT SELECT ON TABLE {c}.{s}.{t} TO {p}")
        else:
            statements.append(f"GRANT SELECT ON SCHEMA {c}.{s} TO {p}")
    else:
        # Whole-catalog access — USE SCHEMA / SELECT granted at the catalog level
        # are inherited by every current and future schema and table.
        statements.append(f"GRANT USE SCHEMA ON CATALOG {c} TO {p}")
        statements.append(f"GRANT SELECT ON CATALOG {c} TO {p}")

    return statements


def build_revoke_statements(grant: DatabricksGrantRecord) -> list[str]:
    """Build the REVOKE statements that undo a provisioned grant.

    Only SELECT is revoked, at the level it was granted. USE CATALOG / USE SCHEMA
    are navigation-only privileges that grant no data access on their own, and
    revoking them could break the principal's other grants elsewhere in the
    catalog — so they are intentionally left in place.
    """
    p = _principal(grant.principal)
    c = _ident(grant.catalog)
    if grant.schema:
        s = _ident(grant.schema)
        if grant.table:
            t = _ident(grant.table)
            return [f"REVOKE SELECT ON TABLE {c}.{s}.{t} FROM {p}"]
        return [f"REVOKE SELECT ON SCHEMA {c}.{s} FROM {p}"]
    return [f"REVOKE SELECT ON CATALOG {c} FROM {p}"]


# ---------------------------------------------------------------------------
# Apply grants — SQL warehouse or Unity Catalog grants API
# ---------------------------------------------------------------------------


def _execute_sql_batch(conn, statements: list[str]) -> None:
    with _cursor(conn) as cur:
        for stmt in statements:
            logger.info(f"Executing: {stmt}")
            cur.execute(stmt)


def _apply_grants_sdk(
    workspace_client,
    principal: str,
    catalog: str,
    schema: str | None,
    table: str | None,
    *,
    revoke: bool,
) -> None:
    """Apply (or remove) grants via the Unity Catalog grants API.

    ``grants.update`` interpolates ``securable_type`` straight into the REST path
    (``/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}``), which
    expects the lowercase securable string — not the ``SecurableType`` enum,
    whose ``repr`` would corrupt the URL. Privileges are passed as bare strings,
    matching the API request body.
    """
    from databricks.sdk.service.catalog import PermissionsChange, Privilege

    def _change(privs: list) -> list:
        kwargs = {"remove": privs} if revoke else {"add": privs}
        return [PermissionsChange(principal=principal, **kwargs)]

    if revoke:
        # Mirror the SQL revoke: only remove SELECT at the granted level.
        if table:
            workspace_client.grants.update(
                "table", f"{catalog}.{schema}.{table}", changes=_change([Privilege.SELECT])
            )
        elif schema:
            workspace_client.grants.update(
                "schema", f"{catalog}.{schema}", changes=_change([Privilege.SELECT])
            )
        else:
            workspace_client.grants.update("catalog", catalog, changes=_change([Privilege.SELECT]))
        return

    workspace_client.grants.update("catalog", catalog, changes=_change([Privilege.USE_CATALOG]))
    if schema:
        schema_privs = [Privilege.USE_SCHEMA] + ([] if table else [Privilege.SELECT])
        workspace_client.grants.update(
            "schema", f"{catalog}.{schema}", changes=_change(schema_privs)
        )
        if table:
            workspace_client.grants.update(
                "table", f"{catalog}.{schema}.{table}", changes=_change([Privilege.SELECT])
            )
    else:
        workspace_client.grants.update(
            "catalog", catalog, changes=_change([Privilege.USE_SCHEMA, Privilege.SELECT])
        )


def provision_access(
    *,
    sql_conn,
    workspace_client,
    principal: str,
    catalog: str,
    schema: str | None,
    table: str | None,
    provisioning: DatabricksProvisioningConfig,
) -> list[str]:
    """Execute the grants to provision read access; return the statement list."""
    statements = build_grant_statements(principal, catalog, schema, table)

    if provisioning.dry_run:
        for stmt in statements:
            logger.info(f"[DRY RUN] {stmt}")
        return statements

    if provisioning.grant_method == "sdk":
        _apply_grants_sdk(workspace_client, principal, catalog, schema, table, revoke=False)
    else:
        _execute_sql_batch(sql_conn, statements)
    return statements


def revoke_access(
    *,
    sql_conn,
    workspace_client,
    grant: DatabricksGrantRecord,
    provisioning: DatabricksProvisioningConfig,
) -> list[str]:
    """Execute the REVOKE statements mirroring the original grant."""
    statements = build_revoke_statements(grant)

    if provisioning.dry_run:
        for stmt in statements:
            logger.info(f"[DRY RUN] {stmt}")
        return statements

    if provisioning.grant_method == "sdk":
        _apply_grants_sdk(
            workspace_client,
            grant.principal,
            grant.catalog,
            grant.schema,
            grant.table,
            revoke=True,
        )
    else:
        _execute_sql_batch(sql_conn, statements)
    return statements


# ---------------------------------------------------------------------------
# Persistent state / log tables (Delta)
# ---------------------------------------------------------------------------
# Keyed on the natural access combo (grantee, catalog, schema, table) so that
# extensions/re-requests for the same access update the single active row in
# place — see the design note in snowflake.py for the full rationale.


def ensure_state_tables(conn, state: DatabricksStateConfig) -> None:
    """Create the grants, SLA, and errors Delta tables if they don't exist."""
    with _cursor(conn) as cur:
        cur.execute(DDL_DBX_GRANTS_TABLE.format(table=state.qualified_grants_table))
        cur.execute(DDL_DBX_SLA_TABLE.format(table=state.qualified_sla_table))
        cur.execute(DDL_DBX_ERRORS_TABLE.format(table=state.qualified_errors_table))
    logger.info(
        f"[State] Delta state tables ready: {state.qualified_grants_table}, "
        f"{state.qualified_sla_table}, {state.qualified_errors_table}"
    )


def is_already_provisioned(conn, action_request_urn: str, state: DatabricksStateConfig) -> bool:
    """Return True if this request URN's grant is still active (not revoked)."""
    # ponytail: integer result columns are CAST to STRING throughout this module
    # because databricks-sql-connector 2.9.x converts arrow results via
    # pandas.to_numpy(na_value=None), which raises on *any* int column under
    # numpy 2.x (it coerces None to the int dtype before applying the null mask).
    # Casting to STRING yields an object column that survives the conversion.
    # Upgrade path: connector >= 3.x reads native types and makes this unnecessary.
    sql = (
        f"SELECT CAST(COUNT(*) AS STRING) FROM {state.qualified_grants_table} "
        f"WHERE latest_action_request_urn = %(urn)s AND revoked_at_ms IS NULL"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def record_grant(conn, grant: DatabricksGrantRecord, state: DatabricksStateConfig) -> None:
    """Upsert a grant row, keyed on (grantee, catalog, schema, table)."""
    schema_key = grant.schema or SCHEMA_ALL
    table_key = grant.table or TABLE_ALL

    # Inline NULL when there is no expiry so we never bind an untyped NULL param.
    expires_expr = "%(expires)s" if grant.expires_at_ms is not None else "NULL"
    sql = f"""
        MERGE INTO {state.qualified_grants_table} AS t
        USING (SELECT %(grantee)s AS grantee, %(catalog)s AS dbx_catalog,
                      %(schema)s AS dbx_schema, %(tbl)s AS dbx_table) AS s
            ON  t.grantee     = s.grantee
            AND t.dbx_catalog = s.dbx_catalog
            AND t.dbx_schema  = s.dbx_schema
            AND t.dbx_table   = s.dbx_table
        WHEN MATCHED THEN UPDATE SET
            latest_action_request_urn = %(urn)s,
            requestor_email           = %(email)s,
            granted_at_ms             = %(granted)s,
            expires_at_ms             = {expires_expr},
            revoked_at_ms             = NULL
        WHEN NOT MATCHED THEN INSERT
            (grantee, dbx_catalog, dbx_schema, dbx_table,
             latest_action_request_urn, requestor_email, granted_at_ms, expires_at_ms, revoked_at_ms)
            VALUES (%(grantee)s, %(catalog)s, %(schema)s, %(tbl)s, %(urn)s, %(email)s, %(granted)s, {expires_expr}, NULL)
    """
    params: dict[str, Any] = {
        "grantee": grant.principal,
        "catalog": grant.catalog,
        "schema": schema_key,
        "tbl": table_key,
        "urn": grant.action_request_urn,
        "email": grant.requestor_email,
        "granted": grant.granted_at_ms,
    }
    if grant.expires_at_ms is not None:
        params["expires"] = grant.expires_at_ms

    with _cursor(conn) as cur:
        cur.execute(sql, params)
    logger.debug(
        f"[State] Grant recorded for {grant.action_request_urn} "
        f"({grant.principal}/{grant.catalog}/{schema_key}/{table_key})"
    )


def get_expired_grants(conn, state: DatabricksStateConfig) -> list[DatabricksGrantRecord]:
    """Return all active grants whose expiry is in the past."""
    now_ms = int(time.time() * 1000)
    # CAST the bigint timestamp columns to STRING — see is_already_provisioned().
    sql = (
        f"SELECT latest_action_request_urn, grantee, dbx_catalog, dbx_schema, dbx_table, "
        f"requestor_email, CAST(granted_at_ms AS STRING), CAST(expires_at_ms AS STRING) "
        f"FROM {state.qualified_grants_table} "
        f"WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= %(now)s AND revoked_at_ms IS NULL"
    )
    grants: list[DatabricksGrantRecord] = []
    with _cursor(conn) as cur:
        cur.execute(sql, {"now": now_ms})
        for row in cur.fetchall():
            urn, grantee, catalog, schema_key, table_key, email, granted, expires = row
            grants.append(
                DatabricksGrantRecord(
                    action_request_urn=urn,
                    principal=grantee,
                    catalog=catalog,
                    schema=schema_key if schema_key != SCHEMA_ALL else None,
                    table=table_key if table_key != TABLE_ALL else None,
                    requestor_email=email,
                    granted_at_ms=int(granted),
                    expires_at_ms=int(expires) if expires is not None else None,
                )
            )
    return grants


def record_revocation(conn, grant: DatabricksGrantRecord, state: DatabricksStateConfig) -> None:
    """Mark the grant row revoked, keyed on the natural access combo."""
    schema_key = grant.schema or SCHEMA_ALL
    table_key = grant.table or TABLE_ALL
    sql = (
        f"UPDATE {state.qualified_grants_table} SET revoked_at_ms = %(now)s "
        f"WHERE grantee = %(grantee)s AND dbx_catalog = %(catalog)s "
        f"AND dbx_schema = %(schema)s AND dbx_table = %(tbl)s"
    )
    with _cursor(conn) as cur:
        cur.execute(
            sql,
            {
                "now": int(time.time() * 1000),
                "grantee": grant.principal,
                "catalog": grant.catalog,
                "schema": schema_key,
                "tbl": table_key,
            },
        )
    logger.debug(
        f"[State] Marked {grant.principal}/{grant.catalog}/{schema_key}/{table_key} as revoked"
    )


def is_sla_notified(
    conn, action_request_urn: str, notification_type: str, state: DatabricksStateConfig
) -> bool:
    """Return True if this SLA notification has already been sent."""
    # CAST COUNT to STRING — see is_already_provisioned().
    sql = (
        f"SELECT CAST(COUNT(*) AS STRING) FROM {state.qualified_sla_table} "
        f"WHERE action_request_urn = %(urn)s AND notification_type = %(ntype)s"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn, "ntype": notification_type})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def record_sla_notification(
    conn, action_request_urn: str, notification_type: str, state: DatabricksStateConfig
) -> None:
    """Record a sent SLA notification (idempotent via MERGE)."""
    sql = f"""
        MERGE INTO {state.qualified_sla_table} AS t
        USING (SELECT %(urn)s AS action_request_urn, %(ntype)s AS notification_type) AS s
            ON t.action_request_urn = s.action_request_urn
            AND t.notification_type = s.notification_type
        WHEN NOT MATCHED THEN INSERT (action_request_urn, notification_type, sent_at_ms)
            VALUES (%(urn)s, %(ntype)s, %(now)s)
    """
    with _cursor(conn) as cur:
        cur.execute(
            sql,
            {
                "urn": action_request_urn,
                "ntype": notification_type,
                "now": int(time.time() * 1000),
            },
        )
    logger.debug(f"[State] Recorded SLA notification {notification_type} for {action_request_urn}")


# ---------------------------------------------------------------------------
# Permanent provisioning failure tracking
# ---------------------------------------------------------------------------

# Substrings that indicate a permanent failure (retrying will never succeed
# without a human fix): the target catalog/schema/table or principal is missing.
_PERMANENT_PATTERNS = (
    "does not exist",
    "doesn't exist",
    "not found",
    "cannot be found",
    "no such",
)


def is_permanent_databricks_error(exc: Exception) -> bool:
    """Return True if the exception will never succeed on retry."""
    try:
        from databricks.sdk.errors import NotFound

        if isinstance(exc, NotFound):
            return True
    except ImportError:
        pass
    if isinstance(exc, ValueError):
        # Raised by _ident / _principal on malformed targets — a config/form fix.
        return True
    msg = str(exc).lower()
    return any(pattern in msg for pattern in _PERMANENT_PATTERNS)


def is_provisioning_failed(conn, action_request_urn: str, state: DatabricksStateConfig) -> bool:
    """Return True if this request URN has a recorded permanent failure."""
    # CAST COUNT to STRING — see is_already_provisioned().
    sql = (
        f"SELECT CAST(COUNT(*) AS STRING) FROM {state.qualified_errors_table} "
        f"WHERE action_request_urn = %(urn)s"
    )
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def record_provisioning_error(
    conn,
    action_request_urn: str,
    error_code: str | None,
    error_message: str,
    state: DatabricksStateConfig,
) -> None:
    """Record a permanent provisioning failure so the request is not retried."""
    sql = f"""
        MERGE INTO {state.qualified_errors_table} AS t
        USING (SELECT %(urn)s AS action_request_urn) AS s
            ON t.action_request_urn = s.action_request_urn
        WHEN NOT MATCHED THEN INSERT (action_request_urn, error_code, error_message, failed_at_ms)
            VALUES (%(urn)s, %(code)s, %(msg)s, %(now)s)
    """
    with _cursor(conn) as cur:
        cur.execute(
            sql,
            {
                "urn": action_request_urn,
                "code": error_code,
                "msg": error_message,
                "now": int(time.time() * 1000),
            },
        )
    logger.warning(
        f"[State] Recorded permanent provisioning failure for {action_request_urn}: {error_message}"
    )
