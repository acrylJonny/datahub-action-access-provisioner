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
from action_access_provisioner.models import (
    DatabricksGrantRecord,
    DatabricksGroupMembershipRecord,
)
from action_access_provisioner.sql.databricks import dcl, ddl, dml
from action_access_provisioner.sql.databricks.ddl import SCHEMA_ALL, TABLE_ALL

logger = logging.getLogger(__name__)

# Unity Catalog object identifiers (catalog/schema/table) — strict allow-list so
# the values, which originate from user-submitted form fields, can be safely
# inlined into GRANT statements (identifiers cannot be passed as bind params).
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
# Control characters are never valid in a principal and would let a value break out
# of the backtick quoting, so they are rejected outright.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f]")


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
    """Backtick-quote a Databricks principal (user email / group name / service principal).

    Group display names legitimately contain spaces and other punctuation, so rather
    than restrict the character set we escape any embedded backticks (Spark SQL doubles
    them) and reject only control characters. The value always comes from a trusted
    DataHub identity or a configured group name, not free-form request text.
    """
    if not name or _CONTROL_CHARS_RE.search(name):
        raise ValueError(f"Invalid Databricks principal: {name!r}")
    return "`" + name.replace("`", "``") + "`"


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
    statements = [dcl.GRANT_USE_CATALOG.format(catalog=c, principal=p)]

    if schema:
        s = _ident(schema)
        statements.append(dcl.GRANT_USE_SCHEMA_ON_SCHEMA.format(catalog=c, schema=s, principal=p))
        if table:
            t = _ident(table)
            statements.append(
                dcl.GRANT_SELECT_ON_TABLE.format(catalog=c, schema=s, table=t, principal=p)
            )
        else:
            statements.append(dcl.GRANT_SELECT_ON_SCHEMA.format(catalog=c, schema=s, principal=p))
    else:
        # Whole-catalog access — USE SCHEMA / SELECT granted at the catalog level
        # are inherited by every current and future schema and table.
        statements.append(dcl.GRANT_USE_SCHEMA_ON_CATALOG.format(catalog=c, principal=p))
        statements.append(dcl.GRANT_SELECT_ON_CATALOG.format(catalog=c, principal=p))

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
    if grant.schema_name:
        s = _ident(grant.schema_name)
        if grant.table:
            t = _ident(grant.table)
            return [dcl.REVOKE_SELECT_ON_TABLE.format(catalog=c, schema=s, table=t, principal=p)]
        return [dcl.REVOKE_SELECT_ON_SCHEMA.format(catalog=c, schema=s, principal=p)]
    return [dcl.REVOKE_SELECT_ON_CATALOG.format(catalog=c, principal=p)]


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
            grant.schema_name,
            grant.table,
            revoke=True,
        )
    else:
        _execute_sql_batch(sql_conn, statements)
    return statements


# ---------------------------------------------------------------------------
# Group membership (the "add the requestor to a group" access model)
# ---------------------------------------------------------------------------


def _resolve_group_id(workspace_client, group_name: str) -> str:
    for group in workspace_client.groups.list(filter=f'displayName eq "{group_name}"'):
        if group.id:
            return str(group.id)
    raise ValueError(f"Databricks group not found: {group_name!r}")


def _resolve_user_id(workspace_client, user_email: str) -> str:
    for user in workspace_client.users.list(filter=f'userName eq "{user_email}"'):
        if user.id:
            return str(user.id)
    raise ValueError(f"Databricks user not found: {user_email!r}")


def add_group_member(workspace_client, group_name: str, user_email: str, *, dry_run: bool) -> None:
    """Add a user to a Databricks group via the SCIM groups API."""
    if dry_run:
        logger.info(f"[DRY RUN] Would add {user_email} to group {group_name}")
        return
    # ponytail: SCIM Patch shape is pinned to databricks-sdk's iam service; if the
    # bundled SDK changes the Patch/PatchOp signature this is the single call to fix.
    from databricks.sdk.service import iam

    group_id = _resolve_group_id(workspace_client, group_name)
    user_id = _resolve_user_id(workspace_client, user_email)
    workspace_client.groups.patch(
        id=group_id,
        operations=[iam.Patch(op=iam.PatchOp.ADD, value={"members": [{"value": user_id}]})],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


def remove_group_member(
    workspace_client, group_name: str, user_email: str, *, dry_run: bool
) -> None:
    """Remove a user from a Databricks group via the SCIM groups API."""
    if dry_run:
        logger.info(f"[DRY RUN] Would remove {user_email} from group {group_name}")
        return
    from databricks.sdk.service import iam

    group_id = _resolve_group_id(workspace_client, group_name)
    user_id = _resolve_user_id(workspace_client, user_email)
    workspace_client.groups.patch(
        id=group_id,
        operations=[iam.Patch(op=iam.PatchOp.REMOVE, path=f'members[value eq "{user_id}"]')],
        schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
    )


# ---------------------------------------------------------------------------
# Persistent state / log tables (Delta)
# ---------------------------------------------------------------------------
# Keyed on the natural access combo (grantee, catalog, schema, table) so that
# extensions/re-requests for the same access update the single active row in
# place — see the design note in snowflake.py for the full rationale.


def ensure_state_tables(conn, state: DatabricksStateConfig) -> None:
    """Create the grants, SLA, errors, memberships, and ledger Delta tables if absent."""
    with _cursor(conn) as cur:
        cur.execute(ddl.GRANTS_TABLE.format(table=state.qualified_grants_table))
        cur.execute(ddl.SLA_TABLE.format(table=state.qualified_sla_table))
        cur.execute(ddl.ERRORS_TABLE.format(table=state.qualified_errors_table))
        cur.execute(ddl.MEMBERSHIPS_TABLE.format(table=state.qualified_memberships_table))
        cur.execute(ddl.LEDGER_TABLE.format(table=state.qualified_ledger_table))
    logger.info(
        f"[State] Delta state tables ready: {state.qualified_grants_table}, "
        f"{state.qualified_sla_table}, {state.qualified_errors_table}, "
        f"{state.qualified_memberships_table}, {state.qualified_ledger_table}"
    )


def is_stage_processed(
    conn, action_request_urn: str, stage: str, state: DatabricksStateConfig
) -> bool:
    """Return True if this (request, stage) has already been claimed in the ledger."""
    sql = dml.COUNT_LEDGER_STAGE.format(table=state.qualified_ledger_table)
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn, "stage": stage})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def claim_stage(conn, action_request_urn: str, stage: str, state: DatabricksStateConfig) -> bool:
    """Atomically claim a processing stage for a request.

    Returns True if this call won the claim (the caller should now perform the
    stage's side effect exactly once), or False if it was already claimed. The
    claim is written *before* the side effect so a replayed event never triggers
    a second notification.
    """
    if is_stage_processed(conn, action_request_urn, stage, state):
        return False
    sql = dml.CLAIM_LEDGER_STAGE.format(table=state.qualified_ledger_table)
    with _cursor(conn) as cur:
        cur.execute(
            sql,
            {"urn": action_request_urn, "stage": stage, "now": int(time.time() * 1000)},
        )
    return True


def is_already_provisioned(conn, action_request_urn: str, state: DatabricksStateConfig) -> bool:
    """Return True if this request URN's grant is still active (not revoked)."""
    # COUNT is CAST to STRING in the query (see sql/databricks/dml.py for why) so
    # we parse it back with int() here.
    sql = dml.COUNT_ACTIVE_GRANT.format(table=state.qualified_grants_table)
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def record_grant(conn, grant: DatabricksGrantRecord, state: DatabricksStateConfig) -> None:
    """Upsert a grant row, keyed on (grantee, catalog, schema, table)."""
    schema_key = grant.schema_name or SCHEMA_ALL
    table_key = grant.table or TABLE_ALL

    # Inline NULL when there is no expiry so we never bind an untyped NULL param.
    expires_expr = "%(expires)s" if grant.expires_at_ms is not None else "NULL"
    sql = dml.MERGE_GRANT.format(table=state.qualified_grants_table, expires_expr=expires_expr)
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
    sql = dml.SELECT_EXPIRED_GRANTS.format(table=state.qualified_grants_table)
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
                    schema_name=schema_key if schema_key != SCHEMA_ALL else None,
                    table=table_key if table_key != TABLE_ALL else None,
                    requestor_email=email,
                    granted_at_ms=int(granted),
                    expires_at_ms=int(expires) if expires is not None else None,
                )
            )
    return grants


def record_revocation(conn, grant: DatabricksGrantRecord, state: DatabricksStateConfig) -> None:
    """Mark the grant row revoked, keyed on the natural access combo."""
    schema_key = grant.schema_name or SCHEMA_ALL
    table_key = grant.table or TABLE_ALL
    sql = dml.UPDATE_REVOKE_GRANT.format(table=state.qualified_grants_table)
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
    sql = dml.COUNT_SLA_NOTIFIED.format(table=state.qualified_sla_table)
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn, "ntype": notification_type})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def record_sla_notification(
    conn, action_request_urn: str, notification_type: str, state: DatabricksStateConfig
) -> None:
    """Record a sent SLA notification (idempotent via MERGE)."""
    sql = dml.MERGE_SLA_NOTIFICATION.format(table=state.qualified_sla_table)
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
# Group membership state (mirrors the grants table, keyed on user + group)
# ---------------------------------------------------------------------------


def is_membership_provisioned(conn, action_request_urn: str, state: DatabricksStateConfig) -> bool:
    """Return True if this request URN's membership is still active (not removed)."""
    sql = dml.COUNT_ACTIVE_MEMBERSHIP.format(table=state.qualified_memberships_table)
    with _cursor(conn) as cur:
        cur.execute(sql, {"urn": action_request_urn})
        row = cur.fetchone()
        return bool(row and int(row[0]) > 0)


def record_membership(
    conn, membership: DatabricksGroupMembershipRecord, state: DatabricksStateConfig
) -> None:
    """Upsert a membership row, keyed on (user_email, group_name)."""
    expires_expr = "%(expires)s" if membership.expires_at_ms is not None else "NULL"
    sql = dml.MERGE_MEMBERSHIP.format(
        table=state.qualified_memberships_table, expires_expr=expires_expr
    )
    params: dict[str, Any] = {
        "user": membership.user_email,
        "grp": membership.group_name,
        "urn": membership.action_request_urn,
        "added": membership.added_at_ms,
    }
    if membership.expires_at_ms is not None:
        params["expires"] = membership.expires_at_ms

    with _cursor(conn) as cur:
        cur.execute(sql, params)
    logger.debug(
        f"[State] Membership recorded for {membership.action_request_urn} "
        f"({membership.user_email} -> {membership.group_name})"
    )


def get_expired_memberships(
    conn, state: DatabricksStateConfig
) -> list[DatabricksGroupMembershipRecord]:
    """Return all active memberships whose expiry is in the past."""
    now_ms = int(time.time() * 1000)
    sql = dml.SELECT_EXPIRED_MEMBERSHIPS.format(table=state.qualified_memberships_table)
    memberships: list[DatabricksGroupMembershipRecord] = []
    with _cursor(conn) as cur:
        cur.execute(sql, {"now": now_ms})
        for row in cur.fetchall():
            urn, user_email, group_name, added, expires = row
            memberships.append(
                DatabricksGroupMembershipRecord(
                    action_request_urn=urn,
                    user_email=user_email,
                    group_name=group_name,
                    added_at_ms=int(added),
                    expires_at_ms=int(expires) if expires is not None else None,
                )
            )
    return memberships


def record_membership_removal(
    conn, membership: DatabricksGroupMembershipRecord, state: DatabricksStateConfig
) -> None:
    """Mark the membership row removed, keyed on (user_email, group_name)."""
    sql = dml.UPDATE_REMOVE_MEMBERSHIP.format(table=state.qualified_memberships_table)
    with _cursor(conn) as cur:
        cur.execute(
            sql,
            {
                "now": int(time.time() * 1000),
                "user": membership.user_email,
                "grp": membership.group_name,
            },
        )
    logger.debug(
        f"[State] Marked {membership.user_email} -> {membership.group_name} membership removed"
    )


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
    sql = dml.COUNT_PROVISIONING_ERROR.format(table=state.qualified_errors_table)
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
    sql = dml.MERGE_PROVISIONING_ERROR.format(table=state.qualified_errors_table)
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
