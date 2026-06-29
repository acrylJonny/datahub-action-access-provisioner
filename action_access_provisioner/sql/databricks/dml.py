"""Databricks Unity Catalog state-table DML.

GRANT/REVOKE access-control statements live in ``dcl.py``. State templates use
``{table}`` for the qualified state table and ``%(name)s`` pyformat bind params
for values.

ponytail: integer result columns are CAST to STRING throughout because
databricks-sql-connector 2.9.x converts arrow results via
``pandas.to_numpy(na_value=None)``, which raises on any int column under numpy 2.x.
Casting to STRING yields an object column that survives the conversion. Upgrade
path: connector >= 3.x reads native types and makes the casts unnecessary.
"""

# ---------------------------------------------------------------------------
# Grants state table
# ---------------------------------------------------------------------------

COUNT_ACTIVE_GRANT = (
    "SELECT CAST(COUNT(*) AS STRING) FROM {table} "
    "WHERE latest_action_request_urn = %(urn)s AND revoked_at_ms IS NULL"
)

# {expires_expr} is either "%(expires)s" or "NULL" so an absent expiry never binds
# an untyped NULL param.
MERGE_GRANT = """
MERGE INTO {table} AS t
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

SELECT_EXPIRED_GRANTS = (
    "SELECT latest_action_request_urn, grantee, dbx_catalog, dbx_schema, dbx_table, "
    "requestor_email, CAST(granted_at_ms AS STRING), CAST(expires_at_ms AS STRING) "
    "FROM {table} "
    "WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= %(now)s AND revoked_at_ms IS NULL"
)

UPDATE_REVOKE_GRANT = (
    "UPDATE {table} SET revoked_at_ms = %(now)s "
    "WHERE grantee = %(grantee)s AND dbx_catalog = %(catalog)s "
    "AND dbx_schema = %(schema)s AND dbx_table = %(tbl)s"
)

# ---------------------------------------------------------------------------
# SLA-notification state table
# ---------------------------------------------------------------------------

COUNT_SLA_NOTIFIED = (
    "SELECT CAST(COUNT(*) AS STRING) FROM {table} "
    "WHERE action_request_urn = %(urn)s AND notification_type = %(ntype)s"
)

MERGE_SLA_NOTIFICATION = """
MERGE INTO {table} AS t
USING (SELECT %(urn)s AS action_request_urn, %(ntype)s AS notification_type) AS s
    ON t.action_request_urn = s.action_request_urn
    AND t.notification_type = s.notification_type
WHEN NOT MATCHED THEN INSERT (action_request_urn, notification_type, sent_at_ms)
    VALUES (%(urn)s, %(ntype)s, %(now)s)
"""

# ---------------------------------------------------------------------------
# Group-membership state table
# ---------------------------------------------------------------------------

COUNT_ACTIVE_MEMBERSHIP = (
    "SELECT CAST(COUNT(*) AS STRING) FROM {table} "
    "WHERE latest_action_request_urn = %(urn)s AND removed_at_ms IS NULL"
)

MERGE_MEMBERSHIP = """
MERGE INTO {table} AS t
USING (SELECT %(user)s AS user_email, %(grp)s AS group_name) AS s
    ON t.user_email = s.user_email AND t.group_name = s.group_name
WHEN MATCHED THEN UPDATE SET
    latest_action_request_urn = %(urn)s,
    added_at_ms               = %(added)s,
    expires_at_ms             = {expires_expr},
    removed_at_ms             = NULL
WHEN NOT MATCHED THEN INSERT
    (user_email, group_name, latest_action_request_urn, added_at_ms,
     expires_at_ms, removed_at_ms)
    VALUES (%(user)s, %(grp)s, %(urn)s, %(added)s, {expires_expr}, NULL)
"""

SELECT_EXPIRED_MEMBERSHIPS = (
    "SELECT latest_action_request_urn, user_email, group_name, "
    "CAST(added_at_ms AS STRING), CAST(expires_at_ms AS STRING) "
    "FROM {table} "
    "WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= %(now)s AND removed_at_ms IS NULL"
)

UPDATE_REMOVE_MEMBERSHIP = (
    "UPDATE {table} SET removed_at_ms = %(now)s "
    "WHERE user_email = %(user)s AND group_name = %(grp)s"
)

# ---------------------------------------------------------------------------
# Permanent-failure (errors) state table
# ---------------------------------------------------------------------------

COUNT_PROVISIONING_ERROR = (
    "SELECT CAST(COUNT(*) AS STRING) FROM {table} WHERE action_request_urn = %(urn)s"
)

MERGE_PROVISIONING_ERROR = """
MERGE INTO {table} AS t
USING (SELECT %(urn)s AS action_request_urn) AS s
    ON t.action_request_urn = s.action_request_urn
WHEN NOT MATCHED THEN INSERT (action_request_urn, error_code, error_message, failed_at_ms)
    VALUES (%(urn)s, %(code)s, %(msg)s, %(now)s)
"""
