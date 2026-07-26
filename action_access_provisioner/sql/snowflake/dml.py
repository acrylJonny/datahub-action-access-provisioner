"""Snowflake state-table DML.

GRANT/REVOKE access-control statements live in ``dcl.py``. State templates use
``{table}`` for the fully-qualified state table and positional ``%s`` bind params
for values.
"""

DESCRIBE_USER = 'DESCRIBE USER "{username}"'

# ---------------------------------------------------------------------------
# Grants state table
# ---------------------------------------------------------------------------

COUNT_ACTIVE_GRANT = (
    "SELECT COUNT(*) FROM {table} WHERE LATEST_ACTION_REQUEST_URN = %s AND REVOKED_AT IS NULL"
)

# {expires_expr} is either "%s::TIMESTAMP_NTZ" or "NULL" depending on whether an
# expiry was supplied — we inline it so an absent expiry never binds an untyped NULL.
MERGE_GRANT = """
MERGE INTO {table} AS target
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

SELECT_EXPIRED_GRANTS = (
    "SELECT LATEST_ACTION_REQUEST_URN, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, "
    "SNOWFLAKE_SCHEMA, REQUESTOR_EMAIL, GRANTED_AT, EXPIRES_AT "
    "FROM {table} "
    "WHERE EXPIRES_AT <= CURRENT_TIMESTAMP() AND REVOKED_AT IS NULL"
)

UPDATE_REVOKE_GRANT = (
    "UPDATE {table} SET REVOKED_AT = CURRENT_TIMESTAMP() "
    "WHERE SNOWFLAKE_ROLE = %s AND SNOWFLAKE_DATABASE = %s AND SNOWFLAKE_SCHEMA = %s"
)

# ---------------------------------------------------------------------------
# SLA-notification state table
# ---------------------------------------------------------------------------

COUNT_SLA_NOTIFIED = (
    "SELECT COUNT(*) FROM {table} WHERE ACTION_REQUEST_URN = %s AND NOTIFICATION_TYPE = %s"
)

INSERT_SLA_NOTIFICATION = (
    "INSERT INTO {table} (ACTION_REQUEST_URN, NOTIFICATION_TYPE, SENT_AT) "
    "SELECT %s, %s, CURRENT_TIMESTAMP() "
    "WHERE NOT EXISTS ("
    "  SELECT 1 FROM {table} WHERE ACTION_REQUEST_URN = %s AND NOTIFICATION_TYPE = %s"
    ")"
)

# ---------------------------------------------------------------------------
# Processing ledger (exactly-once stage claims)
# ---------------------------------------------------------------------------

COUNT_LEDGER_STAGE = "SELECT COUNT(*) FROM {table} WHERE ACTION_REQUEST_URN = %s AND STAGE = %s"

# Insert-if-absent. cur.rowcount is 1 when this call won the claim, 0 when the
# stage was already claimed — that return value is the exactly-once gate.
CLAIM_LEDGER_STAGE = (
    "INSERT INTO {table} (ACTION_REQUEST_URN, STAGE, CLAIMED_AT) "
    "SELECT %s, %s, CURRENT_TIMESTAMP() "
    "WHERE NOT EXISTS ("
    "  SELECT 1 FROM {table} WHERE ACTION_REQUEST_URN = %s AND STAGE = %s"
    ")"
)
