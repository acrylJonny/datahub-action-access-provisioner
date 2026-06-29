"""Databricks Unity Catalog state-table DDL (Delta).

Timestamps are stored as BIGINT epoch-millis rather than TIMESTAMP to avoid any
timezone/format ambiguity and to keep expiry comparison a plain integer compare.
``{table}`` is formatted with the backtick-qualified table name.
"""

# Sentinels stored in the SCHEMA / TABLE columns when no schema / no table is
# specified. Empty strings keep the natural key free of NULLs (which would break
# the MERGE keying), mirroring Snowflake's SCHEMA_ALL sentinel.
SCHEMA_ALL = ""
TABLE_ALL = ""

GRANTS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    grantee                   STRING NOT NULL,
    dbx_catalog               STRING NOT NULL,
    dbx_schema                STRING NOT NULL,
    dbx_table                 STRING NOT NULL,
    latest_action_request_urn STRING NOT NULL,
    requestor_email           STRING,
    granted_at_ms             BIGINT NOT NULL,
    expires_at_ms             BIGINT,
    revoked_at_ms             BIGINT
) USING DELTA
"""

SLA_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    action_request_urn STRING NOT NULL,
    notification_type  STRING NOT NULL,
    sent_at_ms         BIGINT NOT NULL
) USING DELTA
"""

ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    action_request_urn STRING NOT NULL,
    error_code         STRING,
    error_message      STRING,
    failed_at_ms       BIGINT NOT NULL
) USING DELTA
"""

# Group-membership grants (the "add the requestor to a group" access model) are
# tracked separately from object grants: the access mechanism is membership of a
# group, keyed on (user_email, group_name).
MEMBERSHIPS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    user_email                STRING NOT NULL,
    group_name                STRING NOT NULL,
    latest_action_request_urn STRING NOT NULL,
    added_at_ms               BIGINT NOT NULL,
    expires_at_ms             BIGINT,
    removed_at_ms             BIGINT
) USING DELTA
"""
