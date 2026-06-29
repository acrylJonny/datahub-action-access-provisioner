"""Snowflake state-table DDL.

``{table}`` is formatted at runtime with the fully-qualified table name from
``StateConfig`` (e.g. ``JONNY_DEMO.PUBLIC.ACCESS_PROVISIONER_GRANTS``).
"""

# Sentinel stored in SNOWFLAKE_SCHEMA when no schema is specified. Snowflake
# composite PKs do not allow NULL components, so we use an empty string to mean
# "all schemas" — mirrored by the ``DEFAULT ''`` on the SNOWFLAKE_SCHEMA column.
SCHEMA_ALL = ""

GRANTS_TABLE = """
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

SLA_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    ACTION_REQUEST_URN    VARCHAR      NOT NULL,
    NOTIFICATION_TYPE     VARCHAR      NOT NULL,
    SENT_AT               TIMESTAMP_NTZ NOT NULL,
    PRIMARY KEY (ACTION_REQUEST_URN, NOTIFICATION_TYPE)
)
"""

ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    ACTION_REQUEST_URN    VARCHAR       NOT NULL PRIMARY KEY,
    SNOWFLAKE_ERROR_CODE  VARCHAR,
    ERROR_MESSAGE         VARCHAR,
    FAILED_AT             TIMESTAMP_NTZ NOT NULL
)
"""
