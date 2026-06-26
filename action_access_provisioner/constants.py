# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

# Note: ActionRequest does NOT implement Entity — searchAcrossEntities cannot
# be used. listActionRequests (ListActionRequestsInput) is the correct API.
# All fields are top-level; there is no actionRequestInfo / actionRequestStatus
# wrapper.

FETCH_ACTION_REQUEST_QUERY = """
query fetchActionRequest($urn: String!) {
  actionRequest(urn: $urn) {
    urn
    type
    status
    result
    resultNote
    entity { urn }
    assignedUsers
    assignedGroups
    created { time actor { urn } }
    dueDate
    params {
      workflowFormRequest {
        fields {
          id
          values {
            ... on StringValue { stringValue }
            ... on NumberValue { numberValue }
          }
        }
        access { expiresAt }
      }
    }
  }
}
"""

LIST_ACTION_REQUESTS_QUERY = """
query listActionRequests($input: ListActionRequestsInput!) {
  listActionRequests(input: $input) {
    total
    actionRequests {
      urn
      type
      status
      result
      resultNote
      entity { urn }
      assignedUsers
      assignedGroups
      created { time actor { urn } }
      dueDate
      params {
        workflowFormRequest {
          fields {
            id
            values {
              ... on StringValue { stringValue }
              ... on NumberValue { numberValue }
            }
          }
          access { expiresAt }
        }
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Snowflake state-table DDL
# ---------------------------------------------------------------------------

# Sentinel stored in SNOWFLAKE_SCHEMA when no schema is specified.
# Snowflake composite PKs do not allow NULL components, so we use an empty
# string to mean "all schemas".
SCHEMA_ALL = ""

# {table} is formatted at runtime with the fully-qualified table name from
# StateConfig (e.g. JONNY_DEMO.PUBLIC.ACCESS_PROVISIONER_GRANTS).
DDL_GRANTS_TABLE = """
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

DDL_SLA_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    ACTION_REQUEST_URN    VARCHAR      NOT NULL,
    NOTIFICATION_TYPE     VARCHAR      NOT NULL,
    SENT_AT               TIMESTAMP_NTZ NOT NULL,
    PRIMARY KEY (ACTION_REQUEST_URN, NOTIFICATION_TYPE)
)
"""

DDL_ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    ACTION_REQUEST_URN    VARCHAR       NOT NULL PRIMARY KEY,
    SNOWFLAKE_ERROR_CODE  VARCHAR,
    ERROR_MESSAGE         VARCHAR,
    FAILED_AT             TIMESTAMP_NTZ NOT NULL
)
"""

# ---------------------------------------------------------------------------
# Databricks Unity Catalog state-table DDL (Delta)
# ---------------------------------------------------------------------------

# Sentinel stored in the SCHEMA / TABLE columns when no schema / no table is
# specified. We use empty strings so the natural key never contains NULLs,
# mirroring the Snowflake SCHEMA_ALL sentinel.
TABLE_ALL = ""

# Timestamps are stored as BIGINT epoch-millis rather than TIMESTAMP to avoid
# any timezone/format ambiguity and to keep expiry comparison a plain integer
# compare. {table} is formatted with the backtick-qualified table name.
DDL_DBX_GRANTS_TABLE = """
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

DDL_DBX_SLA_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    action_request_urn STRING NOT NULL,
    notification_type  STRING NOT NULL,
    sent_at_ms         BIGINT NOT NULL
) USING DELTA
"""

DDL_DBX_ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (
    action_request_urn STRING NOT NULL,
    error_code         STRING,
    error_message      STRING,
    failed_at_ms       BIGINT NOT NULL
) USING DELTA
"""
