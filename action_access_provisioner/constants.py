"""Module-level constants: GraphQL query strings and Snowflake SQL templates.

Keep all query strings and magic values here so that the logic modules
(graphql.py, snowflake.py) remain free of inline SQL/GraphQL literals.
"""

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
