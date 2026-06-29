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

# SQL for Snowflake and Databricks lives under
# ``action_access_provisioner/sql/<platform>/{ddl,dml,dcl}.py``.
