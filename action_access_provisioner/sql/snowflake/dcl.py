"""Snowflake access-control (GRANT/REVOKE) statement templates.

Identifiers (role/database/schema/warehouse) are interpolated because Snowflake
cannot bind identifiers as parameters.
"""

GRANT_ROLE_TO_ROLE = "GRANT ROLE {target_role} TO ROLE {grantee_role}"
GRANT_DATABASE_USAGE = "GRANT USAGE ON DATABASE {database} TO ROLE {role}"
GRANT_SCHEMA_USAGE = "GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role}"
GRANT_SCHEMA_SELECT_ALL = "GRANT SELECT ON ALL TABLES IN SCHEMA {database}.{schema} TO ROLE {role}"
GRANT_SCHEMA_SELECT_FUTURE = (
    "GRANT SELECT ON FUTURE TABLES IN SCHEMA {database}.{schema} TO ROLE {role}"
)
GRANT_ALL_SCHEMAS_USAGE = "GRANT USAGE ON ALL SCHEMAS IN DATABASE {database} TO ROLE {role}"
GRANT_FUTURE_SCHEMAS_USAGE = "GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {database} TO ROLE {role}"
GRANT_WAREHOUSE_USAGE = "GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role}"

REVOKE_DATABASE_USAGE = "REVOKE USAGE ON DATABASE {database} FROM ROLE {role}"
REVOKE_SCHEMA_USAGE = "REVOKE USAGE ON SCHEMA {database}.{schema} FROM ROLE {role}"
REVOKE_SCHEMA_SELECT_ALL = (
    "REVOKE SELECT ON ALL TABLES IN SCHEMA {database}.{schema} FROM ROLE {role}"
)
REVOKE_SCHEMA_SELECT_FUTURE = (
    "REVOKE SELECT ON FUTURE TABLES IN SCHEMA {database}.{schema} FROM ROLE {role}"
)

# Human-readable summaries returned (for emails/logging) where one line stands in
# for the pair of ALL + FUTURE statements actually executed.
GRANT_SCHEMA_SELECT_SUMMARY = (
    "GRANT SELECT ON ALL/FUTURE TABLES IN SCHEMA {database}.{schema} TO ROLE {role}"
)
REVOKE_SCHEMA_SELECT_SUMMARY = (
    "REVOKE SELECT ON ALL/FUTURE TABLES IN SCHEMA {database}.{schema} FROM ROLE {role}"
)
