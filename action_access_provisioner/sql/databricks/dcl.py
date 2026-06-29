"""Databricks Unity Catalog access-control (GRANT/REVOKE) statement templates.

Backtick-quoted identifiers (catalog/schema/table/principal) are interpolated
because Unity Catalog cannot bind identifiers as parameters.
"""

GRANT_USE_CATALOG = "GRANT USE CATALOG ON CATALOG {catalog} TO {principal}"
GRANT_USE_SCHEMA_ON_SCHEMA = "GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO {principal}"
GRANT_SELECT_ON_TABLE = "GRANT SELECT ON TABLE {catalog}.{schema}.{table} TO {principal}"
GRANT_SELECT_ON_SCHEMA = "GRANT SELECT ON SCHEMA {catalog}.{schema} TO {principal}"
GRANT_USE_SCHEMA_ON_CATALOG = "GRANT USE SCHEMA ON CATALOG {catalog} TO {principal}"
GRANT_SELECT_ON_CATALOG = "GRANT SELECT ON CATALOG {catalog} TO {principal}"

REVOKE_SELECT_ON_TABLE = "REVOKE SELECT ON TABLE {catalog}.{schema}.{table} FROM {principal}"
REVOKE_SELECT_ON_SCHEMA = "REVOKE SELECT ON SCHEMA {catalog}.{schema} FROM {principal}"
REVOKE_SELECT_ON_CATALOG = "REVOKE SELECT ON CATALOG {catalog} FROM {principal}"
