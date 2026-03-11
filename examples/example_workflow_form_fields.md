# Example Workflow Form Fields

When setting up your DataHub Access workflow, configure a form with the following
field IDs. These must match the `field_*` configuration keys in `example_action.yaml`.

| Field ID                  | Type    | Required | Description                                                        |
|---------------------------|---------|----------|--------------------------------------------------------------------|
| `snowflake_database`      | Text    | ✅       | Target Snowflake database (e.g. `PROD`)                            |
| `snowflake_schema`        | Text    | ❌       | Target schema — leave blank to grant access to all schemas         |
| `snowflake_role`          | Text    | ✅       | Snowflake role to be granted (e.g. `ANALYST_READ_ROLE`)            |
| `access_duration_days`    | Number  | ❌       | How many days access should last — omit for indefinite access      |
| `requestor_email`         | Text    | ✅       | Email address where approval/denial notifications are sent         |
| `justification`           | Text    | ✅       | Business justification for the access request                      |

## Example SQL that will be executed on approval

For a request with `database=PROD`, `schema=SALES`, `role=ANALYST_ROLE`, `warehouse=COMPUTE_WH`:

```sql
GRANT USAGE ON DATABASE PROD TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA PROD.SALES TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA PROD.SALES TO ROLE ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA PROD.SALES TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_ROLE;
```

If no schema is specified, access is granted at the database level:

```sql
GRANT USAGE ON DATABASE PROD TO ROLE ANALYST_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE PROD TO ROLE ANALYST_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE PROD TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_ROLE;
```
