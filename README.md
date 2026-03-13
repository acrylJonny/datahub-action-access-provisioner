# datahub-action-access-provisioner

A [DataHub Actions](https://datahubproject.io/docs/actions/) handler that automates:

1. **Access provisioning** — executes Snowflake GRANT statements when a DataHub
   access-request workflow is approved, then emails the requestor.
2. **SLA tracking** — sends reminder and escalation emails to approvers when
   requests sit pending beyond configurable thresholds.
3. **Access expiry / auto-revocation** — automatically revokes Snowflake privileges
   when the declared access duration expires and notifies the original requestor.

## How It Works

```
DataHub workflow approved
        │
        ▼
MetadataChangeLogEvent (actionRequestStatus → COMPLETED / APPROVED)
        │
        ▼
AccessProvisionerAction.act()
        │
        ├─► fetch full request via GraphQL (form fields, resource, requestor)
        │
        ├─► execute Snowflake GRANTs
        │     GRANT USAGE ON DATABASE …
        │     GRANT SELECT ON ALL/FUTURE TABLES IN SCHEMA …
        │     GRANT USAGE ON WAREHOUSE …
        │
        └─► send approval email (Gmail SMTP)

Background threads (always running):
  ┌─ SLA Monitor (every N hours)
  │   └─► search DataHub for PENDING requests
  │       ├─► > warning_after_hours  → send reminder email to assignees
  │       └─► > escalation_after_hours → send escalation email (+ CC leads)
  │
  └─ Expiry Monitor (every N hours)
      └─► check in-memory grant registry
          └─► expired grant → REVOKE Snowflake privileges + notify requestor
```

## Prerequisites

- Python 3.9+
- `acryl-datahub-actions >= 1.0.0`
- `acryl-datahub >= 1.0.0`
- A Snowflake account — the configured user/role must have `GRANT OPTION` on
  the databases/schemas you intend to provision.
- A Gmail account with an [App Password](https://myaccount.google.com/apppasswords)
  (2FA must be enabled on the Google account).

## Installation

```bash
pip install -e ".[dev]"
```

Or from the published package:

```bash
pip install datahub-action-access-provisioner
```

## Configuration

See [`examples/example_action.yaml`](examples/example_action.yaml) for a
fully-annotated configuration file.

### DataHub Cloud (managed) — recommended

Register the action with DataHub Cloud using the `RemoteActionSource`. The Cloud
executor manages scheduling and restarts; there is no `action:` top-level key —
the action type and config move inside `source.config.action_spec`.

```yaml
name: access-provisioner
source:
  type: datahub_integrations.sources.remote_actions.remote_action_source.RemoteActionSource
  config:
    action_urn: "urn:li:dataHubAction:access-provisioner"
    stage: live
    action_spec:
      type: "action-access-provisioner"
      config:
        snowflake_connection:
          account_id: "xy12345"          # e.g. xy12345.us-east-1
          username: "datahub_provisioner"
          password: "${SNOWFLAKE_PASSWORD}"
          warehouse: "COMPUTE_WH"
          role: "SYSADMIN"               # Must have GRANT OPTION privilege
        smtp:
          host: "smtp.gmail.com"
          port: 587
          username: "noreply@yourdomain.com"
          password: "${GMAIL_APP_PASSWORD}"
          use_tls: true
datahub:
  server: "https://your-datahub-instance.acryl.io/gms"
  token: "${DATAHUB_TOKEN}"
```

### Local / self-hosted (development and testing)

For local testing against a DataHub Cloud instance, use the `datahub-cloud` source
and run with `datahub actions -c`:

```yaml
name: access-provisioner
source:
  type: "datahub-cloud"
  config:
    kill_after_idle_timeout: false
action:
  type: "action-access-provisioner"
  config:
    snowflake_connection:
      account_id: "xy12345"
      username: "datahub_provisioner"
      password: "${SNOWFLAKE_PASSWORD}"
      warehouse: "COMPUTE_WH"
      role: "SYSADMIN"
    smtp:
      host: "smtp.gmail.com"
      port: 587
      username: "noreply@yourdomain.com"
      password: "${GMAIL_APP_PASSWORD}"
      use_tls: true
datahub:
  server: "https://your-datahub-instance.acryl.io/gms"
  token: "${DATAHUB_TOKEN}"
```

### Form field IDs

The action reads access-request parameters from DataHub workflow form fields.
You must define a form in DataHub with the following field IDs (defaults shown —
override via `field_*` config keys if needed):

| Config key                  | Default form field ID      | Required |
|-----------------------------|----------------------------|----------|
| `field_snowflake_database`  | `snowflake_database`       | ✅       |
| `field_snowflake_schema`    | `snowflake_schema`         | ❌       |
| `field_snowflake_role`      | `snowflake_role`           | ✅       |
| `field_access_duration_days`| `access_duration_days`     | ❌       |
| `field_requestor_email`     | `requestor_email`          | ✅       |
| `field_justification`       | `justification`            | ❌       |

See [`examples/example_workflow_form_fields.md`](examples/example_workflow_form_fields.md)
for the exact SQL that will be executed for different combinations.

## Running Locally

Use the local (`datahub-cloud` source) configuration from the minimal example above.

```bash
# Set required env vars
export SNOWFLAKE_PASSWORD="..."
export GMAIL_APP_PASSWORD="..."
export DATAHUB_TOKEN="..."

# Run the action
datahub actions -c examples/example_action.yaml
```

## Development

```bash
make install-dev   # install with dev dependencies
make format        # ruff format + fix
make lint          # ruff check
make type-check    # mypy
make test          # pytest
```

### Dry-run mode

Set `provisioning.dry_run: true` in the config to log all GRANT/REVOKE
statements without executing them. Email notifications are still sent.

## Extension and re-request workflows

### How do users extend access before it expires?

Submit a **new form request** through the same DataHub workflow. Each submission
creates a new `ActionRequest` entity with a unique URN. The provisioner handles it
transparently:

```
User submits Request B (same role/database/schema, longer duration)
        │
        ▼
Approved → AccessProvisionerAction._provision(Request B)
        │
        ├─ is_already_provisioned("request-b-urn") → False  ← new URN
        │
        ├─ GRANT statements execute (idempotent — Snowflake silently accepts
        │  re-GRANTs for privileges the role already holds)
        │
        └─ record_grant(Request B)
               MERGE on (SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)
               ┌── row exists (from Request A) → UPDATE EXPIRES_AT + LATEST_URN
               └── row not exist              → INSERT
```

The grants table is keyed on `(SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)`,
not on the request URN. This means:

- There is always **exactly one active row** per access combination.
- The extension MERGE replaces the old expiry timer with the new one.
- The expiry monitor uses the updated `EXPIRES_AT` — it will **not** fire on the
  old timer and revoke still-valid access.

### What about re-requesting after access has been revoked?

Same flow: submit a new request. When the provisioner processes the approved
re-request, the MERGE finds the row with `REVOKED_AT IS NOT NULL`, clears
`REVOKED_AT`, re-GRANTs the Snowflake privileges, and updates the expiry.

### Do I need a separate "extension" workflow in DataHub?

No — the same access-request workflow works for both initial requests and
extensions. If you want approvers to see that this is a renewal rather than a
fresh request, you can add a form field (e.g. `request_type: "extension"`) and
include it in the justification email. No code changes are required.

## Architecture notes

### Grant state table

All active grants are tracked in `ACCESS_PROVISIONER_GRANTS` in Snowflake,
keyed on `(SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)`. This natural
key ensures one active row per access combination and makes extension requests
safe — a MERGE on the natural key updates the expiry in place rather than
creating a duplicate row that would trigger premature revocation.

`SNOWFLAKE_SCHEMA` stores an empty string `''` as a sentinel for "all schemas"
because Snowflake composite PKs do not permit NULL components.

### SLA deduplication

Sent SLA notifications are tracked in `ACCESS_PROVISIONER_SLA_NOTIFICATIONS`,
keyed on `(ACTION_REQUEST_URN, NOTIFICATION_TYPE)`. Each warning/escalation
fires at most once per request across all scheduled runs.

### Scheduled invocation

Because the DataHub executor kills actions after ~5 minutes of idle time, this
action should be run on a schedule (every 30 minutes is recommended). On each
startup the action runs a full catchup pass — fetching recent approved requests
and checking for expired grants and SLA breaches — before entering the live
event-listening window.

### Snowflake user requirements

The Snowflake user configured in `snowflake_connection` must have:

```sql
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE <your_role>;
-- or more specifically:
GRANT GRANT OPTION FOR USAGE ON DATABASE <db> TO ROLE <your_role>;
```

## License

Apache 2.0
