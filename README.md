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
- `acryl-datahub-actions >= 0.0.9`
- `acryl-datahub >= 0.8.34`
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

### Minimal example

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
      role: "SYSADMIN"
    smtp:
      username: "noreply@yourdomain.com"
      password: "${GMAIL_APP_PASSWORD}"
datahub:
  server: "https://your-datahub.acryl.io"
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

## Architecture notes

### Grant registry

Active grants are stored in-memory in the action instance. This means revocations
survive only for the lifetime of the running action process. For production use
with high availability requirements, replace `self._active_grants` with a
persistent store (e.g. a Snowflake table or Redis).

### SLA email deduplication

The `_sla_notified` dict tracks which thresholds have been notified per request
so that reminders are not re-sent on every poll cycle. This state is also
in-memory and resets on restart — acceptable for most deployments, since DataHub
will still emit the MCL event on the next status change.

### Snowflake user requirements

The Snowflake user configured in `snowflake_connection` must have:

```sql
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE <your_role>;
-- or more specifically:
GRANT GRANT OPTION FOR USAGE ON DATABASE <db> TO ROLE <your_role>;
```

## License

Apache 2.0
