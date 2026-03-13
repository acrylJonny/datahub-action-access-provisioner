# datahub-action-access-provisioner — Development Guide

DataHub Action that automatically provisions Snowflake access, tracks SLAs, and revokes expired grants in response to DataHub Access Workflow approvals.

## Essential Commands

```bash
# Install (from repo root)
pip install -e ".[dev]"

# Tests
pytest                   # unit tests (default)
pytest -m integration    # requires live DataHub + Snowflake

# Lint + format
ruff check action_access_provisioner/ tests/ scripts/   # check
ruff format action_access_provisioner/ tests/ scripts/  # format
ruff check action_access_provisioner/ tests/ scripts/ --fix  # auto-fix

# Type checking
mypy action_access_provisioner/

# Run the action locally (uses datahub-cloud source type)
datahub actions -c examples/example_action.yaml

# DataHub Cloud managed deployment uses RemoteActionSource — see examples/example_action.yaml
```

## Code Quality Rules

**ALWAYS run before committing:**

```bash
ruff format action_access_provisioner/ tests/ scripts/ && \
ruff check action_access_provisioner/ tests/ scripts/ --fix
mypy action_access_provisioner/
```

All ruff and mypy errors must be clean.

## Repository Structure

```
datahub-action-access-provisioner/
├── action_access_provisioner/
│   ├── access_provisioner_action.py  # Main Action class (entry point)
│   ├── config.py                     # Pydantic config models
│   ├── email.py                      # SMTP notification helpers
│   ├── graphql.py                    # DataHub GraphQL queries
│   ├── models.py                     # Shared data models (AccessRequest, GrantRecord, etc.)
│   └── snowflake.py                  # GRANT/REVOKE + persistent state table logic
├── examples/
│   ├── example_action.yaml           # Full reference configuration
│   └── example_workflow_form_fields.md  # Required DataHub workflow form field IDs
├── scripts/
│   └── setup_workflow.py             # Pre-step: creates the DataHub Access Workflow via GraphQL
├── tests/
│   ├── test_access_provisioner_action.py
│   ├── test_email.py
│   ├── test_graphql.py
│   ├── test_snowflake.py
│   └── test_snowflake_state.py
└── pyproject.toml
```

## Architecture

### Invocation Model

The DataHub Cloud executor kills actions after ~5 minutes of idle time. This action is designed to run on a **schedule** (every 30 minutes via cron or the DataHub scheduler), not as a persistent daemon.

**On each invocation:**

1. **Catchup pass** (startup):
   - Fetch all COMPLETED/APPROVED requests from the last `lookback_days` days
   - Skip any already recorded in the Snowflake state table (idempotent)
   - Provision Snowflake access + send email for any new approvals
   - Revoke any grants whose `access_duration_days` has elapsed + notify requestor
   - Send SLA reminders for pending requests (deduped via state table)
2. **Live event listener** — handles new `actionRequestStatus` MCL events during the remaining ~5-minute window

### State Persistence

All state is stored in two Snowflake tables (created automatically on first run):

| Table | Purpose |
|---|---|
| `ACCESS_PROVISIONER_GRANTS` | Every provisioned grant — used for idempotency and expiry detection |
| `ACCESS_PROVISIONER_SLA_NOTIFICATIONS` | Sent SLA reminders — prevents duplicate emails across runs |

### Two-Step Setup

Before deploying the action you must run `scripts/setup_workflow.py` **once** to create the DataHub Access Workflow form and approval routing via GraphQL. The workflow defines the form fields that the action reads from; field IDs in the workflow **must match** the `field_*` config keys in `action.yaml`.

```bash
export DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
export DATAHUB_GMS_TOKEN=<token>
python3 scripts/setup_workflow.py          # create both workflows
python3 scripts/setup_workflow.py --dry-run  # preview without API calls
```

## Key Patterns

### Form Field ID Alignment

The action reads specific fields from the DataHub workflow form submission. Every `field_*` key in `action.yaml` must match the `id` of a field in the workflow form created by `setup_workflow.py`:

| `action.yaml` key | Default form field `id` |
|---|---|
| `field_snowflake_database` | `snowflake_database` |
| `field_snowflake_schema` | `snowflake_schema` |
| `field_snowflake_role` | `snowflake_role` |
| `field_access_duration_days` | `access_duration_days` |
| `field_requestor_email` | `requestor_email` |
| `field_justification` | `justification` |

Override with `--field-*` flags on `setup_workflow.py` if you rename fields.

### Snowflake GRANT Logic

`snowflake.py` handles three grant patterns — chosen based on what the requestor fills in:

1. **Role grant** — `GRANT ROLE <role> TO ROLE <grantee>` (when `snowflake_role` is set)
2. **Database grant** — `GRANT USAGE ON DATABASE ... + USAGE ON ALL SCHEMAS + SELECT ON ALL TABLES` (when only database is set)
3. **Schema grant** — scoped to a specific schema (when both database and schema are set)

All GRANT/REVOKE statements are logged before execution. Set `provisioning.dry_run: true` to log only without executing.

### Adding a New Grant Type

1. Add the provisioning logic to `snowflake.py` following the existing pattern.
2. Add any new form fields to `setup_workflow.py` under `build_access_request_workflow()`.
3. Add the corresponding `field_*` key to `AccessProvisionerConfig` in `config.py`.
4. Update `access_provisioner_action.py` to extract and pass the new field.

### Email Notifications

`email.py` sends four notification types via Gmail SMTP:

| Function | Trigger |
|---|---|
| `send_approval_notification` | Access successfully provisioned |
| `send_denial_notification` | Workflow rejected/denied |
| `send_sla_warning` | Request pending past `warning_after_hours` |
| `send_escalation_alert` | Request pending past `escalation_after_hours` |
| `send_revocation_notification` | Grant revoked after access duration expires |

All emails go to `field_requestor_email` from the form; escalation emails additionally CC `sla.escalation_recipients`.

## Configuration Reference

All config lives in `examples/example_action.yaml`. Key sections:

| Section | Key fields |
|---|---|
| `snowflake_connection` | `account_id`, `username`, `password`, `role` (must have GRANT OPTION) |
| `state` | `database`, `schema` — where state tables are created |
| `smtp` | `host`, `port`, `username`, `password` (Gmail App Password) |
| `sla` | `warning_after_hours` (24), `escalation_after_hours` (72), `escalation_recipients` |
| `expiry` | `enabled` (true), `revocation_notification` (true) |
| `provisioning` | `default_warehouse`, `dry_run` |
| `lookback_days` | How far back to scan for approved requests on startup (default: 90) |

## Testing

```bash
pytest                   # unit tests (mocked Snowflake + DataHub)
pytest -m integration    # integration tests (live DataHub + Snowflake required)
pytest --cov=action_access_provisioner  # with coverage
```

Unit tests mock the Snowflake connector and DataHub GraphQL calls via `pytest-mock`. The `freezegun` fixture is used to control time in expiry and SLA tests.

## Commits

Follow Conventional Commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
