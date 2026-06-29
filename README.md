# datahub-action-access-provisioner

A [DataHub Actions](https://datahubproject.io/docs/actions/) handler that automates:

1. **Access provisioning** — executes GRANT statements in **Snowflake** or
   **Databricks (Unity Catalog)** when a DataHub access-request workflow is
   approved, then emails the requestor.
2. **SLA tracking** — sends reminder and escalation emails to approvers when
   requests sit pending beyond configurable thresholds.
3. **Access expiry / auto-revocation** — automatically revokes privileges
   when the declared access duration expires and notifies the original requestor.

Two backends ship in this package, registered as separate action types:

| Action type | Backend | Grantee | State/log tables |
| ----------- | ------- | ------- | ---------------- |
| `action-access-provisioner` | Snowflake | role | Snowflake tables |
| `action-access-provisioner-databricks` | Databricks Unity Catalog | requestor's email (principal) | Delta tables |

The Databricks backend is documented in [its own section](#databricks-backend); the
rest of this README covers the Snowflake backend unless stated otherwise.

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
        └─► send approval email (SMTP)

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
- An SMTP provider for outbound email. Defaults target [Resend](https://resend.com)
  (username `resend`, password = your API key, and a verified domain sender for
  `from_address`); any SMTP server works by overriding `host`/`username`/`port`.

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
          host: "smtp.resend.com"
          port: 587
          username: "resend"
          password: "${RESEND_API_KEY}"
          from_address: "DataHub <noreply@yourdomain.com>"   # verified Resend sender
          use_tls: true
datahub:
  server: "https://your-datahub-instance.acryl.io/gms"
  token: "${DATAHUB_TOKEN}"
```

When creating the ingestion source in the DataHub Cloud UI, go to **Step 5 → Advanced**
and add the following under **Extra Pip Libraries**:

```json
["/datahub-integrations-service", "https://github.com/acrylJonny/datahub-action-access-provisioner/releases/download/v0.1.16/datahub_action_access_provisioner-0.1.16-py3-none-any.whl"]
```

Update the wheel URL to point to the [latest release](https://github.com/acrylJonny/datahub-action-access-provisioner/releases)
when a new version is published.

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
      host: "smtp.resend.com"
      port: 587
      username: "resend"
      password: "${RESEND_API_KEY}"
      from_address: "DataHub <noreply@yourdomain.com>"   # verified Resend sender
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
export RESEND_API_KEY="..."
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

Because the DataHub executor kills actions after ~30 seconds of idle time, this
action should be run on a schedule (every 5–10 minutes is recommended). On each
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

## Databricks backend

The `action-access-provisioner-databricks` action provisions **Unity Catalog**
read access instead of Snowflake roles. It shares the same scheduling model, SLA
tracking, expiry/auto-revocation, and email plumbing — only the grant target and
the state store differ.

See [`examples/example_action_databricks.yaml`](examples/example_action_databricks.yaml)
for a fully-annotated config.

### Install

```bash
pip install -e ".[databricks]"   # databricks-sql-connector + databricks-sdk
```

### How grants work

The grantee is the **requestor's email** (a Databricks principal — user, group,
or service principal), resolved from the requestor's corpuser identity. The
target is **derived from the dataset the request was raised on** — never from
form fields — so the `catalog.schema.table` always matches the entity:

```sql
GRANT USE CATALOG ON CATALOG `catalog` TO `user@example.com`;
GRANT USE SCHEMA  ON SCHEMA  `catalog`.`schema` TO `user@example.com`;
GRANT SELECT      ON TABLE   `catalog`.`schema`.`table` TO `user@example.com`;
```

Revocation only removes `SELECT` at the granted level — the navigation-only
`USE CATALOG` / `USE SCHEMA` privileges are left in place so revoking one grant
never breaks the principal's unrelated access elsewhere in the catalog.

The workflow form therefore only collects `justification` and an optional
`access_duration_days` — there are no catalog/schema/table or email fields.

### Group-based access

By default the grantee is the requestor's own identity. For more scalable access
management you can route a request through a **Databricks group** instead: add a
form field (default ID `databricks_group`, configurable via
`field_databricks_group`) and set `field_databricks_group` in the config. When the
field is empty the action always falls back to the individual requestor.

`provisioning.group_access_mode` selects what supplying a group means:

- `grant` (default) — the object is granted **`TO <group>`**; expiry/revocation
  operate on the group's grant.
- `membership` — the requestor is **added as a member of the group** (the group
  already holds the relevant grants) via the SCIM groups API, and removed again on
  expiry. No object grant is issued. This is the IdP-reconcilable pattern: group
  membership can be managed/audited centrally (e.g. against Entra ID), giving a
  second layer that survives a user leaving. Membership changes always go through
  the SDK regardless of `grant_method`, and are tracked in a dedicated
  `access_provisioner_group_memberships` Delta table.

Notifications (approval, failure, expiry) always go to the human who raised the
request, never to the group.

### Ticketing (Jira / ServiceNow)

Some teams fulfil access through their ITSM tool. Add an optional `ticketing`
block to open a ticket on approval:

- `mode: augment` — grant access in Databricks **and** file a ticket (hand-off /
  audit trail). Idempotent for free: a ticket is only filed when a fresh grant is
  applied.
- `mode: replace` — **skip the grant** and only file a ticket for a human to
  fulfil. The recorded state row carries no expiry (nothing to auto-revoke), and
  if the ticket call fails nothing is recorded so the next catchup retries.

Both Jira (basic auth with an API token, requires `jira_project_key`) and
ServiceNow (basic auth, configurable `servicenow_table`) are supported. See the
commented `ticketing:` block in the example recipe.

### DataHub access mirror (auditing)

Enable `datahub_sync` to mirror the access the action grants back into DataHub
using the native [`role`](https://docs.datahub.com/docs/generated/metamodel/entities/role)
entity, giving a queryable **"who has access"** audit view:

```yaml
datahub_sync:
  enabled: true # off by default
  role_urn_prefix: databricks # role URNs: urn:li:role:databricks.<group>
  platform: databricks
  env: PROD # must match how the datasets were ingested
  request_url: https://your-datahub/... # optional, surfaced on the role
```

What gets written:

- a Databricks group `analytics` becomes a role `urn:li:role:databricks.analytics`
  (`roleProperties` + `actors`);
- **group grants** add a `RoleAssociation` to the granted dataset's `access` aspect,
  so the dataset shows which roles can reach it;
- **membership** changes (membership mode) record the individual user on the role's
  `actors.users`, and expiry removes them again.

This is a strictly **read-only mirror of Unity Catalog** — editing DataHub never
mutates Databricks. It does not require the `SHOW_ACCESS_MANAGEMENT` UI tab; the
value is in the queryable metadata. Errors in the mirror are logged and swallowed
so they can never affect provisioning.

> **Scope.** Only access the action itself issues is mirrored, and only group grants
> are role-modelled today (individual user grants and the full policy structured
> properties are a follow-up). Mirroring pre-existing, out-of-band Unity Catalog
> access (a UC-wide `SHOW GRANTS` reconciliation crawl) is a separate, larger piece
> not yet implemented.

### platform_instance is stripped

The target is parsed from the approved dataset's URN, always taking the trailing
`catalog.schema.table`. A Databricks dataset ingested with a `platform_instance`
has a URN like
`urn:li:dataset:(urn:li:dataPlatform:databricks,<instance>.catalog.schema.table,PROD)`,
and the leading `<instance>` segment is dropped — so a `platform_instance` prefix
can never change which Unity Catalog object gets granted. A request raised on a
non-Databricks dataset (which can't resolve to a UC object) is recorded as a
permanent `INVALID_TARGET` failure rather than retried.

### Connection & auth

A SQL warehouse (`http_path`) is always required: the Delta state/log tables live
there, and (with `grant_method: sql`) GRANT/REVOKE statements run through it too.
Two auth methods are supported:

- **PAT** — set `token`.
- **OAuth service principal** — set `client_id` + `client_secret`.

`grant_method` selects how grants are applied:

- `sql` (default) — runs GRANT/REVOKE through the SQL warehouse.
- `sdk` — uses the Unity Catalog grants API (`databricks-sdk`), no SQL needed
  for the grant itself (the warehouse is still used for the Delta state tables).

### Delta state/log tables

All state lives in Unity Catalog Delta tables (default `datahub.access_provisioner`):

- `access_provisioner_grants` — every active grant, keyed on the natural combo
  `(grantee, catalog, schema, table)` and reconciled with `MERGE`, exactly like
  the Snowflake design (one active row per access combo; extensions update the
  expiry in place). Empty-string sentinels stand in for "all schemas" / "all
  tables" so the key never contains NULLs. Timestamps are stored as epoch-millis
  `BIGINT` to keep expiry a plain integer compare.
- `access_provisioner_sla_notifications` — dedups SLA emails across runs.
- `access_provisioner_errors` — records permanent provisioning failures (e.g. a
  missing catalog/principal) so they are not retried on every catchup pass.

### Databricks principal requirements

The configured user/service principal must be able to grant Unity Catalog
privileges on the target securables and create tables in the state schema, e.g.
ownership of (or `MANAGE` on) the relevant catalog/schema, plus `USE CATALOG` /
`USE SCHEMA` / `CREATE TABLE` on the state schema.

## License

Apache 2.0
