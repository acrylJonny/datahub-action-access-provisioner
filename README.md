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
- _(Optional)_ An SMTP provider for outbound email. Email notifications are
  optional: omit the `smtp` block entirely to disable all notifications
  (approvals, denials, SLA reminders, revocations) — provisioning, expiry and
  state tracking still run. Defaults target [Resend](https://resend.com)
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
["/datahub-integrations-service", "https://github.com/acrylJonny/datahub-action-access-provisioner/releases/download/v0.1.21/datahub_action_access_provisioner-0.1.21-py3-none-any.whl"]
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

### Choosing which workflows grant access

A DataHub deployment normally runs many workflows — deprecation, ownership change,
classification, revocation. All of them can be raised against the same entities and
all of them reach `COMPLETED` / `ACCEPTED` in exactly the same way, so the
provisioner needs to be told which ones actually mean "grant access". Otherwise
approving a *revocation* request results in a grant.

Two independent checks are applied, and both must pass:

1. **Content** (`require_access_fields`, on by default). The request's form must
   carry at least one field that only an access request would set — a duration, a
   target group, or a Snowflake role/database. This excludes metadata and
   revocation workflows without any configuration. Set it to `false` if your access
   workflow asks for none of those (e.g. permanent access with no duration prompt).
2. **Name / URN pattern** (`workflow`), using the same allow/deny semantics as an
   ingestion connector's filters. It is matched against both the workflow's display
   name and its URN: a request is admitted only when neither identifier is denied
   and at least one is allowed. Defaults to allow-all.

```yaml
workflow_filter:
  workflow:
    allow:
      - "Dataset Access Request"
      - "Data Product Access Request"
    deny:
      - ".*Revocation.*"
  require_access_fields: true
```

Naming your access workflows explicitly is recommended for any deployment that runs
more than a couple of workflows — the content check is a safety net, not a
substitute for saying what you mean.

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

### Exactly-once side effects (processing ledger)

Grants themselves are idempotent — they are keyed on the natural access combo and
reconciled with `MERGE`, so re-applying one is a no-op. One-shot **side effects**
(approval / denial / revocation / membership emails) are not naturally idempotent,
so they are gated by a processing ledger table `ACCESS_PROVISIONER_LEDGER`, keyed
on `(ACTION_REQUEST_URN, STAGE)`. Each stage is _claimed_ (an insert-if-absent)
**before** its notification is sent, so a duplicate live event, a replay after a
restart, or a reconciliation pass overlapping a live event never sends a second
email. `claim_stage()` returns `True` only to the caller that won the claim.

The ledger is also what makes **provisioning** idempotent per request. The grants
table cannot answer "has this request been provisioned?" on its own: it holds one
row per access combination, and its `LATEST_ACTION_REQUEST_URN` column is
overwritten whenever a newer request targets the same object. Every superseded
request would then look unprovisioned and be re-granted on every reconciliation
pass, forever. A `provisioned` stage is therefore stamped on the request itself
once the grant is recorded, and that stage — not the grants table — is the
authority. The grants-table check is retained only so rows written before the
ledger existed are not re-provisioned once.

### Delivery guarantees, reconciliation, and delay

Live `actionRequestStatus` events are best-effort. An approval that lands while the
process is down, mid-restart, or during a Kafka consumer rebalance can be missed by
the live handler. A one-shot startup catchup only re-scans once per process, so on a
long-lived daemon a missed event would otherwise wait until the next restart — which
is how approvals can end up delayed by **days**.

To bound that, the action runs a **background reconciliation loop** (on by default)
that re-runs the full catchup/reconcile pass on a fixed interval:

```yaml
reconcile:
  enabled: true # strongly recommended for long-lived daemon deployments
  interval_seconds: 300 # worst-case delay for a missed live event (min 30)
```

Every pass is idempotent (state tables + the processing ledger), so re-scanning
never re-grants or re-notifies. The reconciler and the live-event handler share a
single warehouse connection and are serialised with a lock, so their cursors never
race. Each reconcile pass isolates its phases (approvals / expiry / SLA): a transient
failure in one phase is logged and retried on the next pass rather than aborting the
others.

### Deployment model: scheduled vs. daemon

- **Long-lived daemon (recommended):** leave `reconcile.enabled: true`. Missed live
  events are picked up within `interval_seconds` without waiting for a restart.
- **Scheduled invocation:** if you instead run the action on a schedule (e.g. every
  5–10 minutes, because the DataHub executor kills idle actions after ~30s), the
  startup catchup on each run provides the same guarantee; the background loop is
  redundant but harmless. On each startup the action runs a full catchup pass —
  fetching recent approved requests and checking for expired grants and SLA breaches
  — before entering the live event-listening window.

### Snowflake connection & auth

`snowflake_connection` supports the same authentication mechanisms as the DataHub
Snowflake ingestion connector, selected via `authentication_type` (pick one):

- **`DEFAULT_AUTHENTICATOR`** — `username` + `password`.
- **`KEY_PAIR_AUTHENTICATOR`** — `username` + `private_key` (inline PEM) or
  `private_key_path`, plus `private_key_password` if the key is encrypted.
- **`OAUTH_AUTHENTICATOR`** — a token fetched from an IdP via `oauth_config`
  (`provider: microsoft` or `okta`; secret- or, for Microsoft, certificate-based).
- **`OAUTH_AUTHENTICATOR_TOKEN`** — a pre-minted OAuth `token`.
- **`EXTERNAL_BROWSER_AUTHENTICATOR`** — interactive SSO; local/dev only, since it
  cannot complete on the headless remote executor.

`snowflake_domain` (use `snowflakecomputing.cn` for China regions) and `connect_args`
(extra `snowflake.connector.connect` kwargs) are also supported. Microsoft OAuth
needs the `msal` package — install the `snowflake-oauth` extra.

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
for a fully-annotated config, and [`docs/runbook.md`](docs/runbook.md) for deploying,
verifying and operating it (including known issues).

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

**Group pickers return a URN.** A DataHub group form field yields a corpGroup URN
(`urn:li:corpGroup:analytics-team`), which Unity Catalog would not recognise as a
principal. The action resolves it before use: an explicit
`identity.group_overrides` entry wins, then the group's `displayName` from its
DataHub profile, then the id from the URN. Values that are not corpGroup URNs are
passed through untouched, so forms that already collect the Databricks group name
are unaffected. Set `identity.resolve_group_name_from_datahub: false` to skip the
profile lookup.

### Requesting on behalf of someone else

By default the grant goes to whoever raised the request. Set `field_requested_for`
to the ID of a form field naming who the access is actually for, and the action
grants to them instead. The field accepts a corpuser URN (resolved through the same
identity rules as a requestor) or a bare email, which is taken at face value —
service accounts often have no DataHub corpuser to resolve against.

This covers two cases that are otherwise impossible: a manager or platform owner
requesting for a colleague, and access for a service account, which cannot raise a
request itself.

Precedence is group, then beneficiary, then requestor. If the named beneficiary
cannot be resolved, the action logs a warning and falls back to the requestor rather
than failing the request.

Notifications still go to the requestor, since they own the request. In membership
mode the beneficiary is the member added to the group, and the notification tells
the requestor who was added.

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
  platform_instance: my_workspace # must match how the datasets were ingested
  request_url: https://your-datahub/... # optional, surfaced on the role
```

`env` and `platform_instance` must match the values your Unity Catalog ingestion
used. The mirror rebuilds the dataset URN from `catalog.schema.table`, and the
`platform_instance` is not recoverable from those (it is deliberately stripped when
resolving the grant target — see below). Get it wrong and the mirror silently
attaches the `access` aspect to a dataset URN no ingestion ever produced, leaving
your real dataset unlinked.

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

A SQL warehouse is always required (`http_path`, or `warehouse_id` to derive it):
the Delta state/log tables live there, and (with `grant_method: sql`) GRANT/REVOKE
statements run through it too. All authentication flows through the Databricks SDK
credential chain — the same resolved credential drives both the SQL warehouse and
the Unity Catalog grants API — so the provisioner supports the same mechanisms as
the DataHub Unity Catalog ingestion connector (pick one):

- **PAT** — set `token`.
- **OAuth M2M (Databricks service principal)** — set `client_id` + `client_secret`.
- **Azure AD service principal** (Azure Databricks) — set `azure_auth` with
  `client_id`, `tenant_id`, `client_secret`.
- **Unified auth** — set none of the above; the SDK resolves credentials from its
  default chain (environment variables / config profile).

Requestor identity mapping: Unity Catalog grants target a principal (an email for
an individual grant). The requestor's corpuser id is used directly when it is
already an email; otherwise the email is read from the requestor's DataHub profile.
Use `identity.principal_overrides` to map users whose DataHub id is neither (common
with SSO), and `identity.resolve_email_from_datahub: false` to disable the profile
lookup. Groups resolve the same way via `identity.group_overrides` — see
[Group-based access](#group-based-access).

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
- `access_provisioner_group_memberships` — active group memberships (membership
  access mode), keyed on `(user_email, group_name)`.
- `access_provisioner_ledger` — processing ledger keyed on
  `(action_request_urn, stage)` that guarantees exactly-once side effects: a
  notification stage is claimed before its email is sent, so replayed or duplicate
  events never send a second email.

### Databricks principal requirements

The configured user/service principal must be able to grant Unity Catalog
privileges on the target securables and create tables in the state schema, e.g.
ownership of (or `MANAGE` on) the relevant catalog/schema, plus `USE CATALOG` /
`USE SCHEMA` / `CREATE TABLE` on the state schema.

## License

Apache 2.0
