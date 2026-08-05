# Operations runbook — Databricks access provisioner

How to deploy, verify and operate the `action-access-provisioner-databricks` action
on DataHub Cloud. For what each config key means, see the
[Databricks backend](../README.md#databricks-backend) section of the README; this
document covers running it.

Examples use placeholders: `my_catalog.my_schema.my_table` for the Unity Catalog
object, `my_instance` for the DataHub `platform_instance`, and
`https://datahub.example.com/gms` for the GMS endpoint.

## 1. Before you deploy

You need four things in place. Getting any of them wrong produces a silent
no-op rather than an error, so confirm each one.

1. **An access-request workflow** whose form collects, at minimum, a justification
   and `access_duration_days`. Note its exact display name — the action matches on
   it (step 3).
2. **Unity Catalog datasets ingested into DataHub.** Record the `platform_instance`
   and `env` the ingestion used. The mirror needs both to rebuild dataset URNs.
3. **A Databricks SQL warehouse** plus a principal that can grant on the target
   securables and create tables in the state schema.
4. **A DataHub secret** holding the Databricks credential, referenced from the
   recipe as `${MY_DBX_TOKEN}`.

## 2. Deploy the action

Create an ingestion source of type `RemoteActionSource` with the action config
under `source.config.action_spec`, and add the wheel under **Step 5 → Advanced →
Extra Pip Libraries**:

```json
["/datahub-integrations-service", "https://github.com/acrylJonny/datahub-action-access-provisioner/releases/download/v0.1.22/datahub_action_access_provisioner-0.1.22-py3-none-any.whl"]
```

`/datahub-integrations-service` must stay in the list — `RemoteActionSource` lives
in the executor image, not on PyPI, and dropping it breaks every run.

### Schedule: leave it unscheduled

The action is a **long-running daemon**, not a batch job. It boots, runs a startup
catchup pass, then reconciles on its own timer (`reconcile.interval_seconds`,
default 300s) until the container stops.

Leave the ingestion source **unscheduled** and start it manually. A schedule of any
frequency stacks daemons, because the running container never exits on its own:
`RemoteActionSource` does not expose `kill_after_idle_timeout`, and its default is
`False`, so there is no idle path back out. Each tick therefore starts another
container alongside the one already running. DataHub marks the extra ticks
`DUPLICATE` and stops collecting their logs, but they still boot, connect to
Databricks and provision — invisible in the UI while actively writing to Unity
Catalog.

The daemon's own reconcile loop already provides the recovery a schedule would
normally give you: anything missed while it was down is picked up by the startup
catchup pass when you next start it.

### Restrict which workflows grant access

`workflow_filter` is the single most important setting. Without it, **any** approved
workflow form request raised on a Databricks dataset is treated as a grant — so
approving a revocation or a deprecation request grants access.

```yaml
workflow_filter:
  workflow:
    allow: ["Dataset Access Request", "Data Product Access Request"]
    deny: [".*Revocation.*"]
  require_access_fields: true
```

`allow`/`deny` are regex patterns matched against the workflow's display name and
its URN. `require_access_fields` is a content guard: a request with none of the
access-shaped form fields is refused even if the name matches. Leave it on.

### Granting to a group, or on someone's behalf

If the form offers a group field, point `field_databricks_group` at it. A DataHub
group picker returns a corpGroup URN, which is resolved to a Databricks group name
via `identity.group_overrides`, then the group's DataHub display name, then the id
from the URN — so confirm the resolved name matches a real Databricks group before
going live, and add an override if the two systems name the group differently.

If the form offers a "requested for" field, point `field_requested_for` at it and
the grant goes to that person rather than the requester. Notifications still go to
the requester. Both are optional; leave the config keys unset if the form has no
such fields.

### Match the mirror to your ingestion

```yaml
datahub_sync:
  enabled: true
  platform: databricks
  env: PROD
  platform_instance: my_instance
```

`platform_instance` and `env` must match the Unity Catalog ingestion exactly. The
mirror rebuilds dataset URNs from `catalog.schema.table`, and `platform_instance`
is not recoverable from those. If it is wrong or missing, the `access` aspect is
attached to a dataset URN no ingestion ever produced — a "phantom" dataset that
looks plausible in search while your real dataset shows no access at all.

## 3. Verify the deployment

Start one run and confirm these lines appear, in order:

```
[DatabricksAccessProvisioner] Initialised
[Catchup] Starting startup catchup pass…
[DatabricksAccessProvisioner] Databricks SQL connection established
[State] Delta state tables ready: …access_provisioner_grants, …_ledger
[GraphQL] Found N approved requests in last 90 days
[Catchup] Provisioned 0 new request(s) from backlog
[Catchup] Startup catchup pass complete
[Reconcile] Background reconciliation every 300s
```

On a healthy steady state, every later pass logs `Provisioned 0` — the ledger
records what has already been fulfilled, so re-running never re-grants.

Then check that exactly one run is live. Anything other than a single `RUNNING`
execution needs investigating before you trust the state (see A1).

## 4. The access lifecycle

What a successful request looks like end to end.

**Request approved** → the action picks it up, either from the live event or the
next reconcile pass, and applies three grants:

```
[Provision] principal=user@example.com target=my_catalog.my_schema.my_table for request urn:li:actionRequest:…
Executing: GRANT USE CATALOG ON CATALOG `my_catalog` TO `user@example.com`
Executing: GRANT USE SCHEMA ON SCHEMA `my_catalog`.`my_schema` TO `user@example.com`
Executing: GRANT SELECT ON TABLE `my_catalog`.`my_schema`.`my_table` TO `user@example.com`
[Provision] 3 grant(s) applied for urn:li:actionRequest:…
Email sent to ['user@example.com']: ✅ Your DataHub access request has been approved
```

A row lands in `access_provisioner_grants` with `granted_at_ms` and, when a
duration was requested, `expires_at_ms`. The dataset's `access` aspect gains a
role for the grantee.

**Expiry** → once `expires_at_ms` passes, the next reconcile pass revokes:

```
[Expiry] Revoking expired grant for urn:li:actionRequest:… (principal=user@example.com, catalog=my_catalog)
Executing: REVOKE SELECT ON TABLE `my_catalog`.`my_schema`.`my_table` FROM `user@example.com`
Email sent to ['user@example.com']: 🔒 Your Databricks access has expired and been revoked
```

`revoked_at_ms` is stamped and the role is removed from the dataset's `access`
aspect. Only `SELECT` is revoked — the navigation-only `USE CATALOG` / `USE SCHEMA`
privileges stay, so revoking one grant never breaks unrelated access.

**Rejection** → no grant is applied; only a denial email is sent.

## 5. Checking state

Everything the action knows lives in Delta tables in the configured state schema.

```sql
-- active and expired grants
SELECT grantee, dbx_catalog, dbx_schema, dbx_table,
       from_unixtime(granted_at_ms/1000)  AS granted,
       from_unixtime(expires_at_ms/1000)  AS expires,
       from_unixtime(revoked_at_ms/1000)  AS revoked
FROM my_state_catalog.my_state_schema.access_provisioner_grants;

-- what has already been fulfilled or notified, per request
SELECT stage, COUNT(*) FROM …access_provisioner_ledger GROUP BY stage;

-- requests that will never be retried
SELECT * FROM …access_provisioner_errors;
```

Cross-check against the source of truth in Unity Catalog:

```sql
SHOW GRANTS ON TABLE my_catalog.my_schema.my_table;
```

Use the Unity Catalog name here, **not** the DataHub URN. `platform_instance` is a
DataHub-side prefix and is not part of the Unity Catalog path.

To confirm nothing is writing when it shouldn't be, read the Delta commit log
directly — this is the fastest way to spot an unexpected writer:

```sql
DESCRIBE HISTORY my_state_catalog.my_state_schema.access_provisioner_grants;
```

## 6. Troubleshooting

**A request was approved but nothing was granted.**
Check `workflow_filter` first. A workflow whose name is not in `allow`, or a
request missing access-shaped fields when `require_access_fields` is on, is skipped
with a debug line rather than an error. Then check `access_provisioner_errors` for
a permanent failure such as `INVALID_TARGET` (a request raised on a non-Databricks
dataset), which is recorded once and never retried.

**Grants keep being re-applied every few minutes.**
`granted_at_ms` moving forward on every pass means more than one daemon is running,
or one is running an old build. Confirm with `DESCRIBE HISTORY` on the grants table
and count how many merges land per interval, then reconcile that against the number
of `RUNNING` executions. Databricks query history identifies the client:

```sql
SELECT executed_by, client_application, client_driver, COUNT(*), MAX(start_time)
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 3 HOURS
  AND statement_text ILIKE '%access_provisioner%'
GROUP BY ALL ORDER BY 5 DESC;
```

This matters beyond tidiness: re-applying a grant rewrites `expires_at_ms` to
now + duration, so an affected grant never expires.

**The `access` aspect is on a dataset that does not exist.**
`datahub_sync.platform_instance` does not match your ingestion. Fix the config,
then hard-delete the phantom:

```bash
datahub delete --urn "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.my_table,PROD)" --hard -f
```

A phantom has no schema fields and no properties but does carry an `access` role —
that combination distinguishes it from a real dataset ingested without a
`platform_instance`.

**Emails are not arriving.**
Provisioning and revocation are unaffected by email failure; the send is logged and
swallowed. Check the SMTP provider's sending restrictions — a sandboxed account
that only delivers to its own address will fail every send to anyone else, and each
retry re-fails because the notification stage is only claimed on success.

**Stopping the action.**
Cancelling an execution marks it `CANCELLED` in the UI and stops log collection,
but does not reliably terminate the container: an orphaned daemon can keep
provisioning for hours with no visible execution. Deleting the ingestion source
does stop it, though not immediately — allow up to about 30 minutes, and confirm
via `DESCRIBE HISTORY` that writes have actually ceased before concluding it is
gone. Back the source config up first so you can recreate it; note that recreating
mints a new source URN.

## Appendix A — Known issues

**A1. Cancelling an execution does not stop the container.**
A cancelled execution's daemon can keep running and writing to Unity Catalog
indefinitely, invisible in the UI because log collection has stopped. Deleting the
ingestion source is the reliable lever. This is executor behaviour, not action
behaviour — the action's reconcile thread is a daemon thread that honours its stop
event and exits cleanly with the process. Combined with a tight cron (section 2),
this is how a deployment ends up with several daemons on different builds writing
to the same state tables.

**A2. Live approval events are not delivered.**
The action subscribes to `actionRequestStatus` changes on `actionRequest` entities
so approvals are actioned immediately. In testing these events never arrived — with
a single instance and debug logging on, no `[Live]` lines were emitted and every
request was provisioned by the reconcile pass instead. The consequence is latency,
not incorrectness: provisioning is bounded by `reconcile.interval_seconds` (measured
at 59 seconds against a 300s interval). Lower the interval if you need tighter
response. Diagnosis points upstream of the action, at MCL delivery to the executor's
events consumer.

**A3. One grant row per `(grantee, catalog, schema, table)`.**
The grants table is keyed on the natural combination, so a second request for the
same object by the same grantee updates the existing row rather than adding one.
The expiry becomes that of the most recent grant and the earlier request's identity
is not retained on the row. This is intentional — re-requesting extends access —
but it means the grants table is a current-state view, not a history. Use the
ledger and `DESCRIBE HISTORY` for an audit trail.

**A4. The mirror covers only what this action grants.**
Pre-existing, out-of-band Unity Catalog access is not reflected in DataHub. There
is no reconciliation crawl over `SHOW GRANTS`, so the mirror is a record of what
the action did, not a complete picture of who can reach a dataset.
