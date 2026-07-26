import logging
import threading
import time
from typing import Any

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import MetadataChangeLogEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from action_access_provisioner.config import AccessProvisionerConfig
from action_access_provisioner.email import (
    send_approval_notification,
    send_denial_notification,
    send_escalation_alert,
    send_provisioning_failure_notification,
    send_revocation_notification,
    send_sla_warning,
)
from action_access_provisioner.graphql import (
    fetch_action_request,
    fetch_all_approved_requests,
    fetch_pending_action_requests,
)
from action_access_provisioner.models import (
    ACTION_REQUEST_TYPE_WORKFLOW,
    LEDGER_STAGE_APPROVAL_NOTIFIED,
    LEDGER_STAGE_DENIAL_NOTIFIED,
    LEDGER_STAGE_REVOCATION_NOTIFIED,
    AccessRequest,
    GrantRecord,
    PendingRequestSummary,
)
from action_access_provisioner.snowflake import (
    claim_stage,
    ensure_state_tables,
    get_connection,
    get_expired_grants,
    get_user_default_role,
    is_already_provisioned,
    is_permanent_snowflake_error,
    is_provisioning_failed,
    is_sla_notified,
    provision_access,
    record_grant,
    record_provisioning_error,
    record_revocation,
    record_sla_notification,
    revoke_access,
)

logger = logging.getLogger(__name__)

_ASPECT_ACTION_REQUEST_STATUS = "actionRequestStatus"

_SLA_TYPE_WARNING = "warning"
_SLA_TYPE_ESCALATION = "escalation"


class AccessProvisionerAction(Action):
    """
    DataHub Actions handler for automated Snowflake access provisioning.

    See module docstring for the full scheduling model.
    """

    def __init__(self, config: AccessProvisionerConfig, ctx: PipelineContext) -> None:
        self.config = config
        self.ctx = ctx
        self._snowflake_conn: Any = None
        # A single Snowflake connection is shared between the live-event thread and
        # the background reconciler; the connector is not safe for concurrent cursor
        # use, so all connection access is serialised through this lock. ponytail:
        # coarse lock — fine at this request volume; upgrade path is a per-thread
        # connection pool if throughput ever demands it.
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._reconcile_thread: threading.Thread | None = None

        logger.info("[AccessProvisioner] Initialised")
        if config.provisioning.dry_run:
            logger.warning(
                "[AccessProvisioner] DRY RUN mode — no Snowflake statements will execute"
            )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = AccessProvisionerConfig.model_validate(config_dict or {})
        action = cls(config, ctx)
        action._startup_catchup()
        action._start_reconciler()
        return action

    # ------------------------------------------------------------------
    # Startup catchup + background reconciliation
    # ------------------------------------------------------------------

    def _startup_catchup(self) -> None:
        """Run one reconcile pass at startup (backlog of approvals, expiry, SLA)."""
        logger.info("[Catchup] Starting startup catchup pass…")
        self._reconcile_once()
        logger.info("[Catchup] Startup catchup pass complete")

    def _start_reconciler(self) -> None:
        """Start the background loop that re-runs the reconcile pass on an interval.

        Live ``actionRequestStatus`` events are best-effort; anything missed while the
        process is down, restarting, or during a consumer rebalance would otherwise wait
        until the next process restart (which, for a long-lived daemon, can be days). This
        loop bounds that delay to ``reconcile.interval_seconds``. Every pass is idempotent,
        so it never re-grants or re-notifies.
        """
        if not self.config.reconcile.enabled:
            logger.info("[Reconcile] Background reconciliation disabled by config")
            return
        interval = self.config.reconcile.interval_seconds

        def _loop() -> None:
            # Event.wait doubles as the sleep and the shutdown signal: it returns True
            # only once close() sets the stop event, cleanly ending the loop.
            while not self._stop.wait(interval):
                try:
                    self._reconcile_once()
                except Exception as exc:
                    logger.error(f"[Reconcile] Reconciliation pass failed: {exc}", exc_info=True)

        self._reconcile_thread = threading.Thread(
            target=_loop, name="access-provisioner-reconcile", daemon=True
        )
        self._reconcile_thread.start()
        logger.info(f"[Reconcile] Background reconciliation every {interval}s")

    def _reconcile_once(self) -> None:
        """One reconcile pass: ensure tables, then provision backlog / expire / SLA.

        Holds the connection lock for the whole pass so it never races the live-event
        handler. Each phase is isolated: a transient failure in one phase (or the initial
        connect) is logged and the next scheduled pass retries, rather than aborting the
        others.
        """
        with self._lock:
            try:
                conn = self._get_snowflake_connection()
                ensure_state_tables(conn, self.config.state)
            except Exception as exc:
                logger.error(
                    f"[Reconcile] Cannot connect to Snowflake or create state tables: {exc}",
                    exc_info=True,
                )
                return

            for phase in (
                self._catchup_approved_requests,
                self._catchup_expiry,
                self._catchup_sla,
            ):
                try:
                    phase()
                except Exception as exc:
                    logger.error(f"[Reconcile] Phase {phase.__name__} failed: {exc}", exc_info=True)

    def _catchup_approved_requests(self) -> None:
        """Provision any approved requests not yet in the state table."""
        config_field_ids = self._field_id_map()
        approved = fetch_all_approved_requests(
            self.ctx.graph,
            config_field_ids,
            lookback_days=self.config.lookback_days,
        )
        conn = self._get_snowflake_connection()
        new_count = 0
        for request in approved:
            if is_already_provisioned(conn, request.urn, self.config.state):
                logger.debug(f"[Catchup] {request.urn} already provisioned — skipping")
                continue
            if is_provisioning_failed(conn, request.urn, self.config.state):
                logger.debug(
                    f"[Catchup] {request.urn} has a permanent provisioning failure — skipping"
                )
                continue
            self._provision(request)
            new_count += 1
        logger.info(f"[Catchup] Provisioned {new_count} new request(s) from backlog")

    def _catchup_expiry(self) -> None:
        """Revoke any grants that expired since the last run."""
        if not self.config.expiry.enabled:
            return
        conn = self._get_snowflake_connection()
        expired = get_expired_grants(conn, self.config.state)
        for grant in expired:
            logger.info(
                f"[Expiry] Revoking expired grant for {grant.action_request_urn} "
                f"(role={grant.snowflake_role}, db={grant.snowflake_database})"
            )
            try:
                revoke_access(conn, grant, self.config.provisioning)
                record_revocation(conn, grant, self.config.state)
            except Exception as exc:
                logger.error(
                    f"[Expiry] Failed to revoke {grant.action_request_urn}: {exc}",
                    exc_info=True,
                )
                continue

            if self.config.expiry.revocation_notification and claim_stage(
                conn, grant.action_request_urn, LEDGER_STAGE_REVOCATION_NOTIFIED, self.config.state
            ):
                try:
                    send_revocation_notification(self.config.smtp, grant)
                except Exception as exc:
                    logger.error(f"[Expiry] Failed to send revocation email: {exc}")

    def _catchup_sla(self) -> None:
        """Send SLA reminders/escalations for pending requests that breach configured thresholds."""
        config_field_ids = self._field_id_map()
        pending = fetch_pending_action_requests(self.ctx.graph, config_field_ids)
        now_ms = int(time.time() * 1000)
        conn = self._get_snowflake_connection()

        for req in pending:
            self._evaluate_sla(req, now_ms, conn)

    # ------------------------------------------------------------------
    # Live event handler
    # ------------------------------------------------------------------

    def act(self, event: EventEnvelope) -> None:
        if event.event_type != "MetadataChangeLogEvent_v1":
            return

        mcl: MetadataChangeLogEvent = event.event
        if getattr(mcl, "entityType", None) != "actionRequest":
            return
        if getattr(mcl, "aspectName", None) != _ASPECT_ACTION_REQUEST_STATUS:
            return

        entity_urn = getattr(mcl, "entityUrn", None)
        if not entity_urn:
            return

        logger.debug(f"[Live] actionRequestStatus change on {entity_urn}")
        # Serialise with the background reconciler — both share one Snowflake connection.
        with self._lock:
            self._handle_status_change(entity_urn)

    # ------------------------------------------------------------------
    # Status change handler (used by both live events and catchup)
    # ------------------------------------------------------------------

    def _handle_status_change(self, action_request_urn: str) -> None:
        request = fetch_action_request(self.ctx.graph, action_request_urn, self._field_id_map())
        if not request:
            logger.warning(f"[Live] Could not fetch request {action_request_urn}")
            return
        if request.request_type != ACTION_REQUEST_TYPE_WORKFLOW:
            return

        if request.is_approved:
            conn = self._get_snowflake_connection()
            if is_already_provisioned(conn, action_request_urn, self.config.state):
                logger.info(
                    f"[Live] {action_request_urn} already provisioned — skipping duplicate event"
                )
                return
            # Parity with the catchup path: never re-attempt a request already recorded
            # as a permanent failure (e.g. role/object does not exist).
            if is_provisioning_failed(conn, action_request_urn, self.config.state):
                logger.info(
                    f"[Live] {action_request_urn} has a permanent provisioning failure — skipping"
                )
                return
            self._provision(request)
        elif request.is_denied:
            # Claim the denial-notification stage first so a duplicate/replayed event
            # never sends a second denial email.
            conn = self._get_snowflake_connection()
            if not claim_stage(
                conn, action_request_urn, LEDGER_STAGE_DENIAL_NOTIFIED, self.config.state
            ):
                return
            try:
                send_denial_notification(self.config.smtp, request)
            except Exception as exc:
                logger.error(f"[Live] Failed to send denial email: {exc}")

    # ------------------------------------------------------------------
    # Provisioning
    # ------------------------------------------------------------------

    def _provision(self, request: AccessRequest) -> None:
        ff = request.form_fields
        conn = self._get_snowflake_connection()
        role = self._resolve_snowflake_role(request, conn)
        database = ff.snowflake_database

        if not role or not database:
            logger.error(
                f"[Provision] Request {request.urn} missing required fields "
                f"(role={role!r}, database={database!r}) — skipping"
            )
            # A missing database is a permanent form/mapping misconfiguration, so record a
            # tombstone to stop retrying it on every reconcile pass. A missing role is left
            # retryable: it can stem from a transient DESCRIBE USER failure during default-
            # role resolution rather than a genuinely absent role.
            if not database:
                try:
                    record_provisioning_error(
                        conn,
                        request.urn,
                        "MISSING_FIELDS",
                        f"Missing required field(s): role={role!r}, database={database!r}",
                        self.config.state,
                    )
                except Exception as exc:
                    logger.error(
                        f"[Provision] Failed to record missing-field state for {request.urn}: {exc}"
                    )
            return

        logger.info(
            f"[Provision] role={role} database={database} schema={ff.snowflake_schema!r} "
            f"for request {request.urn}"
        )

        try:
            sql_statements = provision_access(
                conn=conn,
                role=role,
                database=database,
                schema=ff.snowflake_schema,
                warehouse=self.config.provisioning.default_warehouse,
                provisioning=self.config.provisioning,
            )
            logger.info(
                f"[Provision] {len(sql_statements)} statement(s) executed for {request.urn}"
            )
        except Exception as exc:
            logger.error(f"[Provision] Snowflake error for {request.urn}: {exc}", exc_info=True)
            if is_permanent_snowflake_error(exc):
                error_code = str(getattr(exc, "errno", "")) or None
                error_msg = str(exc)
                # Check before sending so the notification is only ever sent once,
                # even if the errors-table write fails and the catchup guard misses.
                already_notified = False
                try:
                    already_notified = is_provisioning_failed(conn, request.urn, self.config.state)
                except Exception:
                    pass
                try:
                    record_provisioning_error(
                        conn, request.urn, error_code, error_msg, self.config.state
                    )
                except Exception as rec_exc:
                    logger.error(
                        f"[Provision] Failed to record error state for {request.urn}: {rec_exc}"
                    )
                if not already_notified:
                    try:
                        send_provisioning_failure_notification(self.config.smtp, request, error_msg)
                    except Exception as mail_exc:
                        logger.error(
                            f"[Provision] Failed to send failure notification "
                            f"for {request.urn}: {mail_exc}"
                        )
            return

        # Persist grant to Snowflake state table
        expires_at_ms: int | None = None
        if ff.access_duration_days:
            expires_at_ms = int(time.time() * 1000) + ff.access_duration_days * 86_400_000

        requestor_email = ff.requestor_email or self._extract_requestor_email(request.requestor_urn)

        grant = GrantRecord(
            action_request_urn=request.urn,
            snowflake_role=role,
            snowflake_database=database,
            snowflake_schema=ff.snowflake_schema,
            requestor_email=requestor_email,
            granted_at_ms=int(time.time() * 1000),
            expires_at_ms=expires_at_ms,
        )
        try:
            record_grant(conn, grant, self.config.state)
        except Exception as exc:
            logger.error(f"[Provision] Failed to record grant state for {request.urn}: {exc}")

        # Claim the approval-notification stage before sending: if record_grant failed above
        # and a later pass re-provisions (grants are idempotent), the email is still sent
        # exactly once.
        if claim_stage(conn, request.urn, LEDGER_STAGE_APPROVAL_NOTIFIED, self.config.state):
            try:
                send_approval_notification(self.config.smtp, request, sql_statements)
            except Exception as exc:
                logger.error(f"[Provision] Failed to send approval email: {exc}")

    # ------------------------------------------------------------------
    # SLA evaluation
    # ------------------------------------------------------------------

    def _evaluate_sla(self, req: PendingRequestSummary, now_ms: int, conn: Any) -> None:
        if not req.created_ms:
            return

        pending_hours = (now_ms - req.created_ms) / 3_600_000
        assignee_emails = [req.requestor_email] if req.requestor_email else []

        if pending_hours >= self.config.sla.escalation_after_hours:
            if not is_sla_notified(conn, req.urn, _SLA_TYPE_ESCALATION, self.config.state):
                logger.info(f"[SLA] Escalating {req.urn} (pending {pending_hours:.1f}h)")
                try:
                    send_escalation_alert(
                        smtp_config=self.config.smtp,
                        action_request_urn=req.urn,
                        resource=req.resource,
                        pending_hours=pending_hours,
                        assignee_emails=assignee_emails,
                        escalation_recipients=self.config.sla.escalation_recipients,
                    )
                    record_sla_notification(conn, req.urn, _SLA_TYPE_ESCALATION, self.config.state)
                except Exception as exc:
                    logger.error(f"[SLA] Failed escalation for {req.urn}: {exc}")

        elif pending_hours >= self.config.sla.warning_after_hours:
            if not is_sla_notified(conn, req.urn, _SLA_TYPE_WARNING, self.config.state):
                logger.info(f"[SLA] Warning for {req.urn} (pending {pending_hours:.1f}h)")
                try:
                    send_sla_warning(
                        smtp_config=self.config.smtp,
                        action_request_urn=req.urn,
                        resource=req.resource,
                        pending_hours=pending_hours,
                        assignee_emails=assignee_emails,
                    )
                    record_sla_notification(conn, req.urn, _SLA_TYPE_WARNING, self.config.state)
                except Exception as exc:
                    logger.error(f"[SLA] Failed warning for {req.urn}: {exc}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _field_id_map(self) -> dict[str, str]:
        return {
            "field_snowflake_database": self.config.field_snowflake_database,
            "field_snowflake_schema": self.config.field_snowflake_schema,
            "field_snowflake_role": self.config.field_snowflake_role,
            "field_access_duration_days": self.config.field_access_duration_days,
            "field_requestor_email": self.config.field_requestor_email,
            "field_justification": self.config.field_justification,
        }

    def _extract_snowflake_username(self, requestor_urn: str) -> str | None:
        """Derive a Snowflake username from a DataHub corpuser URN.

        Strips the ``urn:li:corpuser:`` prefix and then applies the configured
        ``requestor_username_format``:
          - ``'urn_id'`` (default): use the identity segment as-is
            (e.g. ``john.doe@company.com``).
          - ``'email_local_part'``: strip the ``@domain`` suffix
            (e.g. ``john.doe``).
        """
        prefix = "urn:li:corpuser:"
        if not requestor_urn.startswith(prefix):
            logger.warning(f"[Provision] Unexpected requestor URN format: {requestor_urn!r}")
            return None
        urn_id = requestor_urn[len(prefix) :]
        fmt = self.config.provisioning.requestor_username_format
        if fmt == "email_local_part" and "@" in urn_id:
            return urn_id.split("@")[0]
        return urn_id

    def _extract_requestor_email(self, requestor_urn: str | None) -> str | None:
        """Best-effort email extraction from the requestor URN.

        If the URN identity segment looks like an email address (contains ``@``),
        return it directly. Otherwise return None.
        """
        if not requestor_urn:
            return None
        prefix = "urn:li:corpuser:"
        if requestor_urn.startswith(prefix):
            urn_id = requestor_urn[len(prefix) :]
            if "@" in urn_id:
                return urn_id
        return None

    def _resolve_snowflake_role(self, request: AccessRequest, conn: Any) -> str | None:
        """Return the Snowflake role to grant access to.

        Resolution order:
        1. ``snowflake_role`` form field — explicit, takes priority.
        2. Look up the requestor's Snowflake ``DEFAULT_ROLE`` via
           ``DESCRIBE USER "<username>"``, deriving the username from the
           requestor's DataHub URN using the configured
           ``requestor_username_format``.
        """
        if request.form_fields.snowflake_role:
            return request.form_fields.snowflake_role

        if not request.requestor_urn:
            logger.warning(
                f"[Provision] No snowflake_role in form and no requestor_urn "
                f"for {request.urn} — cannot resolve role"
            )
            return None

        username = self._extract_snowflake_username(request.requestor_urn)
        if not username:
            return None

        role = get_user_default_role(conn, username)
        if role:
            logger.info(
                f"[Provision] Resolved Snowflake role '{role}' for user '{username}' "
                f"via DESCRIBE USER (requestor_urn={request.requestor_urn})"
            )
        else:
            logger.warning(
                f"[Provision] Could not resolve DEFAULT_ROLE for Snowflake user "
                f"'{username}' (requestor_urn={request.requestor_urn})"
            )
        return role

    def _get_snowflake_connection(self) -> Any:
        if self._snowflake_conn is None:
            logger.info("[AccessProvisioner] Connecting to Snowflake…")
            self._snowflake_conn = get_connection(self.config.snowflake_connection)
            logger.info("[AccessProvisioner] Snowflake connection established")
        return self._snowflake_conn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        # Signal the reconciler to stop and wait for any in-flight pass to finish so we
        # never close the connection out from under it.
        self._stop.set()
        if self._reconcile_thread is not None:
            self._reconcile_thread.join(timeout=30)
            self._reconcile_thread = None
        with self._lock:
            if self._snowflake_conn:
                self._snowflake_conn.close()
                self._snowflake_conn = None
        logger.info("[AccessProvisioner] Closed")
