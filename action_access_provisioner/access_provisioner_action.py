"""DataHub Action: automated Snowflake access provisioning, SLA tracking, and expiry revocation."""

import logging
import threading
import time
from typing import Any, Optional

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import MetadataChangeLogEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from .config import AccessProvisionerConfig
from .email import (
    send_approval_notification,
    send_denial_notification,
    send_escalation_alert,
    send_revocation_notification,
    send_sla_warning,
)
from .graphql import fetch_action_request, fetch_pending_action_requests
from .models import (
    ACTION_REQUEST_TYPE_WORKFLOW,
    GrantRecord,
    PendingRequestSummary,
)
from .snowflake import get_connection, provision_access, revoke_access

logger = logging.getLogger(__name__)

# MCL aspect names we care about
_ASPECT_ACTION_REQUEST_STATUS = "actionRequestStatus"

# Track which SLA warnings have already been sent to avoid duplicate emails.
# Key: action_request_urn  Value: set of thresholds (hours) already notified.
_SlaTracker = dict[str, set]


class AccessProvisionerAction(Action):
    """
    DataHub Actions handler that:

    1. Reacts to ``actionRequestStatus`` MCL events and, when an ACCESS workflow
       request is APPROVED, executes Snowflake GRANT statements and sends a
       confirmation email.
    2. Runs a background thread that polls for pending requests exceeding SLA
       thresholds and sends reminder / escalation emails.
    3. Runs a background thread that polls for expired grants and auto-revokes
       the corresponding Snowflake privileges, then notifies the original requestor.
    """

    def __init__(self, config: AccessProvisionerConfig, ctx: PipelineContext) -> None:
        self.config = config
        self.ctx = ctx
        self._snowflake_conn: Any = None

        # In-memory store of active grants keyed by action_request_urn.
        # In production you may want to back this with a persistent store.
        self._active_grants: dict[str, GrantRecord] = {}

        # Track which SLA thresholds have already been notified per request.
        self._sla_notified: _SlaTracker = {}

        self._stop_event = threading.Event()
        self._background_threads: list[threading.Thread] = []

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
        action._start_background_threads()
        return action

    # ------------------------------------------------------------------
    # Main event handler
    # ------------------------------------------------------------------

    def act(self, event: EventEnvelope) -> None:
        if event.event_type != "MetadataChangeLogEvent_v1":
            return

        mcl: MetadataChangeLogEvent = event.event  # type: ignore[assignment]
        entity_type = getattr(mcl, "entityType", None)
        aspect_name = getattr(mcl, "aspectName", None)

        if entity_type != "actionRequest" or aspect_name != _ASPECT_ACTION_REQUEST_STATUS:
            return

        entity_urn = getattr(mcl, "entityUrn", None)
        if not entity_urn:
            return

        logger.debug(f"[AccessProvisioner] actionRequestStatus change on {entity_urn}")
        self._handle_status_change(entity_urn)

    # ------------------------------------------------------------------
    # Status change handler
    # ------------------------------------------------------------------

    def _handle_status_change(self, action_request_urn: str) -> None:
        """Fetch the full request and react to its new status."""
        config_field_ids = {
            "field_snowflake_database": self.config.field_snowflake_database,
            "field_snowflake_schema": self.config.field_snowflake_schema,
            "field_snowflake_role": self.config.field_snowflake_role,
            "field_access_duration_days": self.config.field_access_duration_days,
            "field_requestor_email": self.config.field_requestor_email,
            "field_justification": self.config.field_justification,
        }

        request = fetch_action_request(self.ctx.graph, action_request_urn, config_field_ids)
        if not request:
            logger.warning(f"[AccessProvisioner] Could not fetch request {action_request_urn}")
            return

        if request.request_type != ACTION_REQUEST_TYPE_WORKFLOW:
            logger.debug(
                f"[AccessProvisioner] Ignoring non-workflow request type {request.request_type}"
            )
            return

        if request.is_approved:
            self._provision(request)
        elif request.is_denied:
            logger.info(
                f"[AccessProvisioner] Request {action_request_urn} denied — sending notification"
            )
            try:
                send_denial_notification(self.config.smtp, request)
            except Exception as exc:
                logger.error(f"[AccessProvisioner] Failed to send denial email: {exc}")
        else:
            logger.debug(
                f"[AccessProvisioner] Request {action_request_urn} status={request.status} — no action"
            )

    # ------------------------------------------------------------------
    # Provisioning
    # ------------------------------------------------------------------

    def _provision(self, request) -> None:  # type: ignore[no-untyped-def]
        ff = request.form_fields
        role = ff.snowflake_role
        database = ff.snowflake_database

        if not role or not database:
            logger.error(
                f"[AccessProvisioner] Request {request.urn} is missing required fields "
                f"(role={role!r}, database={database!r}) — skipping provisioning"
            )
            return

        logger.info(
            f"[AccessProvisioner] Provisioning: role={role} database={database} "
            f"schema={ff.snowflake_schema!r} for request {request.urn}"
        )

        try:
            conn = self._get_snowflake_connection()
            sql_statements = provision_access(
                conn=conn,
                role=role,
                database=database,
                schema=ff.snowflake_schema,
                warehouse=self.config.provisioning.default_warehouse,
                provisioning=self.config.provisioning,
            )
            logger.info(
                f"[AccessProvisioner] Provisioned {len(sql_statements)} statement(s) for {request.urn}"
            )
        except Exception as exc:
            logger.error(
                f"[AccessProvisioner] Snowflake provisioning failed for {request.urn}: {exc}",
                exc_info=True,
            )
            return

        # Record the grant for later expiry checking
        expires_at_ms: Optional[int] = None
        if ff.access_duration_days:
            expires_at_ms = int(time.time() * 1000) + ff.access_duration_days * 86_400_000

        grant = GrantRecord(
            action_request_urn=request.urn,
            snowflake_role=role,
            snowflake_database=database,
            snowflake_schema=ff.snowflake_schema,
            requestor_email=ff.requestor_email,
            granted_at_ms=int(time.time() * 1000),
            expires_at_ms=expires_at_ms,
        )
        self._active_grants[request.urn] = grant

        # Send approval email
        try:
            send_approval_notification(self.config.smtp, request, sql_statements)
        except Exception as exc:
            logger.error(f"[AccessProvisioner] Failed to send approval email: {exc}")

    # ------------------------------------------------------------------
    # Background threads
    # ------------------------------------------------------------------

    def _start_background_threads(self) -> None:
        sla_thread = threading.Thread(
            target=self._sla_monitor_loop,
            name="access-provisioner-sla-monitor",
            daemon=True,
        )
        sla_thread.start()
        self._background_threads.append(sla_thread)

        if self.config.expiry.enabled:
            expiry_thread = threading.Thread(
                target=self._expiry_monitor_loop,
                name="access-provisioner-expiry-monitor",
                daemon=True,
            )
            expiry_thread.start()
            self._background_threads.append(expiry_thread)

        logger.info(
            f"[AccessProvisioner] Started {len(self._background_threads)} background thread(s)"
        )

    def _sla_monitor_loop(self) -> None:
        """Periodically poll DataHub for pending requests and send SLA reminders."""
        interval = self.config.sla.check_interval_seconds
        logger.info(f"[SLA Monitor] Starting (check every {interval}s)")

        while not self._stop_event.is_set():
            try:
                self._check_sla()
            except Exception as exc:
                logger.error(f"[SLA Monitor] Unexpected error: {exc}", exc_info=True)
            self._stop_event.wait(interval)

    def _expiry_monitor_loop(self) -> None:
        """Periodically check the in-memory grant registry and revoke expired grants."""
        interval = self.config.expiry.check_interval_seconds
        logger.info(f"[Expiry Monitor] Starting (check every {interval}s)")

        while not self._stop_event.is_set():
            try:
                self._check_expiry()
            except Exception as exc:
                logger.error(f"[Expiry Monitor] Unexpected error: {exc}", exc_info=True)
            self._stop_event.wait(interval)

    def _check_sla(self) -> None:
        config_field_ids = {
            "field_snowflake_database": self.config.field_snowflake_database,
            "field_snowflake_schema": self.config.field_snowflake_schema,
            "field_snowflake_role": self.config.field_snowflake_role,
            "field_access_duration_days": self.config.field_access_duration_days,
            "field_requestor_email": self.config.field_requestor_email,
            "field_justification": self.config.field_justification,
        }
        pending = fetch_pending_action_requests(self.ctx.graph, config_field_ids)
        now_ms = int(time.time() * 1000)

        for req in pending:
            self._evaluate_sla(req, now_ms)

    def _evaluate_sla(self, req: PendingRequestSummary, now_ms: int) -> None:
        if not req.created_ms:
            return

        pending_hours = (now_ms - req.created_ms) / 3_600_000
        notified = self._sla_notified.setdefault(req.urn, set())

        assignee_emails: list[str] = []
        # In practice you would look up email addresses for assignee URNs.
        # We surface what we have — the requestor_email is a fallback.
        if req.requestor_email:
            assignee_emails.append(req.requestor_email)

        if pending_hours >= self.config.sla.escalation_after_hours and "escalation" not in notified:
            logger.info(f"[SLA Monitor] Escalating {req.urn} (pending {pending_hours:.1f}h)")
            try:
                send_escalation_alert(
                    smtp_config=self.config.smtp,
                    action_request_urn=req.urn,
                    resource=req.resource,
                    pending_hours=pending_hours,
                    assignee_emails=assignee_emails,
                    escalation_recipients=self.config.sla.escalation_recipients,
                )
                notified.add("escalation")
            except Exception as exc:
                logger.error(f"[SLA Monitor] Failed to send escalation: {exc}")

        elif pending_hours >= self.config.sla.warning_after_hours and "warning" not in notified:
            logger.info(f"[SLA Monitor] Warning for {req.urn} (pending {pending_hours:.1f}h)")
            try:
                send_sla_warning(
                    smtp_config=self.config.smtp,
                    action_request_urn=req.urn,
                    resource=req.resource,
                    pending_hours=pending_hours,
                    assignee_emails=assignee_emails,
                )
                notified.add("warning")
            except Exception as exc:
                logger.error(f"[SLA Monitor] Failed to send warning: {exc}")

    def _check_expiry(self) -> None:
        now_ms = int(time.time() * 1000)
        expired_urns = [
            urn
            for urn, grant in self._active_grants.items()
            if grant.has_expiry
            and grant.expires_at_ms is not None
            and grant.expires_at_ms <= now_ms
        ]

        for urn in expired_urns:
            grant = self._active_grants.pop(urn)
            logger.info(
                f"[Expiry Monitor] Revoking expired grant for {urn} "
                f"(role={grant.snowflake_role}, db={grant.snowflake_database})"
            )
            try:
                conn = self._get_snowflake_connection()
                revoke_access(conn, grant, self.config.provisioning)
            except Exception as exc:
                logger.error(
                    f"[Expiry Monitor] Failed to revoke access for {urn}: {exc}", exc_info=True
                )
                # Put the grant back so we retry on the next cycle
                self._active_grants[urn] = grant
                continue

            if self.config.expiry.revocation_notification:
                try:
                    send_revocation_notification(self.config.smtp, grant)
                except Exception as exc:
                    logger.error(f"[Expiry Monitor] Failed to send revocation email: {exc}")

    # ------------------------------------------------------------------
    # Snowflake connection management
    # ------------------------------------------------------------------

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
        logger.info("[AccessProvisioner] Shutting down…")
        self._stop_event.set()
        for t in self._background_threads:
            t.join(timeout=5)
        if self._snowflake_conn:
            self._snowflake_conn.close()
            self._snowflake_conn = None
        logger.info("[AccessProvisioner] Shutdown complete")
