"""DataHub Action: automated Snowflake access provisioning, SLA tracking, and expiry revocation.

Design note — scheduled invocation model
-----------------------------------------
The DataHub Cloud executor kills Actions after ~5 minutes of idle time, so this
action is designed to be run on a schedule (e.g. every 30 minutes via cron or the
DataHub scheduler) rather than as a persistent daemon.

On each invocation the action:
  1. Runs a *catchup pass* at startup:
       a. Fetches all COMPLETED/APPROVED workflow requests from the last N days.
       b. Skips any already recorded in the Snowflake state table (idempotent).
       c. Provisions Snowflake access + sends email for any new approvals.
       d. Checks every active grant for expiry; revokes + emails if expired.
       e. Checks every pending request for SLA breaches; emails if not already notified.
  2. Listens for live MCL events during the remaining ~5-minute window and handles
     any new status-change events in real time.

All state (provisioned grants, sent SLA notifications) is stored in Snowflake tables
so it persists across invocations and prevents duplicate actions.
"""

import logging
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
    AccessRequest,
    GrantRecord,
    PendingRequestSummary,
)
from action_access_provisioner.snowflake import (
    ensure_state_tables,
    get_connection,
    get_expired_grants,
    is_already_provisioned,
    is_sla_notified,
    provision_access,
    record_grant,
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
        return action

    # ------------------------------------------------------------------
    # Startup catchup pass
    # ------------------------------------------------------------------

    def _startup_catchup(self) -> None:
        """
        Called once at startup. Processes any backlog of approved requests and
        handles expiry/SLA checks for the current state of the world.
        """
        logger.info("[Catchup] Starting startup catchup pass…")

        try:
            conn = self._get_snowflake_connection()
            ensure_state_tables(conn, self.config.state)
        except Exception as exc:
            logger.error(
                f"[Catchup] Cannot connect to Snowflake or create state tables: {exc}",
                exc_info=True,
            )
            return

        self._catchup_approved_requests()
        self._catchup_expiry()
        self._catchup_sla()

        logger.info("[Catchup] Startup catchup pass complete")

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

            if self.config.expiry.revocation_notification:
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
            self._provision(request)
        elif request.is_denied:
            try:
                send_denial_notification(self.config.smtp, request)
            except Exception as exc:
                logger.error(f"[Live] Failed to send denial email: {exc}")

    # ------------------------------------------------------------------
    # Provisioning
    # ------------------------------------------------------------------

    def _provision(self, request: AccessRequest) -> None:
        ff = request.form_fields
        role = ff.snowflake_role
        database = ff.snowflake_database

        if not role or not database:
            logger.error(
                f"[Provision] Request {request.urn} missing required fields "
                f"(role={role!r}, database={database!r}) — skipping"
            )
            return

        logger.info(
            f"[Provision] role={role} database={database} schema={ff.snowflake_schema!r} "
            f"for request {request.urn}"
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
                f"[Provision] {len(sql_statements)} statement(s) executed for {request.urn}"
            )
        except Exception as exc:
            logger.error(f"[Provision] Snowflake error for {request.urn}: {exc}", exc_info=True)
            return

        # Persist grant to Snowflake state table
        expires_at_ms: int | None = None
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
        try:
            record_grant(conn, grant, self.config.state)
        except Exception as exc:
            logger.error(f"[Provision] Failed to record grant state for {request.urn}: {exc}")

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
        if self._snowflake_conn:
            self._snowflake_conn.close()
            self._snowflake_conn = None
        logger.info("[AccessProvisioner] Closed")
