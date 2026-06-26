import logging
import time
from typing import Any

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import MetadataChangeLogEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from action_access_provisioner import databricks as dbx
from action_access_provisioner.config import DatabricksAccessProvisionerConfig
from action_access_provisioner.email import (
    send_dbx_approval_notification,
    send_dbx_provisioning_failure_notification,
    send_dbx_revocation_notification,
    send_denial_notification,
    send_escalation_alert,
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
    DatabricksGrantRecord,
    PendingRequestSummary,
)

logger = logging.getLogger(__name__)

_ASPECT_ACTION_REQUEST_STATUS = "actionRequestStatus"

_SLA_TYPE_WARNING = "warning"
_SLA_TYPE_ESCALATION = "escalation"


class DatabricksAccessProvisionerAction(Action):
    """DataHub Actions handler for automated Databricks Unity Catalog access provisioning."""

    def __init__(self, config: DatabricksAccessProvisionerConfig, ctx: PipelineContext) -> None:
        self.config = config
        self.ctx = ctx
        self._sql_conn: Any = None
        self._workspace_client: Any = None

        logger.info("[DatabricksAccessProvisioner] Initialised")
        if config.provisioning.dry_run:
            logger.warning("[DatabricksAccessProvisioner] DRY RUN mode — no grants will be applied")

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        config = DatabricksAccessProvisionerConfig.model_validate(config_dict or {})
        action = cls(config, ctx)
        action._startup_catchup()
        return action

    # ------------------------------------------------------------------
    # Startup catchup pass
    # ------------------------------------------------------------------

    def _startup_catchup(self) -> None:
        logger.info("[Catchup] Starting startup catchup pass…")
        try:
            conn = self._get_sql_conn()
            dbx.ensure_state_tables(conn, self.config.state)
        except Exception as exc:
            logger.error(
                f"[Catchup] Cannot connect to Databricks or create state tables: {exc}",
                exc_info=True,
            )
            return

        self._catchup_approved_requests()
        self._catchup_expiry()
        self._catchup_sla()
        logger.info("[Catchup] Startup catchup pass complete")

    def _catchup_approved_requests(self) -> None:
        approved = fetch_all_approved_requests(
            self.ctx.graph,
            self._field_id_map(),
            lookback_days=self.config.lookback_days,
        )
        conn = self._get_sql_conn()
        new_count = 0
        for request in approved:
            if dbx.is_already_provisioned(conn, request.urn, self.config.state):
                logger.debug(f"[Catchup] {request.urn} already provisioned — skipping")
                continue
            if dbx.is_provisioning_failed(conn, request.urn, self.config.state):
                logger.debug(
                    f"[Catchup] {request.urn} has a permanent provisioning failure — skipping"
                )
                continue
            self._provision(request)
            new_count += 1
        logger.info(f"[Catchup] Provisioned {new_count} new request(s) from backlog")

    def _catchup_expiry(self) -> None:
        if not self.config.expiry.enabled:
            return
        conn = self._get_sql_conn()
        for grant in dbx.get_expired_grants(conn, self.config.state):
            logger.info(
                f"[Expiry] Revoking expired grant for {grant.action_request_urn} "
                f"(principal={grant.principal}, catalog={grant.catalog})"
            )
            try:
                dbx.revoke_access(
                    sql_conn=conn,
                    workspace_client=self._get_workspace_client(),
                    grant=grant,
                    provisioning=self.config.provisioning,
                )
                dbx.record_revocation(conn, grant, self.config.state)
            except Exception as exc:
                logger.error(
                    f"[Expiry] Failed to revoke {grant.action_request_urn}: {exc}",
                    exc_info=True,
                )
                continue

            if self.config.expiry.revocation_notification:
                try:
                    send_dbx_revocation_notification(self.config.smtp, grant)
                except Exception as exc:
                    logger.error(f"[Expiry] Failed to send revocation email: {exc}")

    def _catchup_sla(self) -> None:
        pending = fetch_pending_action_requests(self.ctx.graph, self._field_id_map())
        now_ms = int(time.time() * 1000)
        conn = self._get_sql_conn()
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

    def _handle_status_change(self, action_request_urn: str) -> None:
        request = fetch_action_request(self.ctx.graph, action_request_urn, self._field_id_map())
        if not request:
            logger.warning(f"[Live] Could not fetch request {action_request_urn}")
            return
        if request.request_type != ACTION_REQUEST_TYPE_WORKFLOW:
            return

        if request.is_approved:
            conn = self._get_sql_conn()
            if dbx.is_already_provisioned(conn, action_request_urn, self.config.state):
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
        principal = self._resolve_principal(request)
        catalog = ff.databricks_catalog

        if not principal or not catalog:
            logger.error(
                f"[Provision] Request {request.urn} missing required fields "
                f"(principal={principal!r}, catalog={catalog!r}) — skipping"
            )
            return

        logger.info(
            f"[Provision] principal={principal} catalog={catalog} "
            f"schema={ff.databricks_schema!r} table={ff.databricks_table!r} "
            f"for request {request.urn}"
        )

        conn = self._get_sql_conn()
        try:
            statements = dbx.provision_access(
                sql_conn=conn,
                workspace_client=self._get_workspace_client(),
                principal=principal,
                catalog=catalog,
                schema=ff.databricks_schema,
                table=ff.databricks_table,
                provisioning=self.config.provisioning,
            )
            logger.info(f"[Provision] {len(statements)} grant(s) applied for {request.urn}")
        except Exception as exc:
            logger.error(f"[Provision] Databricks error for {request.urn}: {exc}", exc_info=True)
            self._handle_provision_failure(conn, request, exc)
            return

        expires_at_ms: int | None = None
        if ff.access_duration_days:
            expires_at_ms = int(time.time() * 1000) + ff.access_duration_days * 86_400_000

        grant = DatabricksGrantRecord(
            action_request_urn=request.urn,
            principal=principal,
            catalog=catalog,
            schema=ff.databricks_schema,
            table=ff.databricks_table,
            requestor_email=ff.requestor_email
            or self._extract_requestor_email(request.requestor_urn),
            granted_at_ms=int(time.time() * 1000),
            expires_at_ms=expires_at_ms,
        )
        try:
            dbx.record_grant(conn, grant, self.config.state)
        except Exception as exc:
            logger.error(f"[Provision] Failed to record grant state for {request.urn}: {exc}")

        try:
            send_dbx_approval_notification(self.config.smtp, request, statements)
        except Exception as exc:
            logger.error(f"[Provision] Failed to send approval email: {exc}")

    def _handle_provision_failure(self, conn: Any, request: AccessRequest, exc: Exception) -> None:
        if not dbx.is_permanent_databricks_error(exc):
            return
        already_notified = False
        try:
            already_notified = dbx.is_provisioning_failed(conn, request.urn, self.config.state)
        except Exception:
            pass
        try:
            dbx.record_provisioning_error(conn, request.urn, None, str(exc), self.config.state)
        except Exception as rec_exc:
            logger.error(f"[Provision] Failed to record error state for {request.urn}: {rec_exc}")
        if not already_notified:
            try:
                send_dbx_provisioning_failure_notification(self.config.smtp, request, str(exc))
            except Exception as mail_exc:
                logger.error(
                    f"[Provision] Failed to send failure notification for {request.urn}: {mail_exc}"
                )

    # ------------------------------------------------------------------
    # SLA evaluation
    # ------------------------------------------------------------------

    def _evaluate_sla(self, req: PendingRequestSummary, now_ms: int, conn: Any) -> None:
        if not req.created_ms:
            return

        pending_hours = (now_ms - req.created_ms) / 3_600_000
        assignee_emails = [req.requestor_email] if req.requestor_email else []

        if pending_hours >= self.config.sla.escalation_after_hours:
            if not dbx.is_sla_notified(conn, req.urn, _SLA_TYPE_ESCALATION, self.config.state):
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
                    dbx.record_sla_notification(
                        conn, req.urn, _SLA_TYPE_ESCALATION, self.config.state
                    )
                except Exception as exc:
                    logger.error(f"[SLA] Failed escalation for {req.urn}: {exc}")

        elif pending_hours >= self.config.sla.warning_after_hours:
            if not dbx.is_sla_notified(conn, req.urn, _SLA_TYPE_WARNING, self.config.state):
                logger.info(f"[SLA] Warning for {req.urn} (pending {pending_hours:.1f}h)")
                try:
                    send_sla_warning(
                        smtp_config=self.config.smtp,
                        action_request_urn=req.urn,
                        resource=req.resource,
                        pending_hours=pending_hours,
                        assignee_emails=assignee_emails,
                    )
                    dbx.record_sla_notification(conn, req.urn, _SLA_TYPE_WARNING, self.config.state)
                except Exception as exc:
                    logger.error(f"[SLA] Failed warning for {req.urn}: {exc}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _field_id_map(self) -> dict[str, str]:
        return {
            "field_databricks_catalog": self.config.field_databricks_catalog,
            "field_databricks_schema": self.config.field_databricks_schema,
            "field_databricks_table": self.config.field_databricks_table,
            "field_access_duration_days": self.config.field_access_duration_days,
            "field_requestor_email": self.config.field_requestor_email,
            "field_justification": self.config.field_justification,
        }

    def _resolve_principal(self, request: AccessRequest) -> str | None:
        """The Databricks principal is always the requestor's email.

        Prefer the explicit ``requestor_email`` form field; fall back to the
        requestor's corpuser URN identity when it is an email address.
        """
        if request.form_fields.requestor_email:
            return request.form_fields.requestor_email
        return self._extract_requestor_email(request.requestor_urn)

    @staticmethod
    def _extract_requestor_email(requestor_urn: str | None) -> str | None:
        if not requestor_urn:
            return None
        prefix = "urn:li:corpuser:"
        if requestor_urn.startswith(prefix):
            urn_id = requestor_urn[len(prefix) :]
            if "@" in urn_id:
                return urn_id
        return None

    def _get_sql_conn(self) -> Any:
        if self._sql_conn is None:
            logger.info("[DatabricksAccessProvisioner] Connecting to Databricks SQL warehouse…")
            self._sql_conn = dbx.get_sql_connection(self.config.databricks_connection)
            logger.info("[DatabricksAccessProvisioner] Databricks SQL connection established")
        return self._sql_conn

    def _get_workspace_client(self) -> Any:
        # Only needed for grant_method='sdk'; built lazily so SQL-only setups
        # never require the databricks-sdk grants path.
        if self.config.provisioning.grant_method != "sdk":
            return None
        if self._workspace_client is None:
            self._workspace_client = self.config.databricks_connection.get_workspace_client()
        return self._workspace_client

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._sql_conn:
            try:
                self._sql_conn.close()
            except Exception:
                pass
            self._sql_conn = None
        logger.info("[DatabricksAccessProvisioner] Closed")
