import logging
import time
from collections.abc import Callable
from typing import Any

from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import MetadataChangeLogEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext

from action_access_provisioner import databricks as dbx
from action_access_provisioner.config import (
    DatabricksAccessProvisionerConfig,
    GroupAccessMode,
    TicketingConfig,
    TicketingMode,
)
from action_access_provisioner.datahub_sync import DatahubSync
from action_access_provisioner.email import (
    send_dbx_approval_notification,
    send_dbx_membership_notification,
    send_dbx_membership_removal_notification,
    send_dbx_provisioning_failure_notification,
    send_dbx_revocation_notification,
    send_dbx_ticket_notification,
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
    DatabricksGroupMembershipRecord,
    PendingRequestSummary,
    corpuser_email_from_urn,
)
from action_access_provisioner.ticketing import TicketResult, create_access_ticket

logger = logging.getLogger(__name__)

_ASPECT_ACTION_REQUEST_STATUS = "actionRequestStatus"

_SLA_TYPE_WARNING = "warning"
_SLA_TYPE_ESCALATION = "escalation"


def _is_group_principal(principal: str) -> bool:
    # A grant's principal is either a Databricks group name or an individual
    # corpuser email; group names are never email addresses, so the absence of an
    # "@" distinguishes the two. ponytail: heuristic — the upgrade path is to persist
    # the principal kind on the grant record instead of inferring it.
    return "@" not in principal


class DatabricksAccessProvisionerAction(Action):
    """DataHub Actions handler for automated Databricks Unity Catalog access provisioning."""

    def __init__(self, config: DatabricksAccessProvisionerConfig, ctx: PipelineContext) -> None:
        self.config = config
        self.ctx = ctx
        self._sql_conn: Any = None
        self._workspace_client: Any = None
        self._datahub_sync: DatahubSync | None = None

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
            if self._already_provisioned(conn, request.urn):
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

            # Mirror the revocation for group grants at table granularity.
            if _is_group_principal(grant.principal) and grant.schema_name and grant.table:
                self._mirror_group_revoke(
                    grant.principal, grant.catalog, grant.schema_name, grant.table
                )

            if self.config.expiry.revocation_notification:
                try:
                    send_dbx_revocation_notification(self.config.smtp, grant)
                except Exception as exc:
                    logger.error(f"[Expiry] Failed to send revocation email: {exc}")

        for membership in dbx.get_expired_memberships(conn, self.config.state):
            logger.info(
                f"[Expiry] Removing expired membership for {membership.action_request_urn} "
                f"({membership.user_email} -> {membership.group_name})"
            )
            try:
                dbx.remove_group_member(
                    self._require_workspace_client(),
                    membership.group_name,
                    membership.user_email,
                    dry_run=self.config.provisioning.dry_run,
                )
                dbx.record_membership_removal(conn, membership, self.config.state)
            except Exception as exc:
                logger.error(
                    f"[Expiry] Failed to remove membership {membership.action_request_urn}: {exc}",
                    exc_info=True,
                )
                continue

            self._mirror_membership_remove(membership.group_name, membership.user_email)

            if self.config.expiry.revocation_notification:
                try:
                    send_dbx_membership_removal_notification(self.config.smtp, membership)
                except Exception as exc:
                    logger.error(f"[Expiry] Failed to send membership removal email: {exc}")

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
            if self._already_provisioned(conn, action_request_urn):
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
        # The requestor's email is always the notification recipient; it is also the
        # grantee unless the form requested a group (group-based access).
        requestor_email = self._resolve_requestor_email(request)

        ticketing = self.config.ticketing
        # 'replace' mode hands fulfilment to the ITSM tool, so no grant is issued.
        replace = ticketing is not None and ticketing.mode == TicketingMode.REPLACE

        # Membership access model: add the requestor to the requested group rather
        # than granting the object directly. Replace-mode ticketing still wins.
        group = request.form_fields.databricks_group
        if (
            not replace
            and group
            and self.config.provisioning.group_access_mode == GroupAccessMode.MEMBERSHIP
        ):
            self._provision_membership(request, requestor_email, group)
            return

        principal = self._resolve_grantee(request, requestor_email)
        # The grant target (catalog.schema.table) is derived from the dataset the
        # request was raised on — never from form fields — so it always matches the
        # entity and any platform_instance prefix is stripped (see the parser).
        target = dbx.parse_databricks_dataset_urn(request.resource)

        if not principal or target is None:
            logger.error(
                f"[Provision] Request {request.urn} cannot be provisioned "
                f"(principal={principal!r}, resource={request.resource!r}) — skipping"
            )
            if principal and request.resource and target is None:
                # A non-Databricks-dataset entity can never resolve to a UC target;
                # record a permanent failure so we don't retry it every catchup.
                self._record_invalid_target(request)
            return

        catalog, schema, table = target
        logger.info(
            f"[Provision] principal={principal} target={catalog}.{schema}.{table} "
            f"for request {request.urn}"
        )

        conn = self._get_sql_conn()

        statements: list[str] = []
        if not replace:
            try:
                statements = dbx.provision_access(
                    sql_conn=conn,
                    workspace_client=self._get_workspace_client(),
                    principal=principal,
                    catalog=catalog,
                    schema=schema,
                    table=table,
                    provisioning=self.config.provisioning,
                )
                logger.info(f"[Provision] {len(statements)} grant(s) applied for {request.urn}")
            except Exception as exc:
                logger.error(
                    f"[Provision] Databricks error for {request.urn}: {exc}", exc_info=True
                )
                self._handle_provision_failure(
                    conn, request, exc, requestor_email, catalog, schema, table
                )
                return

        ticket: TicketResult | None = None
        if ticketing is not None:
            try:
                ticket = self._open_ticket(ticketing, request, principal, catalog, schema, table)
                logger.info(
                    f"[Provision] Opened {ticketing.provider} ticket {ticket.key} for {request.urn}"
                )
            except Exception as exc:
                logger.error(
                    f"[Provision] Failed to open ticket for {request.urn}: {exc}", exc_info=True
                )
                if replace:
                    # The ticket is the only fulfilment in replace mode; with none
                    # filed there is nothing to record, so let the next catchup retry.
                    return

        # In replace mode no grant was issued, so there is nothing to auto-revoke.
        expires_at_ms: int | None = None
        if not replace and request.form_fields.access_duration_days:
            expires_at_ms = (
                int(time.time() * 1000) + request.form_fields.access_duration_days * 86_400_000
            )

        grant = DatabricksGrantRecord(
            action_request_urn=request.urn,
            principal=principal,
            catalog=catalog,
            schema_name=schema,
            table=table,
            # The grantee may be a group, but notifications (incl. expiry) always go
            # to the human who raised the request.
            requestor_email=requestor_email,
            granted_at_ms=int(time.time() * 1000),
            expires_at_ms=expires_at_ms,
        )
        try:
            dbx.record_grant(conn, grant, self.config.state)
        except Exception as exc:
            logger.error(f"[Provision] Failed to record grant state for {request.urn}: {exc}")

        # Mirror group grants into DataHub for auditing. Individual (user) grants are
        # not role-modelled here — that is the structured-property path in the design,
        # deferred until the policy SP definitions are registered.
        if not replace and group:
            self._mirror_group_grant(group, catalog, schema, table)

        self._send_approval(
            request, requestor_email, principal, catalog, schema, table, statements, ticket, replace
        )

    def _already_provisioned(self, conn: Any, action_request_urn: str) -> bool:
        # A request is fulfilled by either an object grant or a group membership;
        # both are keyed on the action-request URN.
        return dbx.is_already_provisioned(
            conn, action_request_urn, self.config.state
        ) or dbx.is_membership_provisioned(conn, action_request_urn, self.config.state)

    def _provision_membership(
        self, request: AccessRequest, requestor_email: str | None, group: str
    ) -> None:
        if not requestor_email:
            logger.error(
                f"[Provision] Request {request.urn} has no requestor email — "
                f"cannot add to group {group!r}; skipping"
            )
            return
        logger.info(
            f"[Provision] membership: add {requestor_email} to group {group} "
            f"for request {request.urn}"
        )
        conn = self._get_sql_conn()
        try:
            dbx.add_group_member(
                self._require_workspace_client(),
                group,
                requestor_email,
                dry_run=self.config.provisioning.dry_run,
            )
        except Exception as exc:
            logger.error(
                f"[Provision] Failed to add {requestor_email} to group {group} "
                f"for {request.urn}: {exc}",
                exc_info=True,
            )
            self._handle_membership_failure(conn, request, exc, requestor_email, group)
            return

        expires_at_ms: int | None = None
        if request.form_fields.access_duration_days:
            expires_at_ms = (
                int(time.time() * 1000) + request.form_fields.access_duration_days * 86_400_000
            )
        membership = DatabricksGroupMembershipRecord(
            action_request_urn=request.urn,
            user_email=requestor_email,
            group_name=group,
            added_at_ms=int(time.time() * 1000),
            expires_at_ms=expires_at_ms,
        )
        try:
            dbx.record_membership(conn, membership, self.config.state)
        except Exception as exc:
            logger.error(f"[Provision] Failed to record membership state for {request.urn}: {exc}")

        self._mirror_membership_add(group, requestor_email)

        try:
            send_dbx_membership_notification(
                self.config.smtp,
                request,
                recipient=requestor_email,
                member=requestor_email,
                group=group,
            )
        except Exception as exc:
            logger.error(f"[Provision] Failed to send membership email: {exc}")

    def _handle_membership_failure(
        self,
        conn: Any,
        request: AccessRequest,
        exc: Exception,
        recipient: str | None,
        group: str,
    ) -> None:
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
                send_dbx_provisioning_failure_notification(
                    self.config.smtp,
                    request,
                    str(exc),
                    recipient=recipient,
                    catalog=group,
                    schema="(group membership)",
                    table="",
                )
            except Exception as mail_exc:
                logger.error(
                    f"[Provision] Failed to send failure notification for {request.urn}: {mail_exc}"
                )

    def _open_ticket(
        self,
        ticketing: TicketingConfig,
        request: AccessRequest,
        principal: str,
        catalog: str,
        schema: str | None,
        table: str | None,
    ) -> TicketResult:
        target = f"{catalog}.{schema}.{table}" if table else f"{catalog}.{schema or '(all)'}"
        duration = (
            f"{request.form_fields.access_duration_days} days"
            if request.form_fields.access_duration_days
            else "Indefinite"
        )
        summary = f"DataHub access request: {target} for {principal}"
        description = (
            "Access requested via DataHub.\n\n"
            f"Grantee: {principal}\n"
            f"Unity Catalog target: {target}\n"
            f"DataHub resource: {request.resource}\n"
            f"Requested duration: {duration}\n"
            f"Justification: {request.form_fields.justification or '—'}\n"
            f"Action request: {request.urn}"
        )
        return create_access_ticket(ticketing, summary=summary, description=description)

    def _send_approval(
        self,
        request: AccessRequest,
        recipient: str | None,
        principal: str,
        catalog: str,
        schema: str,
        table: str,
        statements: list[str],
        ticket: TicketResult | None,
        replace: bool,
    ) -> None:
        try:
            if replace and ticket is not None:
                send_dbx_ticket_notification(
                    self.config.smtp,
                    request,
                    recipient=recipient,
                    principal=principal,
                    catalog=catalog,
                    schema=schema,
                    table=table,
                    ticket_key=ticket.key,
                    ticket_url=ticket.url,
                )
                return
            display = list(statements)
            if ticket is not None:
                ref = f"{ticket.key} ({ticket.url})" if ticket.url else ticket.key
                display = [f"Ticket opened: {ref}", *display]
            send_dbx_approval_notification(
                self.config.smtp,
                request,
                display,
                recipient=recipient,
                principal=principal,
                catalog=catalog,
                schema=schema,
                table=table,
            )
        except Exception as exc:
            logger.error(f"[Provision] Failed to send approval email: {exc}")

    def _record_invalid_target(self, request: AccessRequest) -> None:
        try:
            conn = self._get_sql_conn()
            dbx.record_provisioning_error(
                conn,
                request.urn,
                "INVALID_TARGET",
                f"Cannot derive a Databricks catalog.schema.table from entity {request.resource}",
                self.config.state,
            )
        except Exception as exc:
            logger.error(
                f"[Provision] Failed to record invalid-target state for {request.urn}: {exc}"
            )

    def _handle_provision_failure(
        self,
        conn: Any,
        request: AccessRequest,
        exc: Exception,
        recipient: str | None,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
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
                send_dbx_provisioning_failure_notification(
                    self.config.smtp,
                    request,
                    str(exc),
                    recipient=recipient,
                    catalog=catalog,
                    schema=schema,
                    table=table,
                )
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
        # The grant target comes from the dataset entity, so only the remaining
        # form fields (duration, justification, optional group) need mapping here.
        return {
            "field_access_duration_days": self.config.field_access_duration_days,
            "field_justification": self.config.field_justification,
            "field_databricks_group": self.config.field_databricks_group,
        }

    @staticmethod
    def _resolve_requestor_email(request: AccessRequest) -> str | None:
        """The requestor's corpuser email — the notification recipient, and the
        grantee when no group is requested."""
        return corpuser_email_from_urn(request.requestor_urn)

    @staticmethod
    def _resolve_grantee(request: AccessRequest, requestor_email: str | None) -> str | None:
        """The Databricks principal the grant is applied to: an explicitly requested
        group (group-based access) or, by default, the requestor's own identity."""
        return request.form_fields.databricks_group or requestor_email

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
        return self._require_workspace_client()

    def _require_workspace_client(self) -> Any:
        # Group-membership changes always go through the SDK (SCIM), regardless of
        # the SQL/SDK grant_method used for object grants.
        if self._workspace_client is None:
            self._workspace_client = self.config.databricks_connection.get_workspace_client()
        return self._workspace_client

    # ------------------------------------------------------------------
    # DataHub access mirror (audit) — read-only write-back, never mutates UC
    # ------------------------------------------------------------------

    def _sync(self) -> DatahubSync | None:
        if not self.config.datahub_sync.enabled:
            return None
        if self._datahub_sync is None:
            self._datahub_sync = DatahubSync(self.ctx.graph, self.config.datahub_sync)
        return self._datahub_sync

    def _mirror(self, action: Callable[[DatahubSync], None]) -> None:
        """Run a mirror operation, swallowing errors — the audit view must never break
        provisioning."""
        sync = self._sync()
        if sync is None:
            return
        try:
            action(sync)
        except Exception as exc:
            logger.error(f"[Sync] DataHub mirror failed (access unaffected): {exc}")

    def _mirror_group_grant(self, group: str, catalog: str, schema: str, table: str) -> None:
        self._mirror(lambda s: s.on_group_grant(group, catalog, schema, table))

    def _mirror_group_revoke(self, group: str, catalog: str, schema: str, table: str) -> None:
        self._mirror(lambda s: s.on_group_revoke(group, catalog, schema, table))

    def _mirror_membership_add(self, group: str, user_email: str) -> None:
        self._mirror(lambda s: s.on_membership_add(group, user_email))

    def _mirror_membership_remove(self, group: str, user_email: str) -> None:
        self._mirror(lambda s: s.on_membership_remove(group, user_email))

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
