import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from importlib import resources

from action_access_provisioner.config import SmtpConfig
from action_access_provisioner.models import AccessRequest, DatabricksGrantRecord, GrantRecord

logger = logging.getLogger(__name__)

_DEFAULT_FOOTER = "This is an automated notification from DataHub Access Provisioner."


def _send(
    smtp_config: SmtpConfig,
    to_addresses: list[str],
    subject: str,
    html_body: str,
    cc_addresses: list[str] | None = None,
) -> None:
    """Send an HTML email via SMTP."""
    if not to_addresses:
        logger.warning(f"No recipients provided for email subject='{subject}' — skipping")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_config.get_from_address()
    msg["To"] = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)

    msg.attach(MIMEText(html_body, "html"))

    all_recipients = to_addresses + (cc_addresses or [])

    try:
        if smtp_config.use_tls:
            with smtplib.SMTP(smtp_config.host, smtp_config.port) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                server.login(smtp_config.username, smtp_config.password)
                server.sendmail(smtp_config.get_from_address(), all_recipients, msg.as_string())
        else:
            # Implicit SSL (port 465)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_config.host, smtp_config.port, context=context) as server:
                server.login(smtp_config.username, smtp_config.password)
                server.sendmail(smtp_config.get_from_address(), all_recipients, msg.as_string())

        logger.info(f"Email sent to {all_recipients}: {subject}")
    except Exception as exc:
        logger.error(f"Failed to send email to {all_recipients}: {exc}", exc_info=True)
        raise


def _load_template(name: str) -> str:
    return (
        resources.files("action_access_provisioner")
        .joinpath("templates")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )


def _render(
    body_template: str,
    *,
    heading_color: str,
    heading: str,
    footer: str = _DEFAULT_FOOTER,
    **body_vars: str,
) -> str:
    """Render a body fragment into the shared base layout.

    Substituted values are passed as ``str.format`` args, so braces inside the
    values (e.g. an error message) are never re-interpreted — only the static
    template literals are.
    """
    body = _load_template(body_template).format(**body_vars)
    return _load_template("base.html").format(
        heading_color=heading_color, heading=heading, body=body, footer=footer
    )


def send_approval_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
    sql_statements: list[str],
) -> None:
    """Notify the requestor that their access request has been approved and provisioned."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    note = request.note or ""
    note_row = (
        "<tr style='background:#f9f9f9'><td style='padding:6px;font-weight:bold;'>Approver Note"
        f"</td><td style='padding:6px;'>{note}</td></tr>"
        if note
        else ""
    )
    html = _render(
        "approval.html",
        heading_color="#28a745",
        heading="Access Request Approved",
        resource=request.resource or "—",
        database=request.form_fields.snowflake_database or "—",
        schema=request.form_fields.snowflake_schema or "(all schemas)",
        role=request.form_fields.snowflake_role or "—",
        duration=(
            f"{request.form_fields.access_duration_days} days"
            if request.form_fields.access_duration_days
            else "Indefinite"
        ),
        note_row=note_row,
        sql_block=_sql_block(sql_statements),
    )
    _send(smtp_config, to, "✅ Your DataHub access request has been approved", html)


def send_denial_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
) -> None:
    """Notify the requestor that their access request has been denied."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    html = _render(
        "denial.html",
        heading_color="#dc3545",
        heading="Access Request Denied",
        resource=request.resource or "—",
        note=request.note or "No reason provided.",
    )
    _send(smtp_config, to, "❌ Your DataHub access request has been denied", html)


def send_sla_warning(
    smtp_config: SmtpConfig,
    action_request_urn: str,
    resource: str | None,
    pending_hours: float,
    assignee_emails: list[str],
    datahub_url: str | None = None,
) -> None:
    """Remind approvers that a request has been pending longer than the SLA threshold."""
    html = _render(
        "sla_warning.html",
        heading_color="#fd7e14",
        heading="SLA Warning — Pending Access Request",
        footer="This is an automated SLA reminder from DataHub Access Provisioner.",
        pending_hours=f"{pending_hours:.0f}",
        resource=resource or action_request_urn,
        urn=action_request_urn,
        link=_request_link(action_request_urn, datahub_url),
    )
    subject = f"⚠️ Action required: access request pending for {pending_hours:.0f}h"
    _send(smtp_config, assignee_emails, subject, html)


def send_escalation_alert(
    smtp_config: SmtpConfig,
    action_request_urn: str,
    resource: str | None,
    pending_hours: float,
    assignee_emails: list[str],
    escalation_recipients: list[str],
    datahub_url: str | None = None,
) -> None:
    """Send escalation email when SLA has been significantly breached."""
    html = _render(
        "escalation.html",
        heading_color="#dc3545",
        heading="SLA Escalation — Overdue Access Request",
        footer="This is an automated escalation from DataHub Access Provisioner.",
        pending_hours=f"{pending_hours:.0f}",
        resource=resource or action_request_urn,
        urn=action_request_urn,
        assignees=", ".join(assignee_emails) if assignee_emails else "—",
        link=_request_link(action_request_urn, datahub_url),
    )
    subject = (
        f"🚨 Escalation: access request pending {pending_hours:.0f}h — immediate action required"
    )
    _send(smtp_config, assignee_emails, subject, html, cc_addresses=escalation_recipients)


def send_provisioning_failure_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
    error_message: str,
) -> None:
    """Notify the requestor (and approver via CC) that provisioning failed permanently."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    if not to:
        logger.warning(
            f"[Email] No requestor email for {request.urn} — skipping failure notification"
        )
        return

    html = _render(
        "provisioning_failure.html",
        heading_color="#fd7e14",
        heading="Access Provisioning Failed",
        resource=request.resource or "—",
        database=request.form_fields.snowflake_database or "—",
        schema=request.form_fields.snowflake_schema or "(all schemas)",
        error_message=error_message,
        urn=request.urn,
    )
    _send(smtp_config, to, "⚠️ DataHub access request could not be provisioned", html)


def send_revocation_notification(
    smtp_config: SmtpConfig,
    grant: GrantRecord,
) -> None:
    """Notify the original requestor that their access has been auto-revoked on expiry."""
    to = [grant.requestor_email] if grant.requestor_email else []
    html = _render(
        "revocation.html",
        heading_color="#6c757d",
        heading="Access Revoked — Expiry Reached",
        database=grant.snowflake_database,
        schema=grant.snowflake_schema or "(all schemas)",
        role=grant.snowflake_role,
        urn=grant.action_request_urn,
    )
    _send(smtp_config, to, "🔒 Your Snowflake access has expired and been revoked", html)


def _dbx_target_label(catalog: str | None, schema: str | None, table: str | None) -> str:
    """Render a Unity Catalog target as 'catalog.schema.table' with sensible fallbacks."""
    cat = catalog or "—"
    if table:
        return f"{cat}.{schema}.{table}"
    return f"{cat}.{schema or '(all schemas)'}"


def send_dbx_approval_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
    sql_statements: list[str],
) -> None:
    """Notify the requestor that their Databricks access has been provisioned."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    ff = request.form_fields
    note = request.note or ""
    note_row = (
        "<tr><td style='padding:6px;font-weight:bold;'>Approver Note"
        f"</td><td style='padding:6px;'>{note}</td></tr>"
        if note
        else ""
    )
    html = _render(
        "dbx_approval.html",
        heading_color="#28a745",
        heading="Access Request Approved",
        resource=request.resource or "—",
        target=_dbx_target_label(ff.databricks_catalog, ff.databricks_schema, ff.databricks_table),
        granted_to=ff.requestor_email or "—",
        duration=f"{ff.access_duration_days} days" if ff.access_duration_days else "Indefinite",
        note_row=note_row,
        sql_block=_sql_block(sql_statements),
    )
    _send(smtp_config, to, "✅ Your DataHub access request has been approved", html)


def send_dbx_provisioning_failure_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
    error_message: str,
) -> None:
    """Notify the requestor that Databricks provisioning failed permanently."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    if not to:
        logger.warning(
            f"[Email] No requestor email for {request.urn} — skipping failure notification"
        )
        return

    ff = request.form_fields
    html = _render(
        "dbx_provisioning_failure.html",
        heading_color="#fd7e14",
        heading="Access Provisioning Failed",
        resource=request.resource or "—",
        target=_dbx_target_label(ff.databricks_catalog, ff.databricks_schema, ff.databricks_table),
        error_message=error_message,
        urn=request.urn,
    )
    _send(smtp_config, to, "⚠️ DataHub access request could not be provisioned", html)


def send_dbx_revocation_notification(
    smtp_config: SmtpConfig,
    grant: DatabricksGrantRecord,
) -> None:
    """Notify the original requestor that their Databricks access was auto-revoked."""
    to = [grant.requestor_email] if grant.requestor_email else []
    html = _render(
        "dbx_revocation.html",
        heading_color="#6c757d",
        heading="Access Revoked — Expiry Reached",
        target=_dbx_target_label(grant.catalog, grant.schema, grant.table),
        principal=grant.principal,
        urn=grant.action_request_urn,
    )
    _send(smtp_config, to, "🔒 Your Databricks access has expired and been revoked", html)


def _sql_block(statements: list[str]) -> str:
    if not statements:
        return "  (no statements executed)"
    return "\n".join(f"  {s}" for s in statements)


def _request_link(action_request_urn: str, datahub_url: str | None) -> str:
    if datahub_url:
        return f'<a href="{datahub_url}">{datahub_url}</a>'
    return action_request_urn
