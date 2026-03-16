"""Gmail SMTP email notification helpers."""

import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from action_access_provisioner.config import SmtpConfig
from action_access_provisioner.models import AccessRequest, GrantRecord

logger = logging.getLogger(__name__)


def _send(
    smtp_config: SmtpConfig,
    to_addresses: list[str],
    subject: str,
    html_body: str,
    cc_addresses: Optional[list[str]] = None,
) -> None:
    """Send an HTML email via Gmail SMTP."""
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


# ---------------------------------------------------------------------------
# Email templates
# ---------------------------------------------------------------------------


def send_approval_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
    sql_statements: list[str],
) -> None:
    """Notify the requestor that their access request has been approved and provisioned."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    subject = "✅ Your DataHub access request has been approved"

    db = request.form_fields.snowflake_database or "—"
    schema = request.form_fields.snowflake_schema or "(all schemas)"
    role = request.form_fields.snowflake_role or "—"
    duration = (
        f"{request.form_fields.access_duration_days} days"
        if request.form_fields.access_duration_days
        else "Indefinite"
    )
    note = request.note or ""
    resource = request.resource or "—"

    sql_block = (
        "\n".join(f"  {s}" for s in sql_statements)
        if sql_statements
        else "  (no statements executed)"
    )

    html = f"""
<html><body style="font-family: Arial, sans-serif; color: #333;">
  <h2 style="color: #28a745;">Access Request Approved</h2>
  <p>Your access request in DataHub has been <strong>approved</strong> and Snowflake access has been provisioned.</p>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 6px; font-weight: bold;">DataHub Resource</td><td style="padding: 6px;">{resource}</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Snowflake Database</td><td style="padding: 6px;">{db}</td></tr>
    <tr><td style="padding: 6px; font-weight: bold;">Snowflake Schema</td><td style="padding: 6px;">{schema}</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Role Granted</td><td style="padding: 6px;">{role}</td></tr>
    <tr><td style="padding: 6px; font-weight: bold;">Access Duration</td><td style="padding: 6px;">{duration}</td></tr>
    {"<tr style='background:#f9f9f9'><td style='padding:6px;font-weight:bold;'>Approver Note</td><td style='padding:6px;'>" + note + "</td></tr>" if note else ""}
  </table>
  <h3 style="margin-top: 24px;">SQL executed in Snowflake</h3>
  <pre style="background:#f4f4f4; padding: 12px; border-radius: 4px; font-size: 13px;">{sql_block}</pre>
  <p style="color:#888; font-size: 12px; margin-top: 24px;">This is an automated notification from DataHub Access Provisioner.</p>
</body></html>
"""
    _send(smtp_config, to, subject, html)


def send_denial_notification(
    smtp_config: SmtpConfig,
    request: AccessRequest,
) -> None:
    """Notify the requestor that their access request has been denied."""
    to = [request.form_fields.requestor_email] if request.form_fields.requestor_email else []
    subject = "❌ Your DataHub access request has been denied"

    resource = request.resource or "—"
    note = request.note or "No reason provided."

    html = f"""
<html><body style="font-family: Arial, sans-serif; color: #333;">
  <h2 style="color: #dc3545;">Access Request Denied</h2>
  <p>Your access request in DataHub for <strong>{resource}</strong> has been <strong>denied</strong>.</p>
  <p><strong>Reason:</strong> {note}</p>
  <p>If you believe this decision is incorrect, please contact your data governance team.</p>
  <p style="color:#888; font-size: 12px; margin-top: 24px;">This is an automated notification from DataHub Access Provisioner.</p>
</body></html>
"""
    _send(smtp_config, to, subject, html)


def send_sla_warning(
    smtp_config: SmtpConfig,
    action_request_urn: str,
    resource: Optional[str],
    pending_hours: float,
    assignee_emails: list[str],
    datahub_url: Optional[str] = None,
) -> None:
    """Remind approvers that a request has been pending longer than the SLA threshold."""
    subject = f"⚠️ Action required: access request pending for {pending_hours:.0f}h"
    resource_str = resource or action_request_urn
    link = f'<a href="{datahub_url}">{datahub_url}</a>' if datahub_url else action_request_urn

    html = f"""
<html><body style="font-family: Arial, sans-serif; color: #333;">
  <h2 style="color: #fd7e14;">SLA Warning — Pending Access Request</h2>
  <p>The following access request has been pending for <strong>{pending_hours:.0f} hours</strong> without a decision.</p>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 6px; font-weight: bold;">Resource</td><td style="padding: 6px;">{resource_str}</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Request URN</td><td style="padding: 6px;">{action_request_urn}</td></tr>
    <tr><td style="padding: 6px; font-weight: bold;">Pending for</td><td style="padding: 6px;">{pending_hours:.0f} hours</td></tr>
  </table>
  <p style="margin-top: 16px;">Please review and action this request: {link}</p>
  <p style="color:#888; font-size: 12px; margin-top: 24px;">This is an automated SLA reminder from DataHub Access Provisioner.</p>
</body></html>
"""
    _send(smtp_config, assignee_emails, subject, html)


def send_escalation_alert(
    smtp_config: SmtpConfig,
    action_request_urn: str,
    resource: Optional[str],
    pending_hours: float,
    assignee_emails: list[str],
    escalation_recipients: list[str],
    datahub_url: Optional[str] = None,
) -> None:
    """Send escalation email when SLA has been significantly breached."""
    subject = (
        f"🚨 Escalation: access request pending {pending_hours:.0f}h — immediate action required"
    )
    resource_str = resource or action_request_urn
    link = f'<a href="{datahub_url}">{datahub_url}</a>' if datahub_url else action_request_urn

    html = f"""
<html><body style="font-family: Arial, sans-serif; color: #333;">
  <h2 style="color: #dc3545;">SLA Escalation — Overdue Access Request</h2>
  <p>This is an escalation notice. The following access request has been pending for
  <strong>{pending_hours:.0f} hours</strong> and has exceeded the escalation threshold.</p>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 6px; font-weight: bold;">Resource</td><td style="padding: 6px;">{resource_str}</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Request URN</td><td style="padding: 6px;">{action_request_urn}</td></tr>
    <tr><td style="padding: 6px; font-weight: bold;">Pending for</td><td style="padding: 6px;">{pending_hours:.0f} hours</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Assigned approvers</td>
        <td style="padding: 6px;">{", ".join(assignee_emails) if assignee_emails else "—"}</td></tr>
  </table>
  <p style="margin-top: 16px;">Review the request immediately: {link}</p>
  <p style="color:#888; font-size: 12px; margin-top: 24px;">This is an automated escalation from DataHub Access Provisioner.</p>
</body></html>
"""
    _send(smtp_config, assignee_emails, subject, html, cc_addresses=escalation_recipients)


def send_revocation_notification(
    smtp_config: SmtpConfig,
    grant: GrantRecord,
) -> None:
    """Notify the original requestor that their access has been auto-revoked on expiry."""
    to = [grant.requestor_email] if grant.requestor_email else []
    subject = "🔒 Your Snowflake access has expired and been revoked"

    db = grant.snowflake_database
    schema = grant.snowflake_schema or "(all schemas)"
    role = grant.snowflake_role

    html = f"""
<html><body style="font-family: Arial, sans-serif; color: #333;">
  <h2 style="color: #6c757d;">Access Revoked — Expiry Reached</h2>
  <p>Your temporary Snowflake access has reached its expiry date and has been automatically revoked.</p>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 6px; font-weight: bold;">Snowflake Database</td><td style="padding: 6px;">{db}</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Schema</td><td style="padding: 6px;">{schema}</td></tr>
    <tr><td style="padding: 6px; font-weight: bold;">Role Revoked</td><td style="padding: 6px;">{role}</td></tr>
    <tr style="background:#f9f9f9"><td style="padding: 6px; font-weight: bold;">Original Request</td>
        <td style="padding: 6px;">{grant.action_request_urn}</td></tr>
  </table>
  <p>If you need continued access, please submit a new request via DataHub.</p>
  <p style="color:#888; font-size: 12px; margin-top: 24px;">This is an automated notification from DataHub Access Provisioner.</p>
</body></html>
"""
    _send(smtp_config, to, subject, html)
