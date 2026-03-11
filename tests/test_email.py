"""Unit tests for email notification helpers."""

import time
from unittest.mock import MagicMock, patch

import pytest

from action_access_provisioner.config import SmtpConfig
from action_access_provisioner.email import (
    send_approval_notification,
    send_denial_notification,
    send_escalation_alert,
    send_revocation_notification,
    send_sla_warning,
)
from action_access_provisioner.models import AccessRequest, FormFieldValues, GrantRecord


@pytest.fixture
def smtp_config():
    return SmtpConfig(username="sender@gmail.com", password="app-password")


@pytest.fixture
def approved_request():
    return AccessRequest(
        urn="urn:li:actionRequest:001",
        status="COMPLETED",
        result="APPROVED",
        note="Approved by Alice",
        request_type="WORKFLOW_FORM_REQUEST",
        resource="urn:li:dataset:(urn:li:dataPlatform:snowflake,PROD.SALES.ORDERS,PROD)",
        requestor_urn="urn:li:corpuser:bob",
        created_ms=int(time.time() * 1000),
        due_date_ms=None,
        form_fields=FormFieldValues(
            snowflake_database="PROD",
            snowflake_schema="SALES",
            snowflake_role="ANALYST",
            access_duration_days=14,
            requestor_email="bob@example.com",
        ),
    )


def test_send_approval_notification_calls_smtp(smtp_config, approved_request):
    with patch("action_access_provisioner.email.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server

        send_approval_notification(
            smtp_config,
            approved_request,
            ["GRANT USAGE ON DATABASE PROD TO ROLE ANALYST"],
        )

        mock_server.sendmail.assert_called_once()
        args = mock_server.sendmail.call_args
        assert "bob@example.com" in args[0][1]


def test_send_approval_notification_no_email_skips(smtp_config, approved_request):
    approved_request.form_fields.requestor_email = None
    with patch("action_access_provisioner.email.smtplib.SMTP") as mock_smtp_cls:
        send_approval_notification(smtp_config, approved_request, [])
        mock_smtp_cls.assert_not_called()


def test_send_denial_notification_sends_email(smtp_config):
    denied = AccessRequest(
        urn="urn:li:actionRequest:002",
        status="COMPLETED",
        result="DENIED",
        note="Not permitted",
        request_type="WORKFLOW_FORM_REQUEST",
        resource=None,
        requestor_urn=None,
        created_ms=None,
        due_date_ms=None,
        form_fields=FormFieldValues(requestor_email="carol@example.com"),
    )
    with patch("action_access_provisioner.email.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server
        send_denial_notification(smtp_config, denied)
        mock_server.sendmail.assert_called_once()


def test_send_sla_warning(smtp_config):
    with patch("action_access_provisioner.email.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server
        send_sla_warning(
            smtp_config,
            action_request_urn="urn:li:actionRequest:003",
            resource="urn:li:dataset:foo",
            pending_hours=26.5,
            assignee_emails=["approver@example.com"],
        )
        mock_server.sendmail.assert_called_once()


def test_send_escalation_alert_ccs_escalation_recipients(smtp_config):
    with patch("action_access_provisioner.email.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server
        send_escalation_alert(
            smtp_config,
            action_request_urn="urn:li:actionRequest:004",
            resource=None,
            pending_hours=80,
            assignee_emails=["approver@example.com"],
            escalation_recipients=["lead@example.com"],
        )
        call_args = mock_server.sendmail.call_args
        all_recipients = call_args[0][1]
        assert "lead@example.com" in all_recipients


def test_send_revocation_notification(smtp_config):
    grant = GrantRecord(
        action_request_urn="urn:li:actionRequest:005",
        snowflake_role="OLD_ROLE",
        snowflake_database="PROD",
        snowflake_schema="ANALYTICS",
        requestor_email="dave@example.com",
        granted_at_ms=int(time.time() * 1000) - 86_400_000,
        expires_at_ms=int(time.time() * 1000) - 1000,
    )
    with patch("action_access_provisioner.email.smtplib.SMTP") as mock_smtp_cls:
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_server
        send_revocation_notification(smtp_config, grant)
        mock_server.sendmail.assert_called_once()
