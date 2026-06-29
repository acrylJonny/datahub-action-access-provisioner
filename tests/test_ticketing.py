from unittest.mock import patch

import pytest

from action_access_provisioner.config import TicketingConfig
from action_access_provisioner.ticketing import create_access_ticket

_TICKET_MODULE = "action_access_provisioner.ticketing"


def _jira_config(**overrides) -> TicketingConfig:
    base = {
        "provider": "jira",
        "base_url": "https://acme.atlassian.net/",
        "username": "svc@acme.io",
        "api_token": "tok",
        "jira_project_key": "ACCESS",
    }
    base.update(overrides)
    return TicketingConfig.model_validate(base)


def _servicenow_config(**overrides) -> TicketingConfig:
    base = {
        "provider": "servicenow",
        "base_url": "https://acme.service-now.com",
        "username": "svc",
        "api_token": "pw",
    }
    base.update(overrides)
    return TicketingConfig.model_validate(base)


def test_jira_requires_project_key():
    with pytest.raises(ValueError):
        TicketingConfig.model_validate(
            {
                "provider": "jira",
                "base_url": "https://acme.atlassian.net",
                "username": "svc@acme.io",
                "api_token": "tok",
            }
        )


def test_dry_run_skips_http():
    config = _jira_config(dry_run=True)
    with patch(f"{_TICKET_MODULE}._http_post") as mock_post:
        result = create_access_ticket(config, summary="s", description="d")
    mock_post.assert_not_called()
    assert result.key == "DRY-RUN"


def test_jira_create_returns_key_and_url():
    config = _jira_config()
    with patch(f"{_TICKET_MODULE}._http_post", return_value={"key": "ACCESS-42"}) as mock_post:
        result = create_access_ticket(config, summary="grant orders", description="body")

    url, kwargs = mock_post.call_args[0][0], mock_post.call_args.kwargs
    assert url == "https://acme.atlassian.net/rest/api/2/issue"
    assert kwargs["json_body"]["fields"]["project"]["key"] == "ACCESS"
    assert result.key == "ACCESS-42"
    assert result.url == "https://acme.atlassian.net/browse/ACCESS-42"


def test_servicenow_create_returns_number_and_url():
    config = _servicenow_config(servicenow_table="sc_request")
    payload = {"result": {"number": "REQ0001", "sys_id": "abc123"}}
    with patch(f"{_TICKET_MODULE}._http_post", return_value=payload) as mock_post:
        result = create_access_ticket(config, summary="grant orders", description="body")

    url = mock_post.call_args[0][0]
    assert url == "https://acme.service-now.com/api/now/table/sc_request"
    assert result.key == "REQ0001"
    assert "sys_id=abc123" in (result.url or "")
