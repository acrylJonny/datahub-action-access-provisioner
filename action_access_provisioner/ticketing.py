import logging
from typing import Any

from pydantic import BaseModel

from action_access_provisioner.config import TicketingConfig, TicketProvider

logger = logging.getLogger(__name__)


class TicketResult(BaseModel):
    """The outcome of opening an access ticket."""

    key: str
    url: str | None = None


def _http_post(
    url: str,
    *,
    json_body: dict[str, Any],
    auth: tuple[str, str],
    timeout: int = 30,
) -> dict[str, Any]:
    # Lazy import: requests is provided by the DataHub executor runtime, so it is
    # not a declared dependency of this package.
    import requests

    resp = requests.post(
        url,
        json=json_body,
        auth=auth,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _create_jira(config: TicketingConfig, summary: str, description: str) -> TicketResult:
    payload = {
        "fields": {
            "project": {"key": config.jira_project_key},
            "summary": summary,
            "description": description,
            "issuetype": {"name": config.jira_issue_type},
        }
    }
    data = _http_post(
        f"{config.base_url_clean}/rest/api/2/issue",
        json_body=payload,
        auth=(config.username, config.api_token),
    )
    key = data.get("key", "")
    url = f"{config.base_url_clean}/browse/{key}" if key else None
    return TicketResult(key=key, url=url)


def _create_servicenow(config: TicketingConfig, summary: str, description: str) -> TicketResult:
    data = _http_post(
        f"{config.base_url_clean}/api/now/table/{config.servicenow_table}",
        json_body={"short_description": summary, "description": description},
        auth=(config.username, config.api_token),
    )
    # ServiceNow wraps the created record in a "result" object.
    result = data.get("result", {}) if isinstance(data, dict) else {}
    number = result.get("number", "")
    sys_id = result.get("sys_id", "")
    url = (
        f"{config.base_url_clean}/nav_to.do?uri={config.servicenow_table}.do?sys_id={sys_id}"
        if sys_id
        else None
    )
    return TicketResult(key=number or sys_id, url=url)


def create_access_ticket(
    config: TicketingConfig,
    *,
    summary: str,
    description: str,
) -> TicketResult:
    """Open an access ticket in the configured provider and return its identifier."""
    if config.dry_run:
        logger.info(f"[DRY RUN] Would open {config.provider} ticket: {summary}")
        return TicketResult(key="DRY-RUN", url=None)

    if config.provider == TicketProvider.JIRA:
        return _create_jira(config, summary, description)
    return _create_servicenow(config, summary, description)
