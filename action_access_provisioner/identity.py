"""Resolve a DataHub requestor to a Databricks principal (a user email).

Unity Catalog grants target a principal (user/group/service principal); for an
individual requestor that principal is their email. DataHub usernames are not always
emails (SSO often uses opaque ids), so resolution tries, in order: an explicit
override map, the corpuser URN id when it is itself an email, then the email recorded
on the user's DataHub corpuser profile.
"""

import logging

from action_access_provisioner.config import DatabricksIdentityConfig
from action_access_provisioner.models import corpuser_email_from_urn

logger = logging.getLogger(__name__)

_CORPUSER_PREFIX = "urn:li:corpuser:"
_CORPGROUP_PREFIX = "urn:li:corpGroup:"


def _get_aspect(graph: object, entity_urn: str, aspect_type: type):
    """Call get_aspect on the graph or its wrapped inner graph; None on any failure."""
    target = graph
    if not hasattr(target, "get_aspect"):
        target = getattr(graph, "graph", None)
    if target is None or not hasattr(target, "get_aspect"):
        return None
    try:
        return target.get_aspect(entity_urn=entity_urn, aspect_type=aspect_type)
    except Exception as exc:
        logger.debug(
            f"[Identity] get_aspect({aspect_type.__name__}) failed for {entity_urn}: {exc}"
        )
        return None


def _email_from_datahub(graph: object, requestor_urn: str) -> str | None:
    """Look up the requestor's email from their DataHub corpuser profile."""
    from datahub.metadata.schema_classes import (
        CorpUserEditableInfoClass,
        CorpUserInfoClass,
    )

    info = _get_aspect(graph, requestor_urn, CorpUserInfoClass)
    if info is not None and getattr(info, "email", None):
        return str(info.email)
    editable = _get_aspect(graph, requestor_urn, CorpUserEditableInfoClass)
    if editable is not None and getattr(editable, "email", None):
        return str(editable.email)
    return None


def resolve_databricks_principal(
    graph: object,
    requestor_urn: str | None,
    config: DatabricksIdentityConfig,
) -> str | None:
    """Map a DataHub requestor URN to a Databricks principal (email), or None."""
    if not requestor_urn:
        return None

    overrides = config.principal_overrides
    if requestor_urn in overrides:
        return overrides[requestor_urn]
    urn_id = (
        requestor_urn[len(_CORPUSER_PREFIX) :]
        if requestor_urn.startswith(_CORPUSER_PREFIX)
        else requestor_urn
    )
    if urn_id in overrides:
        return overrides[urn_id]

    email = corpuser_email_from_urn(requestor_urn)
    if email:
        return email

    if config.resolve_email_from_datahub:
        email = _email_from_datahub(graph, requestor_urn)
        if email:
            return email

    logger.warning(
        f"[Identity] Could not resolve a Databricks principal for {requestor_urn!r}; "
        "the corpuser id is not an email, no override is configured, and no email is "
        "recorded on the DataHub profile."
    )
    return None


def _group_name_from_datahub(graph: object, group_urn: str) -> str | None:
    """The group's display name from its DataHub corpGroup profile."""
    from datahub.metadata.schema_classes import CorpGroupInfoClass

    info = _get_aspect(graph, group_urn, CorpGroupInfoClass)
    for attr in ("displayName", "name"):
        value = getattr(info, attr, None) if info is not None else None
        if value:
            return str(value)
    return None


def resolve_databricks_group(
    graph: object,
    value: str | None,
    config: DatabricksIdentityConfig,
) -> str | None:
    """Map a requested group to a Databricks group name.

    Workflow forms usually pick a group with a DataHub group picker, which yields a
    corpGroup URN rather than a name Unity Catalog would recognise. Anything that is
    not such a URN is passed through untouched, so forms that already collect the
    Databricks group name keep working.
    """
    if not value:
        return None
    if not value.startswith(_CORPGROUP_PREFIX):
        return value

    urn_id = value[len(_CORPGROUP_PREFIX) :]
    overrides = config.group_overrides
    for key in (value, urn_id):
        if key in overrides:
            return overrides[key]

    if config.resolve_group_name_from_datahub:
        name = _group_name_from_datahub(graph, value)
        if name:
            return name

    # The URN id is the group's DataHub id, which for IdP-provisioned groups is
    # usually the same string the IdP pushed into Databricks.
    logger.info(
        f"[Identity] No display name resolved for {value!r}; falling back to the "
        f"group id {urn_id!r} as the Databricks group name."
    )
    return urn_id
