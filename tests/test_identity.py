from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    CorpUserEditableInfoClass,
    CorpUserInfoClass,
)

from action_access_provisioner.config import DatabricksIdentityConfig
from action_access_provisioner.identity import (
    resolve_databricks_group,
    resolve_databricks_principal,
)


class FakeGraph:
    """Serves preset aspects keyed on (entity_urn, aspect_type)."""

    def __init__(self, aspects=None):
        self.aspects = aspects or {}
        self.calls = 0

    def get_aspect(self, entity_urn, aspect_type):
        self.calls += 1
        return self.aspects.get((entity_urn, aspect_type))


def test_override_by_full_urn_wins():
    cfg = DatabricksIdentityConfig(
        principal_overrides={"urn:li:corpuser:jsmith": "jane.smith@corp.com"}
    )
    graph = FakeGraph()
    assert (
        resolve_databricks_principal(graph, "urn:li:corpuser:jsmith", cfg) == "jane.smith@corp.com"
    )
    # Override short-circuits before any graph lookup.
    assert graph.calls == 0


def test_override_by_bare_id():
    cfg = DatabricksIdentityConfig(principal_overrides={"jsmith": "jane.smith@corp.com"})
    assert (
        resolve_databricks_principal(FakeGraph(), "urn:li:corpuser:jsmith", cfg)
        == "jane.smith@corp.com"
    )


def test_email_form_urn_used_directly():
    graph = FakeGraph()
    result = resolve_databricks_principal(
        graph, "urn:li:corpuser:jane@corp.com", DatabricksIdentityConfig()
    )
    assert result == "jane@corp.com"
    assert graph.calls == 0


def test_email_looked_up_from_datahub_profile():
    urn = "urn:li:corpuser:jsmith"
    graph = FakeGraph(
        {(urn, CorpUserInfoClass): CorpUserInfoClass(active=True, email="jane.smith@corp.com")}
    )
    assert (
        resolve_databricks_principal(graph, urn, DatabricksIdentityConfig())
        == "jane.smith@corp.com"
    )


def test_email_falls_back_to_editable_info():
    urn = "urn:li:corpuser:jsmith"
    graph = FakeGraph(
        {
            (urn, CorpUserInfoClass): CorpUserInfoClass(active=True, email=None),
            (urn, CorpUserEditableInfoClass): CorpUserEditableInfoClass(email="jane@corp.com"),
        }
    )
    assert resolve_databricks_principal(graph, urn, DatabricksIdentityConfig()) == "jane@corp.com"


def test_returns_none_when_unresolvable_and_lookup_disabled():
    cfg = DatabricksIdentityConfig(resolve_email_from_datahub=False)
    graph = FakeGraph()
    assert resolve_databricks_principal(graph, "urn:li:corpuser:jsmith", cfg) is None
    assert graph.calls == 0


def test_group_plain_name_passes_through():
    """Forms that already collect the Databricks group name must keep working."""
    graph = FakeGraph()
    cfg = DatabricksIdentityConfig()
    assert resolve_databricks_group(graph, "analytics_team", cfg) == "analytics_team"
    assert graph.calls == 0


def test_group_urn_resolves_to_display_name():
    urn = "urn:li:corpGroup:analytics-team"
    graph = FakeGraph(
        {
            (urn, CorpGroupInfoClass): CorpGroupInfoClass(
                admins=[], members=[], groups=[], displayName="analytics_team"
            )
        }
    )
    assert resolve_databricks_group(graph, urn, DatabricksIdentityConfig()) == "analytics_team"


def test_group_override_wins_over_lookup():
    urn = "urn:li:corpGroup:analytics-team"
    graph = FakeGraph(
        {
            (urn, CorpGroupInfoClass): CorpGroupInfoClass(
                admins=[], members=[], groups=[], displayName="wrong"
            )
        }
    )
    cfg = DatabricksIdentityConfig(group_overrides={"analytics-team": "uc_analytics"})
    assert resolve_databricks_group(graph, urn, cfg) == "uc_analytics"
    assert graph.calls == 0


def test_group_urn_falls_back_to_group_id():
    """An unresolvable profile still yields a usable name rather than a URN."""
    urn = "urn:li:corpGroup:analytics-team"
    assert (
        resolve_databricks_group(FakeGraph(), urn, DatabricksIdentityConfig()) == "analytics-team"
    )
