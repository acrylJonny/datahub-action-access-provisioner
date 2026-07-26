from datahub.metadata.schema_classes import CorpUserEditableInfoClass, CorpUserInfoClass

from action_access_provisioner.config import DatabricksIdentityConfig
from action_access_provisioner.identity import resolve_databricks_principal


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
