from datahub.metadata.schema_classes import (
    AccessClass,
    ActorsClass,
    RoleAssociationClass,
    RoleGroupClass,
    RolePropertiesClass,
    RoleUserClass,
)

from action_access_provisioner.config import DatahubSyncConfig
from action_access_provisioner.datahub_sync import DatahubSync

_ROLE_URN = "urn:li:role:databricks.analytics"
_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:databricks,prod.sales.orders,PROD)"
_GROUP_URN = "urn:li:corpGroup:analytics"
_USER_URN = "urn:li:corpuser:alice@example.com"


class FakeGraph:
    """Minimal DataHubGraph stand-in: serves preset aspects, records emitted MCPs."""

    def __init__(self, aspects=None):
        self.aspects = aspects or {}
        self.emitted = []

    def get_aspect(self, urn, aspect_type):
        return self.aspects.get((urn, aspect_type))

    def emit_mcp(self, mcp):
        self.emitted.append(mcp)


def _sync(graph):
    return DatahubSync(graph, DatahubSyncConfig(enabled=True))


def _emitted(graph, aspect_type):
    return [m.aspect for m in graph.emitted if isinstance(m.aspect, aspect_type)]


def test_resolve_graph_unwraps_acryl_wrapper():
    inner = FakeGraph()
    wrapper = type("AcrylDataHubGraph", (), {"graph": inner})()
    assert DatahubSync(wrapper, DatahubSyncConfig(enabled=True))._graph is inner


def test_group_grant_mints_role_actors_and_dataset_access():
    graph = FakeGraph()
    _sync(graph).on_group_grant("analytics", "prod", "sales", "orders")

    props = _emitted(graph, RolePropertiesClass)
    assert len(props) == 1 and props[0].name == "analytics"

    actors = _emitted(graph, ActorsClass)[-1]
    assert [g.group for g in actors.groups] == [_GROUP_URN]

    access = _emitted(graph, AccessClass)[-1]
    assert [r.urn for r in access.roles] == [_ROLE_URN]
    # the dataset URN must match how datasets are ingested (lowercased, no platform_instance)
    assert graph.emitted[-1].entityUrn == _DATASET_URN


def test_group_grant_is_idempotent_when_already_mirrored():
    graph = FakeGraph(
        {
            (_ROLE_URN, RolePropertiesClass): RolePropertiesClass(name="analytics", type="READ"),
            (_ROLE_URN, ActorsClass): ActorsClass(groups=[RoleGroupClass(group=_GROUP_URN)]),
            (_DATASET_URN, AccessClass): AccessClass(roles=[RoleAssociationClass(urn=_ROLE_URN)]),
        }
    )
    _sync(graph).on_group_grant("analytics", "prod", "sales", "orders")

    # Role props already present and association already there → nothing to re-emit.
    assert _emitted(graph, RolePropertiesClass) == []
    assert _emitted(graph, AccessClass) == []


def test_membership_add_records_user_on_role_actors():
    graph = FakeGraph()
    _sync(graph).on_membership_add("analytics", "alice@example.com")

    actors = _emitted(graph, ActorsClass)[-1]
    assert [u.user for u in actors.users] == [_USER_URN]
    assert [g.group for g in actors.groups] == [_GROUP_URN]


def test_membership_remove_drops_user_but_keeps_others():
    graph = FakeGraph(
        {
            (_ROLE_URN, ActorsClass): ActorsClass(
                users=[
                    RoleUserClass(user=_USER_URN),
                    RoleUserClass(user="urn:li:corpuser:bob@example.com"),
                ],
                groups=[RoleGroupClass(group=_GROUP_URN)],
            )
        }
    )
    _sync(graph).on_membership_remove("analytics", "alice@example.com")

    actors = _emitted(graph, ActorsClass)[-1]
    assert [u.user for u in actors.users] == ["urn:li:corpuser:bob@example.com"]


def test_group_revoke_removes_association_without_deleting_role():
    graph = FakeGraph(
        {(_DATASET_URN, AccessClass): AccessClass(roles=[RoleAssociationClass(urn=_ROLE_URN)])}
    )
    _sync(graph).on_group_revoke("analytics", "prod", "sales", "orders")

    access = _emitted(graph, AccessClass)[-1]
    assert access.roles == []
    # The role itself is left intact — other datasets may still reference it.
    assert _emitted(graph, RolePropertiesClass) == []


_USER_ROLE_URN = "urn:li:role:databricks.user.alice@example.com"


def test_user_grant_mints_per_user_role_and_dataset_access():
    graph = FakeGraph()
    _sync(graph).on_user_grant("alice@example.com", "prod", "sales", "orders")

    props = _emitted(graph, RolePropertiesClass)
    assert len(props) == 1 and props[0].name == "alice@example.com"

    actors = _emitted(graph, ActorsClass)[-1]
    assert [u.user for u in actors.users] == [_USER_URN]

    access = _emitted(graph, AccessClass)[-1]
    assert [r.urn for r in access.roles] == [_USER_ROLE_URN]
    assert graph.emitted[-1].entityUrn == _DATASET_URN


def test_user_revoke_removes_association_without_deleting_role():
    graph = FakeGraph(
        {(_DATASET_URN, AccessClass): AccessClass(roles=[RoleAssociationClass(urn=_USER_ROLE_URN)])}
    )
    _sync(graph).on_user_revoke("alice@example.com", "prod", "sales", "orders")

    access = _emitted(graph, AccessClass)[-1]
    assert access.roles == []
    assert _emitted(graph, RolePropertiesClass) == []
