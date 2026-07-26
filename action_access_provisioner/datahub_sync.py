"""Mirror Databricks access changes into DataHub for auditing.

When enabled (``datahub_sync.enabled``), the action mirrors every group grant and
group membership it makes into DataHub using the native ``role`` entity:

* a Databricks group ``analytics`` becomes ``urn:li:role:databricks.analytics``;
* the corpGroup is recorded on the role's ``actors`` so "who has access" is queryable;
* individual members added via membership mode are recorded on ``actors.users``;
* a group grant on ``catalog.schema.table`` adds a ``RoleAssociation`` to that
  dataset's ``access`` aspect, so the dataset shows which roles can reach it.

This is a strictly **read-only mirror** of Unity Catalog: it only ever writes DataHub
metadata and never calls Databricks. Drift (out-of-band UC changes) is out of scope
for this write-back path and is the job of a future reconciliation crawl.

ponytail: list aspects (``access``, ``actors``) are updated read-modify-write against
the live graph, which is eventually consistent. Two writes racing the same aspect can
clobber each other; the reconciliation crawl is the upgrade path that re-asserts truth.
"""

import logging
from typing import Protocol, TypeVar, runtime_checkable

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_group_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AccessClass,
    ActorsClass,
    RoleAssociationClass,
    RoleGroupClass,
    RolePropertiesClass,
    RoleUserClass,
    _Aspect,
)

from action_access_provisioner.config import DatahubSyncConfig

logger = logging.getLogger(__name__)

_ROLE_TYPE_READ = "READ"

_AspectT = TypeVar("_AspectT", bound=_Aspect)


@runtime_checkable
class _MetadataGraph(Protocol):
    """The slice of ``DataHubGraph`` the mirror relies on.

    Declared structurally so the real ``DataHubGraph``, the ``AcrylDataHubGraph``
    wrapper, and test doubles all satisfy it without inheritance.
    """

    def get_aspect(self, entity_urn: str, aspect_type: type[_AspectT]) -> _AspectT | None: ...

    def emit_mcp(self, mcp: MetadataChangeProposalWrapper) -> None: ...


def _resolve_graph(graph: object) -> _MetadataGraph:
    """Unwrap the action's graph handle to one exposing ``get_aspect``/``emit_mcp``.

    The action receives either a ``DataHubGraph`` directly or an ``AcrylDataHubGraph``
    that wraps one in ``.graph`` — mirror the pattern used by
    ``graphql._execute_graphql``.
    """
    if isinstance(graph, _MetadataGraph):
        return graph
    inner = getattr(graph, "graph", None)
    if isinstance(inner, _MetadataGraph):
        return inner
    raise TypeError(
        f"Graph object {type(graph)} exposes no get_aspect/emit_mcp — cannot mirror access to DataHub."
    )


class DatahubSync:
    """Write granted Databricks access into DataHub as role/actors/access aspects."""

    def __init__(self, graph: object, config: DatahubSyncConfig) -> None:
        self._graph = _resolve_graph(graph)
        self.config = config

    # -- URN builders ----------------------------------------------------

    def _role_urn(self, group: str) -> str:
        return f"urn:li:role:{self.config.role_urn_prefix}.{group}"

    def _user_role_urn(self, user_email: str) -> str:
        # Individual grants are modelled as a single-actor role so the dataset's access
        # aspect (which associates roles, not raw users) can point at exactly one user.
        return f"urn:li:role:{self.config.role_urn_prefix}.user.{user_email}"

    def _dataset_urn(self, catalog: str, schema: str, table: str) -> str:
        # catalog/schema/table come straight off the dataset URN the request was raised
        # on, so they already match DataHub's stored (lowercased) identifiers.
        return make_dataset_urn(
            platform=self.config.platform,
            name=f"{catalog}.{schema}.{table}",
            env=self.config.env,
        )

    # -- public hooks ----------------------------------------------------

    def on_group_grant(self, group: str, catalog: str, schema: str, table: str) -> None:
        """A group was GRANTed access to a table — record the role and dataset link."""
        self._ensure_role(group)
        self._modify_access(
            self._dataset_urn(catalog, schema, table), self._role_urn(group), add=True
        )

    def on_group_revoke(self, group: str, catalog: str, schema: str, table: str) -> None:
        """A group grant expired — drop the dataset→role association (keep the role)."""
        self._modify_access(
            self._dataset_urn(catalog, schema, table), self._role_urn(group), add=False
        )

    def on_membership_add(self, group: str, user_email: str) -> None:
        """A user was added to a group — record them on the role's actors."""
        role_urn = self._role_urn(group)
        self._ensure_role_properties(group)
        self._upsert_actors(
            role_urn, add_groups=[make_group_urn(group)], add_users=[make_user_urn(user_email)]
        )

    def on_membership_remove(self, group: str, user_email: str) -> None:
        """A user's membership expired — remove them from the role's actors."""
        self._upsert_actors(self._role_urn(group), remove_users=[make_user_urn(user_email)])

    def on_user_grant(self, user_email: str, catalog: str, schema: str, table: str) -> None:
        """An individual user was GRANTed access to a table — record a per-user role
        (with the user as its sole actor) and link it to the dataset."""
        self._ensure_user_role(user_email)
        self._modify_access(
            self._dataset_urn(catalog, schema, table), self._user_role_urn(user_email), add=True
        )

    def on_user_revoke(self, user_email: str, catalog: str, schema: str, table: str) -> None:
        """An individual user's grant expired — drop the dataset→role association."""
        self._modify_access(
            self._dataset_urn(catalog, schema, table), self._user_role_urn(user_email), add=False
        )

    # -- internals -------------------------------------------------------

    def _emit(self, entity_urn: str, aspect: _Aspect) -> None:
        self._graph.emit_mcp(MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect))

    def _ensure_role(self, group: str) -> None:
        self._ensure_role_properties(group)
        # Represent the Databricks group itself as an actor group on the role.
        self._upsert_actors(self._role_urn(group), add_groups=[make_group_urn(group)])

    def _ensure_user_role(self, user_email: str) -> None:
        role_urn = self._user_role_urn(user_email)
        if self._graph.get_aspect(role_urn, RolePropertiesClass) is None:
            self._emit(
                role_urn,
                RolePropertiesClass(
                    name=user_email,
                    type=_ROLE_TYPE_READ,
                    description=(
                        f"Databricks Unity Catalog direct grant to user '{user_email}' "
                        "(mirrored by DataHub access provisioner)"
                    ),
                    requestUrl=self.config.request_url,
                ),
            )
        self._upsert_actors(role_urn, add_users=[make_user_urn(user_email)])

    def _ensure_role_properties(self, group: str) -> None:
        role_urn = self._role_urn(group)
        if self._graph.get_aspect(role_urn, RolePropertiesClass) is not None:
            return
        self._emit(
            role_urn,
            RolePropertiesClass(
                name=group,
                type=_ROLE_TYPE_READ,
                description=f"Databricks Unity Catalog group '{group}' (mirrored by DataHub access provisioner)",
                requestUrl=self.config.request_url,
            ),
        )

    def _modify_access(self, dataset_urn: str, role_urn: str, *, add: bool) -> None:
        current = self._graph.get_aspect(dataset_urn, AccessClass)
        roles = list(current.roles) if current and current.roles else []
        present = any(r.urn == role_urn for r in roles)
        if add:
            if present:
                return
            roles.append(RoleAssociationClass(urn=role_urn))
        else:
            if not present:
                return
            roles = [r for r in roles if r.urn != role_urn]
        self._emit(dataset_urn, AccessClass(roles=roles))

    def _upsert_actors(
        self,
        role_urn: str,
        *,
        add_users: list[str] | None = None,
        remove_users: list[str] | None = None,
        add_groups: list[str] | None = None,
    ) -> None:
        current = self._graph.get_aspect(role_urn, ActorsClass)
        users = list(current.users) if current and current.users else []
        groups = list(current.groups) if current and current.groups else []

        user_ids = {u.user for u in users}
        for user_urn in add_users or []:
            if user_urn not in user_ids:
                users.append(RoleUserClass(user=user_urn))
                user_ids.add(user_urn)
        if remove_users:
            removing = set(remove_users)
            users = [u for u in users if u.user not in removing]

        group_ids = {g.group for g in groups}
        for group_urn in add_groups or []:
            if group_urn not in group_ids:
                groups.append(RoleGroupClass(group=group_urn))
                group_ids.add(group_urn)

        self._emit(role_urn, ActorsClass(users=users, groups=groups))
