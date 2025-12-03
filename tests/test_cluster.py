# tests/cluster/test_cluster.py
from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable, Optional, Mapping

import pytest

from disco.metastore import Metastore  # type: ignore[unused-import]  # for type hints only
from disco.cluster import Cluster, State, ServerStatus, ACTIVE_SERVERS, SERVERS, BASE_STRUCTURE


# ---------------------------------------------------------------------------
# Fake Metastore
# ---------------------------------------------------------------------------


class FakeMetastore:
    """
    Minimal in-memory implementation of the Metastore API used by Cluster.

    - Stores values as Python objects (no serialization).
    - Records watch registrations so tests can invoke callbacks directly.
    """

    def __init__(self) -> None:
        self._stopped = False
        self.structure_ensured: list[str] = []

        self.children_watches: list[tuple[str, Callable[[list[str], str], bool]]] = []
        self.data_watches: list[tuple[str, Callable[[Any, str], bool]]] = []

        # path -> value (Python objects)
        self.data: dict[str, Any] = {}

    # --- Metastore-like API ----------------------------------------------

    def ensure_structure(self, base_structure: list[str]) -> None:
        self.structure_ensured.extend(base_structure)

    @property
    def stopped(self) -> bool:
        return self._stopped

    def stop(self) -> None:
        self._stopped = True

    def watch_members_with_callback(
        self,
        path: str,
        callback: Callable[[list[str], str], bool],
    ):
        self.children_watches.append((path, callback))
        return f"child-watch-{len(self.children_watches)}"

    def watch_with_callback(
        self,
        path: str,
        callback: Callable[[Any, str], bool],
    ):
        self.data_watches.append((path, callback))
        return f"watch-{len(self.data_watches)}"

    def list_members(self, path: str) -> list[str]:
        """
        Return immediate children under `path`, based on `self.data` keys.
        """
        prefix = path.rstrip("/") + "/"
        children: set[str] = set()
        for p in self.data.keys():
            if p.startswith(prefix):
                rest = p[len(prefix):]
                child = rest.split("/", 1)[0]
                if child:
                    children.add(child)
        return sorted(children)

    def get_keys(self, path: str) -> dict[str, Any] | None:
        """
        Returns a flat dict of direct children and their values under `path`.
        """
        prefix = path.rstrip("/") + "/"
        result: dict[str, Any] = {}
        for p, v in self.data.items():
            if p.startswith(prefix):
                rest = p[len(prefix):]
                if "/" in rest:
                    # ignore deeper than one level for this simple fake
                    continue
                result[rest] = v
        return result or None

    def update_keys(self, path: str, members: dict[str, Any]) -> None:
        for key, value in members.items():
            self.update_key(f"{path}/{key}", value)

    def update_key(
        self,
        path: str,
        value: Any,
        ephemeral: bool = False,  # matches Metastore.update_key signature
    ) -> None:
        self.data[path] = value

    def drop_key(self, path: str) -> bool:
        prefix = path.rstrip("/")
        to_delete = [
            p for p in self.data.keys()
            if p == prefix or p.startswith(prefix + "/")
        ]
        for p in to_delete:
            del self.data[p]
        return bool(to_delete)

    def __contains__(self, item: str) -> bool:
        # Direct value stored?
        if item in self.data:
            return True

        # Or does it have any children?
        prefix = item.rstrip("/") + "/"
        return any(p.startswith(prefix) for p in self.data.keys())


@pytest.fixture
def meta() -> FakeMetastore:
    return FakeMetastore()


@pytest.fixture
def cluster(meta: FakeMetastore) -> Cluster:
    return Cluster(meta=meta)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_cluster_init_ensures_base_structure_and_registers_children_watch(meta: FakeMetastore, cluster: Cluster):
    # BASE_STRUCTURE should be ensured
    assert meta.structure_ensured == BASE_STRUCTURE

    # Exactly one children watch registered on ACTIVE_SERVERS
    assert len(meta.children_watches) == 1
    path, cb = meta.children_watches[0]
    assert path == ACTIVE_SERVERS
    assert callable(cb)


def test_watch_children_adds_and_removes_servers(meta: FakeMetastore, cluster: Cluster):
    # Simulate children callback from ACTIVE_SERVERS
    _, children_cb = meta.children_watches[0]

    # First event: two servers appear
    keep = children_cb(["srv1", "srv2"], ACTIVE_SERVERS)
    assert keep is True

    status = cluster.server_status
    assert status["srv1"] == State.AVAILABLE
    assert status["srv2"] == State.AVAILABLE

    # 3 watches per server (state, nodes, repid)
    paths = {p for (p, _cb) in meta.data_watches}
    assert f"{SERVERS}/srv1/state" in paths
    assert f"{SERVERS}/srv1/nodes" in paths
    assert f"{SERVERS}/srv1/repid" in paths
    assert f"{SERVERS}/srv2/state" in paths

    # Second event: srv2 disappears, srv3 appears
    keep = children_cb(["srv1", "srv3"], ACTIVE_SERVERS)
    assert keep is True

    status = cluster.server_status
    assert "srv2" not in status
    assert status["srv1"] == State.AVAILABLE
    assert status["srv3"] == State.AVAILABLE


def test_watch_server_state_updates_internal_state(meta: FakeMetastore, cluster: Cluster):
    # Register one server via children callback
    _, children_cb = meta.children_watches[0]
    children_cb(["srv"], ACTIVE_SERVERS)

    # srv is initially AVAILABLE
    assert cluster.server_status["srv"] == State.AVAILABLE

    # Call internal callback directly with decoded State
    cluster._watch_server_state("srv", State.ACTIVE, f"{SERVERS}/srv/state")
    assert cluster.server_status["srv"] == State.ACTIVE


def test_watch_server_nodes_and_repid_builds_address_book(meta: FakeMetastore, cluster: Cluster):
    # Register one server
    _, children_cb = meta.children_watches[0]
    children_cb(["srv"], ACTIVE_SERVERS)

    # Update nodes and repid via internal callbacks (decoded values)
    cluster._watch_server_nodes("srv", ["n1", "n2"], f"{SERVERS}/srv/nodes")
    cluster._watch_server_repid("srv", "rep1", f"{SERVERS}/srv/repid")

    book = cluster.address_book
    # (repid, node) -> address
    assert book[("rep1", "n1")] == "srv"
    assert book[("rep1", "n2")] == "srv"

    # Mapping should be read-only
    with pytest.raises(TypeError):
        book[("rep1", "n3")] = "other"  # type: ignore[index]


def test_address_book_updates_on_node_change(meta: FakeMetastore, cluster: Cluster):
    _, children_cb = meta.children_watches[0]
    children_cb(["srv"], ACTIVE_SERVERS)

    cluster._watch_server_nodes("srv", ["n1", "n2"], f"{SERVERS}/srv/nodes")
    cluster._watch_server_repid("srv", "rep1", f"{SERVERS}/srv/repid")

    book1 = dict(cluster.address_book)
    assert ("rep1", "n1") in book1
    assert ("rep1", "n2") in book1

    # Now change nodes
    cluster._watch_server_nodes("srv", ["n2"], f"{SERVERS}/srv/nodes")

    book2 = dict(cluster.address_book)
    assert ("rep1", "n1") not in book2
    assert ("rep1", "n2") in book2


def test_get_available_prefers_matching_expid_and_unique_partitions(meta: FakeMetastore, cluster: Cluster):
    # Register three servers
    _, children_cb = meta.children_watches[0]
    children_cb(["s1", "s2", "s3"], ACTIVE_SERVERS)

    # Write status into metastore
    meta.update_keys(
        f"{SERVERS}/s1",
        asdict(ServerStatus(expid="exp1", partition=0)),
    )
    meta.update_keys(
        f"{SERVERS}/s2",
        asdict(ServerStatus(expid="exp1", partition=1)),
    )
    meta.update_keys(
        f"{SERVERS}/s3",
        asdict(ServerStatus(expid="exp2", partition=2)),
    )

    servers, partitions = cluster.get_available(expid="exp1")

    # s1 and s2 should be preferred (matching expid, distinct partitions)
    assert servers == ["s1", "s2", "s3"]
    assert partitions == [0, 1]


def test_list_active_servers_uses_metastore_list_members(meta: FakeMetastore, cluster: Cluster):
    # Simulate two active servers by writing into ACTIVE_SERVERS
    meta.update_key(f"{ACTIVE_SERVERS}/s1", 0)
    meta.update_key(f"{ACTIVE_SERVERS}/s2", 0)

    listed = cluster.list_active_servers()
    assert set(listed) == {"s1", "s2"}


def test_update_server_status_register_and_attribute_updates(meta: FakeMetastore, cluster: Cluster):
    # Register a new server
    cluster.update_server_status(
        server="srv",
        register=True,
        partition=5,
        expid="expX",
        repid="repX",
        nodes=["n1", "n2"],
        state=State.ACTIVE,
    )

    # ACTIVE_SERVERS entry should exist
    assert f"{ACTIVE_SERVERS}/srv" in meta.data

    # Status data under SERVERS/srv should be present
    keys = meta.get_keys(f"{SERVERS}/srv")
    assert keys is not None

    status = ServerStatus(**keys)
    assert status.partition == 5
    assert status.expid == "expX"
    assert status.repid == "repX"
    assert status.nodes == ["n1", "n2"]
    assert status.state == State.ACTIVE

    # Unregister
    cluster.update_server_status(server="srv", register=False)
    assert f"{ACTIVE_SERVERS}/srv" not in meta.data
