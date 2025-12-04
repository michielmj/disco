import pytest
from dataclasses import asdict
from typing import Any, Callable, List, Tuple


from disco.cluster import (
    Cluster,
    State,
    WorkerInfo,
    REGISTERED_WORKERS,
    WORKERS,
    BASE_STRUCTURE,
)


# ---------------------------------------------------------------------------
# Fake Metastore
# ---------------------------------------------------------------------------

class FakeMetastore:
    """
    Minimal fake of disco.metastore.Metastore, enough to test Cluster.

    NOTE:
    - Paths are treated as already "logical" (no group or chroot).
    - watch_with_callback behaves like a DataWatch: callback is invoked
      immediately when the watch is registered, and on each update_key for
      that path.
    - watch_members_with_callback is driven manually from tests.
    """

    def __init__(self) -> None:
        self.structure_calls: List[list[str]] = []

        # path -> value
        self.data: dict[str, Any] = {}

        # watches
        self.children_watches: List[Tuple[str, Callable[[list[str], str], bool]]] = []
        self.data_watches: List[Tuple[str, Callable[[Any, str], bool]]] = []

    # --- Metastore-like API ------------------------------------------------

    def ensure_structure(self, base_structure: list[str]) -> None:
        self.structure_calls.append(list(base_structure))

    # watches ---------------------------------------------------------------

    def watch_members_with_callback(
        self,
        path: str,
        callback: Callable[[list[str], str], bool],
    ):
        self.children_watches.append((path, callback))
        return "watch-members"

    def watch_with_callback(
        self,
        path: str,
        callback: Callable[[Any, str], bool],
    ):
        """
        Simulate Kazoo DataWatch: call once with current data (if any),
        and again on each update_key.
        """
        self.data_watches.append((path, callback))

        # initial call with current value (or None)
        current = self.data.get(path)
        keep = callback(current, path)
        if not keep:
            self.data_watches.remove((path, callback))

        return "watch-data"

    # basic KV --------------------------------------------------------------
    # noinspection PyUnusedLocal
    def update_key(self, path: str, value: Any, ephemeral: bool = False) -> None:
        self.data[path] = value

        # trigger all watches on this path
        for p, cb in list(self.data_watches):
            if p == path:
                keep = cb(value, path)
                if not keep:
                    self.data_watches.remove((p, cb))

    def update_keys(self, path: str, members: dict[str, Any]) -> None:
        """Simple shallow update: store each key at path/key."""
        for key, value in members.items():
            self.update_key(f"{path}/{key}", value)

    def get_key(self, path: str) -> Any:
        return self.data.get(path)

    def get_keys(self, path: str) -> dict[str, Any] | None:
        """
        Return immediate children under `path` as {name: value}.
        Only supports one-level WorkerInfo layout.
        """
        prefix = path.rstrip("/")
        plen = len(prefix) + 1
        result: dict[str, Any] = {}

        for p, v in self.data.items():
            if not p.startswith(prefix + "/"):
                continue
            tail = p[plen:]
            # only immediate children (no nested slashes)
            if "/" in tail:
                continue
            result[tail] = v

        return result or None

    def drop_key(self, path: str) -> bool:
        keys = [k for k in self.data if k == path or k.startswith(path + "/")]
        for k in keys:
            del self.data[k]
        return bool(keys)

    def __contains__(self, item: str) -> bool:
        """
        Mimic ZooKeeper semantics: a node 'item' is considered to exist if
        - it has data stored directly, OR
        - it has at least one child (i.e. some key with 'item/' as a prefix).
        """
        if item in self.data:
            return True

        prefix = item.rstrip("/") + "/"
        return any(key.startswith(prefix) for key in self.data.keys())

    # listing ---------------------------------------------------------------

    def list_members(self, path: str) -> list[str]:
        prefix = path.rstrip("/")
        plen = len(prefix) + 1
        children: set[str] = set()

        for p in self.data.keys():
            if not p.startswith(prefix + "/"):
                continue
            tail = p[plen:]
            head = tail.split("/", 1)[0]
            if head:
                children.add(head)

        return sorted(children)

    # not used by Cluster, but exist on real Metastore
    @property
    def stopped(self) -> bool:
        return False


@pytest.fixture
def meta() -> FakeMetastore:
    return FakeMetastore()


# noinspection PyTypeChecker
@pytest.fixture
def cluster(meta: FakeMetastore) -> Cluster:
    return Cluster(meta)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# noinspection PyTypeChecker
def test_cluster_init_ensures_base_structure(meta: FakeMetastore):
    _cluster = Cluster(meta)
    assert meta.structure_calls
    # last call should be BASE_STRUCTURE
    assert meta.structure_calls[-1] == BASE_STRUCTURE


def test_register_and_unregister_worker(meta: FakeMetastore, cluster: Cluster):
    # Initially nothing in REGISTERED_WORKERS
    assert REGISTERED_WORKERS not in meta.data

    cluster.register_worker("s1", state=State.INITIALIZING)

    # WorkerInfo was written (shallow layout)
    assert meta.get_keys(f"{WORKERS}/s1") == asdict(WorkerInfo())

    # Active worker node with state
    assert meta.get_key(f"{REGISTERED_WORKERS}/s1") == State.INITIALIZING

    # Trigger children watch to let Cluster install its per-worker watches
    path, children_cb = meta.children_watches[0]
    assert path == REGISTERED_WORKERS
    children_cb(["s1"], path)

    # In-memory state should now be visible
    assert cluster.get_worker_state("s1") == State.INITIALIZING
    assert cluster.worker_states["s1"] == State.INITIALIZING

    # Unregister removes the registered node entry
    cluster.unregister_worker("s1")
    assert f"{REGISTERED_WORKERS}/s1" not in meta.data

    with pytest.raises(RuntimeError):
        cluster.unregister_worker("s1")


def test_set_and_get_worker_state_updates_internal_state(meta: FakeMetastore, cluster: Cluster):
    cluster.register_worker("s1", state=State.CREATED)

    # Trigger children watch to attach state watcher
    path, children_cb = meta.children_watches[0]
    children_cb(["s1"], path)

    # Initial state from metastore
    assert cluster.get_worker_state("s1") == State.CREATED
    assert cluster.worker_states["s1"] == State.CREATED

    # Change state via Cluster API
    cluster.set_worker_state("s1", State.AVAILABLE)

    # Data is stored in metastore
    assert meta.get_key(f"{REGISTERED_WORKERS}/s1") == State.AVAILABLE
    # And internal view is updated by the watch
    assert cluster.worker_states["s1"] == State.AVAILABLE


def test_update_worker_info_and_address_book(meta: FakeMetastore, cluster: Cluster):
    cluster.register_worker("s1", state=State.AVAILABLE)

    # Write info first
    cluster.update_worker_info("s1", repid="r1", nodes=["n1", "n2"])

    # Trigger children watch so node/repid watches are installed and immediately fired
    path, children_cb = meta.children_watches[0]
    children_cb(["s1"], path)

    # Address book should map (repid, node) -> worker address
    book = cluster.address_book
    assert book[("r1", "n1")] == "s1"
    assert book[("r1", "n2")] == "s1"


def test_get_available_prefers_matching_expid_and_unique_partitions(meta: FakeMetastore, cluster: Cluster):
    # Register three workers in AVAILABLE state
    cluster.register_worker("s1", state=State.AVAILABLE)
    cluster.register_worker("s2", state=State.AVAILABLE)
    cluster.register_worker("s3", state=State.AVAILABLE)

    # Add worker info with expid/partition
    cluster.update_worker_info("s1", expid="exp1", partition=0)
    cluster.update_worker_info("s2", expid="exp1", partition=1)
    cluster.update_worker_info("s3", expid="exp2", partition=2)

    # Trigger children watch once with all registered workers
    path, children_cb = meta.children_watches[0]
    children = meta.list_members(REGISTERED_WORKERS)
    assert set(children) == {"s1", "s2", "s3"}
    children_cb(children, path)

    workers, partitions = cluster.get_available(expid="exp1")

    # First entries should be the preferred ones (matching expid, unique partitions)
    preferred = set(workers[:2])
    assert preferred == {"s1", "s2"}
    assert set(partitions) == {0, 1}

    # The remaining worker is "others"
    assert set(workers[2:]) == {"s3"}
