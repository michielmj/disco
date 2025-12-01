# tests/test_metastore.py

from __future__ import annotations

import pickle
from typing import Any, Callable, Dict, List

import pytest

# Adjust these imports to your actual module names
from disco.metastore.store import Metastore, QUEUE_POLLING_S
import disco.metastore.store as store_module  # <-- module we patch BASE_STRUCTURE in


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeQueue:
    def __init__(self):
        self._items: List[bytes] = []

    def put(self, item: bytes) -> None:
        self._items.append(item)

    def get(self) -> bytes | None:
        if self._items:
            return self._items.pop(0)
        return None


class FakeKazooClient:
    """
    In-memory fake ZooKeeper client. Stores data in a flat dict of path->bytes.
    """
    def __init__(self) -> None:
        self.data: Dict[str, bytes] = {}
        self.queues: Dict[str, FakeQueue] = {}
        self.ensure_calls: List[str] = []

    # Basic CRUD ------------------------------------------------------------

    def ensure_path(self, path: str) -> None:
        self.ensure_calls.append(path)

    def exists(self, path: str) -> bool:
        """
        Simulate ZooKeeper exists():
        - True if there is data at `path`, or
        - True if there is any child under `path/…`.
        """
        prefix = path.rstrip("/")
        if not prefix:
            prefix = "/"

        # Direct data at this node
        if prefix in self.data:
            return True

        # Any child under this node
        child_prefix = prefix + "/"
        return any(p.startswith(child_prefix) for p in self.data.keys())

    def get(self, path: str):
        return self.data.get(path, b""), None

    def set(self, path: str, value: bytes) -> None:
        self.data[path] = value

    def create(self, path: str, value: bytes, makepath: bool = False) -> None:
        self.data[path] = value

    def delete(self, path: str, recursive: bool = False) -> None:
        # naive recursive delete
        prefix = path.rstrip("/")
        to_delete = [p for p in self.data if p == prefix or p.startswith(prefix + "/")]
        for p in to_delete:
            del self.data[p]

    def get_children(self, path: str) -> list[str]:
        # Return immediate children under path
        prefix = path.rstrip("/")
        if prefix == "":
            prefix = "/"
        plen = len(prefix)
        children = set()
        for p in self.data.keys():
            if not p.startswith(prefix):
                continue
            if p == prefix:
                continue
            sub = p[plen:]
            if not sub.startswith("/"):
                continue
            parts = sub.strip("/").split("/", 1)
            if parts[0]:
                children.add(parts[0])
        return sorted(children)

    # Queue -----------------------------------------------------------------

    def Queue(self, path: str) -> FakeQueue:
        if path not in self.queues:
            self.queues[path] = FakeQueue()
        return self.queues[path]


class FakeConnectionManager:
    """
    Minimal fake for ZkConnectionManager used by Metastore tests.
    """
    def __init__(self, client: FakeKazooClient) -> None:
        self._client = client
        self.watch_registrations: list[tuple[str, Callable]] = []

    @property
    def client(self) -> FakeKazooClient:
        return self._client

    def watch_data(self, path: str, callback: Callable[[bytes | None, str], bool]):
        """
        Simulate watch registration; return a fake UUID-like object.
        """
        self.watch_registrations.append((path, callback))
        return f"watch-{len(self.watch_registrations)}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_client():
    return FakeKazooClient()


@pytest.fixture
def connection(fake_client):
    return FakeConnectionManager(fake_client)


@pytest.fixture(autouse=True)
def patch_base_structure(monkeypatch):
    # Ensure BASE_STRUCTURE is predictable for tests
    monkeypatch.setattr(
        store_module,
        "BASE_STRUCTURE",
        ["/base", "/nested/path"],
        raising=False,
    )
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_init_ensures_base_structure_without_group(connection, fake_client):
    m = Metastore(connection=connection, group=None)

    # Should have ensured each BASE_STRUCTURE path under chroot,
    # here chroot is implicit "/", so paths are as-is.
    assert "/base" in fake_client.ensure_calls
    assert "/nested/path" in fake_client.ensure_calls


def test_init_ensures_base_structure_with_group(connection, fake_client):
    m = Metastore(connection=connection, group="g1")

    # With group, base structure is prefixed with /g1
    assert "/g1/base" in fake_client.ensure_calls
    assert "/g1/nested/path" in fake_client.ensure_calls


def test_update_and_get_key_default_pickle(connection, fake_client):
    m = Metastore(connection=connection, group=None)

    value = {"a": 1, "b": [1, 2, 3]}
    m.update_key("/foo/bar", value)

    # ensure raw stored data is pickled
    stored = fake_client.data["/foo/bar"]
    assert stored == pickle.dumps(value)

    # get_key should unpickle
    assert m.get_key("/foo/bar") == value


def test_update_and_get_key_with_group(connection, fake_client):
    m = Metastore(connection=connection, group="sim1")

    m.update_key("foo", 42)
    assert "/sim1/foo" in fake_client.data

    assert m.get_key("foo") == 42
    assert m["foo"] == 42  # __getitem__


def test_custom_serialization(connection, fake_client):
    calls: list[str] = []

    def packb(obj: Any) -> bytes:
        calls.append("pack")
        return str(obj).encode("utf-8")

    def unpackb(data: bytes) -> Any:
        calls.append("unpack")
        return int(data.decode("utf-8"))

    m = Metastore(connection=connection, group=None, packb=packb, unpackb=unpackb)

    m.update_key("/num", 123)
    assert "pack" in calls
    assert fake_client.data["/num"] == b"123"

    val = m.get_key("/num")
    assert "unpack" in calls
    assert val == 123


def test_watch_with_callback_wraps_and_registers(connection, fake_client):
    m = Metastore(connection=connection, group="g")

    received: list[tuple[Any, str]] = []

    def user_cb(value: Any, path: str) -> bool:
        received.append((value, path))
        return True

    watch_id = m.watch_with_callback("/foo", user_cb)
    assert watch_id == "watch-1"

    # connection.watch_data should have been called with full path
    assert len(connection.watch_registrations) == 1
    path, wrapped = connection.watch_registrations[0]
    assert path == "/g/foo"

    # simulate a watch event
    wrapped(pickle.dumps({"x": 1}), "/g/foo")
    assert received == [({"x": 1}, "/g/foo")]

    # simulate deletion → wrapper returns False and does not call user_cb
    before = len(received)
    keep = wrapped(None, "/g/foo")
    assert keep is False
    assert len(received) == before  # no new calls


def test_contains_and_list_members(connection, fake_client):
    m = Metastore(connection=connection, group=None)

    m.update_key("/root/a", 1)
    m.update_key("/root/b", 2)
    m.update_key("/root/sub/c", 3)

    assert "/root/a" in fake_client.data
    assert "root/a" in m
    assert "root/x" not in m

    children = m.list_members("root")
    assert set(children) == {"a", "b", "sub"}


def test_get_and_update_keys_with_expand_dict(connection, fake_client):
    m = Metastore(connection=connection, group=None)

    # nested structure to write
    data = {
        "replications": {
            "r1": {"assignments": {"a": 1, "b": 2}},
            "r2": {"assignments": {"c": 3}},
        },
        "simple": 99,
    }

    expand = {"replications": {"assignments": None}}

    m.update_keys("meta", data, expand=expand, drop=True)

    # Check a few raw paths exist
    assert "/meta/simple" in fake_client.data
    assert "/meta/replications/r1/assignments/a" in fake_client.data

    # Now read back with same expand spec
    read = m.get_keys("meta", expand=expand)
    assert read["simple"] == 99
    assert read["replications"]["r1"]["assignments"]["a"] == 1
    assert read["replications"]["r2"]["assignments"]["c"] == 3


def test_drop_key(connection, fake_client):
    m = Metastore(connection=connection, group=None)

    m.update_key("/root/a", 1)
    m.update_key("/root/sub/b", 2)

    assert m.drop_key("/root")
    # both entries should be removed
    assert not fake_client.data
    assert not m.drop_key("/root")  # already gone → False


def test_enqueue_and_dequeue(connection, fake_client, monkeypatch):
    # Speed up the test by shortening the polling interval
    monkeypatch.setattr(
        store_module,
        "QUEUE_POLLING_S",
        0.01,
        raising=False,
    )

    m = Metastore(connection=connection, group=None)

    m.enqueue("/queue", {"x": 1})
    m.enqueue("/queue", {"x": 2})

    v1 = m.dequeue("/queue", timeout=1.0)
    v2 = m.dequeue("/queue", timeout=1.0)
    v3 = m.dequeue("/queue", timeout=0.1)  # will return None after timeout

    assert v1 == {"x": 1}
    assert v2 == {"x": 2}
    assert v3 is None