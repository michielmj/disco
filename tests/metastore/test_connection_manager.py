import uuid
from typing import Any, Callable, List, Tuple

import pytest

# Adjust these imports to match your project structure
from disco.metastore import ZkConnectionManager
from kazoo.client import KazooState


class FakeKazooClient:
    """
    Minimal fake KazooClient for testing ZkConnectionManager.

    It only implements what the manager needs:
      - add_listener
      - start / stop / close
    """

    # noinspection PyUnusedLocal
    def __init__(self, *args, **kwargs) -> None:
        self.listeners: list[Callable[[Any], None]] = []
        self.started = False
        self.stopped = False
        self.closed = False

    def add_listener(self, callback: Callable[[Any], None]) -> None:
        self.listeners.append(callback)

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def close(self) -> None:
        self.closed = True


class DummySettings:
    """Simple stand-in for ZookeeperSettings."""
    def __init__(self, chroot: str = "/") -> None:
        self.chroot = chroot


@pytest.fixture
def fake_kazoo(monkeypatch):
    """
    Monkeypatch create_zk_client to return our FakeKazooClient and
    DataWatch to just record registrations.
    """
    fake_client = FakeKazooClient()

    # noinspection PyUnusedLocal
    def _fake_create_zk_client(settings):
        return fake_client

    monkeypatch.setattr(
        "disco.metastore.helpers.create_zk_client",
        _fake_create_zk_client,
    )

    data_registrations: List[Tuple[FakeKazooClient, str, Callable]] = []
    children_registrations: List[Tuple[FakeKazooClient, str, Callable]] = []

    def _fake_data_watch(client, path, func):
        data_registrations.append((client, path, func))
        return None

    def _fake_children_watch(client, path, func):
        children_registrations.append((client, path, func))
        return None

    monkeypatch.setattr("disco.metastore.helpers.DataWatch", _fake_data_watch)
    monkeypatch.setattr("disco.metastore.helpers.ChildrenWatch", _fake_children_watch)

    return fake_client, data_registrations, children_registrations


# noinspection PyTypeChecker,PyUnresolvedReferences,PyArgumentList
def test_start_and_stop_lifecycle(fake_kazoo):
    fake_client, _, _ = fake_kazoo

    mgr = ZkConnectionManager(DummySettings())
    mgr.start()

    assert fake_client.started is True
    assert fake_client.stopped is False
    assert fake_client.closed is False

    mgr.stop()

    assert fake_client.stopped is True
    assert fake_client.closed is True


# noinspection PyTypeChecker,PyUnresolvedReferences,PyArgumentList
def test_watch_data_registers_and_reinstalls_on_session_loss(fake_kazoo):
    fake_client, registrations, children_regs = fake_kazoo

    mgr = ZkConnectionManager(DummySettings())
    mgr.start()

    # register a single watch; return False on data=None to stop watching
    # noinspection PyUnusedLocal
    def cb(data: bytes | None, path: str) -> bool:
        # simulate "delete this watch" when node is deleted
        return data is not None

    watch_id = mgr.watch_data("/foo", cb)
    assert isinstance(watch_id, uuid.UUID)

    # DataWatch should have been called once
    assert len(registrations) == 1
    client1, path1, func1 = registrations[0]
    assert client1 is fake_client
    assert path1 == "/foo"
    assert callable(func1)

    # Simulate LOST -> CONNECTED transition
    mgr._on_state_change(KazooState.LOST)
    mgr._on_state_change(KazooState.CONNECTED)

    # After reinstallation, DataWatch should have been called twice
    assert len(registrations) == 2
    client2, path2, func2 = registrations[1]
    assert client2 is fake_client
    assert path2 == "/foo"
    assert callable(func2)

    # Ensure that callback removal on False works:
    # call wrapper with None to simulate node deletion
    keep = func2(None, None, None)
    assert keep is False

    # Because the watcher removed itself from the cache, another
    # LOST->CONNECTED should NOT reinstall it (no new registration)
    mgr._on_state_change(KazooState.LOST)
    mgr._on_state_change(KazooState.CONNECTED)

    # Still only 2 registrations, no extra watch
    assert len(registrations) == 2


# noinspection PyTypeChecker,PyUnresolvedReferences,PyArgumentList
def test_watch_children_registers_and_reinstalls_on_session_loss(fake_kazoo):
    fake_client, data_regs, children_regs = fake_kazoo

    mgr = ZkConnectionManager(DummySettings())
    mgr.start()

    # noinspection PyUnusedLocal
    def cb(children: list[str] | None, path: str) -> bool:
        # stop watching when children becomes None
        return children is not None

    watch_id = mgr.watch_children("/bar", cb)
    assert isinstance(watch_id, uuid.UUID)

    # ChildrenWatch should have been called once
    assert len(children_regs) == 1
    client1, path1, func1 = children_regs[0]
    assert client1 is fake_client
    assert path1 == "/bar"
    assert callable(func1)

    # LOST -> CONNECTED triggers re-install
    mgr._on_state_change(KazooState.LOST)
    mgr._on_state_change(KazooState.CONNECTED)

    assert len(children_regs) == 2
    client2, path2, func2 = children_regs[1]
    assert client2 is fake_client
    assert path2 == "/bar"

    # Simulate a normal update
    keep = func2(["a", "b"])
    assert keep is True

    # Simulate terminal event: children=None → stop watching
    keep2 = func2(None)
    assert keep2 is False

    # Further reconnect should NOT re-install
    mgr._on_state_change(KazooState.LOST)
    mgr._on_state_change(KazooState.CONNECTED)

    assert len(children_regs) == 2
