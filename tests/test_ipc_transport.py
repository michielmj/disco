from __future__ import annotations

import time
from multiprocessing import Queue
from multiprocessing.shared_memory import SharedMemory
from queue import Empty

from disco.envelopes import EventEnvelope, PromiseEnvelope
from disco.transports.ipc_egress import IPCTransport
from disco.transports.ipc_messages import IPCEventMsg, IPCPromiseMsg
from disco.transports.ipc_receiver import IPCReceiver


GET_TIMEOUT = 1.0  # seconds


class FakeCluster:
    def __init__(self) -> None:
        # (repid, node) -> address
        self.address_book: dict[tuple[str, str], str] = {}


class FakeNodeController:
    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []
        self.promises: list[PromiseEnvelope] = []

    def receive_event(self, envelope: EventEnvelope) -> None:
        self.events.append(envelope)

    def receive_promise(self, envelope: PromiseEnvelope) -> None:
        self.promises.append(envelope)


def _safe_get(q: Queue, what: str):
    try:
        return q.get(timeout=GET_TIMEOUT)
    except Empty:
        raise AssertionError(f"Timed out waiting for {what} on Queue")


def test_ipc_handles_node_true_when_address_and_queues_present() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "remote"
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()

    transport = IPCTransport(
        cluster=cluster,
        event_queues={"remote": event_queue},
        promise_queues={"remote": promise_queue},
    )

    assert transport.handles_node("1", "beta")


def test_ipc_handles_node_false_when_no_address_in_book() -> None:
    cluster = FakeCluster()
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()

    transport = IPCTransport(
        cluster=cluster,
        event_queues={"remote": event_queue},
        promise_queues={"remote": promise_queue},
    )

    assert not transport.handles_node("1", "beta")


def test_ipc_handles_node_false_when_missing_queues_for_address() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "remote"
    event_queue: Queue[IPCEventMsg] = Queue()

    transport = IPCTransport(
        cluster=cluster,
        event_queues={"remote": event_queue},
        promise_queues={},
    )

    assert not transport.handles_node("1", "beta")


def test_ipc_egress_small_inline_event() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "remote"
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()

    transport = IPCTransport(
        cluster=cluster,
        event_queues={"remote": event_queue},
        promise_queues={"remote": promise_queue},
    )

    assert transport.handles_node("1", "beta")

    envelope = EventEnvelope(
        target_node="beta",
        target_simproc="worker",
        epoch=1.0,
        data=b"small",
        headers={"k": "v"},
    )

    transport.send_event("1", envelope)
    msg = _safe_get(event_queue, "IPCEventMsg")

    assert msg.target_node == "beta"
    assert msg.target_simproc == "worker"
    assert msg.epoch == 1.0
    assert msg.headers == {"k": "v"}
    assert msg.data == b"small"
    assert msg.shm_name is None
    assert msg.size == len(b"small")


def test_ipc_egress_large_shm_event() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "remote"
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()

    transport = IPCTransport(
        cluster=cluster,
        event_queues={"remote": event_queue},
        promise_queues={"remote": promise_queue},
        large_payload_threshold=4,
    )

    assert transport.handles_node("1", "beta")

    payload = b"0123456789"
    envelope = EventEnvelope(
        target_node="beta",
        target_simproc="worker",
        epoch=2.0,
        data=payload,
        headers={},
    )

    transport.send_event("1", envelope)
    msg = _safe_get(event_queue, "IPCEventMsg (large)")

    assert msg.target_node == "beta"
    assert msg.target_simproc == "worker"
    assert msg.epoch == 2.0
    assert msg.headers == {}
    assert msg.data is None
    assert msg.shm_name is not None
    assert msg.size == len(payload)

    shm = SharedMemory(name=msg.shm_name)
    try:
        assert bytes(shm.buf[: msg.size]) == payload
    finally:
        shm.close()
        shm.unlink()


def test_ipc_egress_send_promise() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "remote"
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()

    transport = IPCTransport(
        cluster=cluster,
        event_queues={"remote": event_queue},
        promise_queues={"remote": promise_queue},
    )

    assert transport.handles_node("1", "beta")

    envelope = PromiseEnvelope(
        target_node="beta",
        target_simproc="worker",
        seqnr=5,
        epoch=3.0,
        num_events=2,
    )

    transport.send_promise("1", envelope)
    msg = _safe_get(promise_queue, "IPCPromiseMsg")

    assert msg.target_node == "beta"
    assert msg.target_simproc == "worker"
    assert msg.seqnr == 5
    assert msg.epoch == 3.0
    assert msg.num_events == 2


def test_ipc_receiver_small_event() -> None:
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()
    node = FakeNodeController()
    receiver = IPCReceiver(nodes={"beta": node}, event_queue=event_queue, promise_queue=promise_queue)

    msg = IPCEventMsg(
        target_node="beta",
        target_simproc="worker",
        epoch=4.0,
        headers={"a": "b"},
        data=b"inline",
        shm_name=None,
        size=len(b"inline"),
    )
    event_queue.put(msg)

    receiver._process_event(_safe_get(event_queue, "IPCEventMsg (receiver small)"))

    assert len(node.events) == 1
    envelope = node.events[0]
    assert envelope.target_node == "beta"
    assert envelope.target_simproc == "worker"
    assert envelope.epoch == 4.0
    assert envelope.data == b"inline"
    assert envelope.headers == {"a": "b"}


def test_ipc_receiver_large_event_unlinks_shm() -> None:
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()
    node = FakeNodeController()
    receiver = IPCReceiver(nodes={"beta": node}, event_queue=event_queue, promise_queue=promise_queue)

    payload = b"0123456789"
    shm = SharedMemory(create=True, size=len(payload))
    shm.buf[: len(payload)] = payload

    msg = IPCEventMsg(
        target_node="beta",
        target_simproc="worker",
        epoch=5.0,
        headers={},
        data=None,
        shm_name=shm.name,
        size=len(payload),
    )
    event_queue.put(msg)

    receiver._process_event(_safe_get(event_queue, "IPCEventMsg (receiver large)"))

    assert len(node.events) == 1
    assert node.events[0].target_node == "beta"
    assert node.events[0].data == payload

    # SharedMemory should have been unlinked by the receiver
    try:
        SharedMemory(name=shm.name)
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("SharedMemory should be unlinked")


def test_ipc_receiver_promise_delivery() -> None:
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()
    node = FakeNodeController()
    receiver = IPCReceiver(nodes={"beta": node}, event_queue=event_queue, promise_queue=promise_queue)

    msg = IPCPromiseMsg(
        target_node="beta",
        target_simproc="worker",
        seqnr=8,
        epoch=6.0,
        num_events=1,
    )
    promise_queue.put(msg)

    receiver._process_promise(_safe_get(promise_queue, "IPCPromiseMsg (receiver)"))

    assert len(node.promises) == 1
    promise = node.promises[0]
    assert promise.target_node == "beta"
    assert promise.target_simproc == "worker"
    assert promise.seqnr == 8
    assert promise.epoch == 6.0
    assert promise.num_events == 1


def test_ipc_receiver_unknown_node_raises() -> None:
    event_queue: Queue[IPCEventMsg] = Queue()
    promise_queue: Queue[IPCPromiseMsg] = Queue()
    receiver = IPCReceiver(nodes={}, event_queue=event_queue, promise_queue=promise_queue)

    msg = IPCEventMsg(
        target_node="missing",
        target_simproc="worker",
        epoch=7.0,
        headers={},
        data=b"payload",
        shm_name=None,
        size=len(b"payload"),
    )

    try:
        receiver._process_event(msg)
    except KeyError:
        return
    raise AssertionError("Expected KeyError for unknown node")
