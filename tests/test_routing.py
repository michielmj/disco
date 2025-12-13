from __future__ import annotations

import pickle
from typing import Any, List

from disco.envelopes import EventEnvelope, PromiseEnvelope
from disco.node_controller import NodeController
from disco.router import WorkerRouter
from disco.transports.inprocess import InProcessTransport


class FakeCluster:
    def __init__(self) -> None:
        # (repid, node) -> address
        self.address_book: dict[tuple[str, str], str] = {}


def test_local_event_delivery() -> None:
    """
    Local target ("alpha/main") should be delivered via the local delivery hook
    without going through any transport, with the payload serialized once.
    """
    cluster = FakeCluster()
    router = WorkerRouter(cluster=cluster, transports=[])
    controller = NodeController("alpha", "e1", "r1", 0, router)

    captured: List[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        captured.append(envelope)

    # Monkeypatch local delivery hook (takes a single EventEnvelope)
    controller._deliver_local_event = record  # type: ignore[assignment]

    controller.send_event("alpha/main", epoch=1.0, data=b"payload", headers={"k": "v"})

    assert len(captured) == 1
    envelope = captured[0]
    assert envelope.target_node == "alpha"
    assert envelope.target_simproc == "main"
    assert envelope.epoch == 1.0
    # Default serializer is pickle.dumps
    assert envelope.data == pickle.dumps(b"payload")
    assert envelope.headers == {"k": "v"}


def test_self_alias_uses_local_delivery() -> None:
    """
    Target "self/..." must be resolved to the controller's own node_name and
    still use local delivery (no transport).
    """
    cluster = FakeCluster()
    router = WorkerRouter(cluster=cluster, transports=[])
    controller = NodeController("gamma", "e1", "r1", 0, router)

    captured: List[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        captured.append(envelope)

    controller._deliver_local_event = record  # type: ignore[assignment]

    controller.send_event("self/compute", epoch=2.5, data=b"bytes")

    assert len(captured) == 1
    envelope = captured[0]
    assert envelope.target_node == "gamma"
    assert envelope.target_simproc == "compute"
    assert envelope.epoch == 2.5
    assert pickle.loads(envelope.data) == b"bytes"


def test_remote_event_serialization_once() -> None:
    """
    For a remote target, NodeController should serialize the payload exactly once,
    construct an EventEnvelope, and pass it through the router/transport.
    """
    cluster = FakeCluster()
    cluster.address_book[("r1", "beta")] = "local"

    received: list[EventEnvelope] = []

    def record_envelope(envelope: EventEnvelope) -> None:
        received.append(envelope)

    # Receiver side: capture the envelope at receive_event level
    receiver_router = WorkerRouter(cluster=cluster, transports=[])
    receiver = NodeController("beta", "e1", "r1", 0, receiver_router)
    receiver.receive_event = record_envelope  # type: ignore[assignment]

    transport = InProcessTransport(nodes={"beta": receiver}, cluster=cluster)
    sender_router = WorkerRouter(cluster=cluster, transports=[transport])

    calls: list[bytes] = []

    def serializer(obj: Any) -> bytes:
        calls.append(b"call")
        return pickle.dumps(obj)

    # Sender uses custom serializer
    sender = NodeController("alpha", "e1", "r1", 0, sender_router, serializer=serializer)

    sender.send_event("beta/main", epoch=3.0, data=b"remote")

    assert len(calls) == 1
    assert len(received) == 1
    envelope = received[0]
    assert envelope.target_node == "beta"
    assert envelope.target_simproc == "main"
    assert pickle.loads(envelope.data) == b"remote"


def test_remote_promise_via_transport() -> None:
    """
    Promises to remote nodes should be wrapped in PromiseEnvelope and passed
    through the transport, ending up at the receiver's receive_promise().
    """
    cluster = FakeCluster()
    cluster.address_book[("r1", "beta")] = "local"

    received_promises: list[PromiseEnvelope] = []

    def record_promise(envelope: PromiseEnvelope) -> None:
        received_promises.append(envelope)

    receiver_router = WorkerRouter(cluster=cluster, transports=[])
    receiver = NodeController("beta", "e1", "r1", 0, receiver_router)
    receiver.receive_promise = record_promise  # type: ignore[assignment]

    transport = InProcessTransport(nodes={"beta": receiver}, cluster=cluster)
    sender_router = WorkerRouter(cluster=cluster, transports=[transport])
    sender = NodeController("alpha", "e1", "r1", 0, sender_router)

    sender.send_promise("beta/control", seqnr=7, epoch=4.5, num_events=2)

    assert len(received_promises) == 1
    promise = received_promises[0]
    assert promise.target_node == "beta"
    assert promise.target_simproc == "control"
    assert promise.seqnr == 7
    assert promise.epoch == 4.5
    assert promise.num_events == 2


def test_remote_event_through_transport() -> None:
    """
    Remote events should go through InProcessTransport and arrive at the
    receiver's receive_event() as an EventEnvelope with serialized data.
    """
    cluster = FakeCluster()
    cluster.address_book[("r1", "beta")] = "local"

    received: list[EventEnvelope] = []

    def record_envelope(envelope: EventEnvelope) -> None:
        received.append(envelope)

    receiver_router = WorkerRouter(cluster=cluster, transports=[])
    receiver = NodeController("beta", "e1", "r1", 0, receiver_router)
    receiver.receive_event = record_envelope  # type: ignore[assignment]

    transport = InProcessTransport(nodes={"beta": receiver}, cluster=cluster)
    sender_router = WorkerRouter(cluster=cluster, transports=[transport])
    sender = NodeController("alpha", "e1", "r1", 0, sender_router)

    sender.send_event("beta/main", epoch=5.0, data=b"via-transport")

    assert len(received) == 1
    envelope = received[0]
    assert envelope.target_node == "beta"
    assert envelope.target_simproc == "main"
    assert envelope.epoch == 5.0
    assert envelope.data == pickle.dumps(b"via-transport")
