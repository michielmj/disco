from __future__ import annotations

import pickle
from typing import Any

from disco.envelopes import EventEnvelope, PromiseEnvelope
from disco.node_controller import NodeController
from disco.router import ServerRouter
from disco.transports.inprocess import InProcessTransport


class FakeCluster:
    def __init__(self) -> None:
        self.address_book: dict[tuple[str, str], str] = {}


def test_local_event_delivery() -> None:
    cluster = FakeCluster()
    router = ServerRouter(cluster=cluster, transports=[], repid="1")
    controller = NodeController("alpha", router)

    captured: list[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        captured.append(envelope)

    controller._deliver_local_event = record

    controller.send_event("alpha/main", epoch=1.0, data=b"payload", headers={"k": "v"})

    assert len(captured) == 1
    envelope = captured[0]
    assert envelope.target_node == "alpha"
    assert envelope.target_simproc == "main"
    assert envelope.data == pickle.dumps(b"payload")
    assert envelope.headers == {"k": "v"}


def test_self_alias_uses_local_delivery() -> None:
    cluster = FakeCluster()
    router = ServerRouter(cluster=cluster, transports=[], repid="2")
    controller = NodeController("gamma", router)

    captured: list[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        captured.append(envelope)

    controller._deliver_local_event = record

    controller.send_event("self/compute", epoch=2.5, data=b"bytes")

    assert len(captured) == 1
    envelope = captured[0]
    assert envelope.target_node == "gamma"
    assert envelope.target_simproc == "compute"


def test_remote_event_serialization_once() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "local"

    received: list[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        received.append(envelope)

    receiver_router = ServerRouter(cluster=cluster, transports=[], repid="1")
    receiver = NodeController("beta", receiver_router)
    receiver._deliver_local_event = record

    transport = InProcessTransport(nodes={"beta": receiver}, cluster=cluster)
    sender_router = ServerRouter(cluster=cluster, transports=[transport], repid="1")
    sender = NodeController("alpha", sender_router)

    calls: list[bytes] = []

    def serializer(obj: Any) -> bytes:
        calls.append(b"call")
        return pickle.dumps(obj)

    sender.serializer = serializer

    sender.send_event("beta/main", epoch=3.0, data=b"remote")

    assert len(calls) == 1
    assert len(received) == 1
    assert pickle.loads(received[0].data) == b"remote"


def test_remote_promise_via_transport() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "local"

    received_promises: list[PromiseEnvelope] = []

    def record_promise(envelope: PromiseEnvelope) -> None:
        received_promises.append(envelope)

    receiver_router = ServerRouter(cluster=cluster, transports=[], repid="1")
    receiver = NodeController("beta", receiver_router)
    receiver._deliver_local_promise = record_promise

    transport = InProcessTransport(nodes={"beta": receiver}, cluster=cluster)
    sender_router = ServerRouter(cluster=cluster, transports=[transport], repid="1")
    sender = NodeController("alpha", sender_router)

    sender.send_promise("beta/control", seqnr=7, epoch=4.5, num_events=2)

    assert len(received_promises) == 1
    promise = received_promises[0]
    assert promise.seqnr == 7
    assert promise.target_simproc == "control"


def test_remote_event_through_transport() -> None:
    cluster = FakeCluster()
    cluster.address_book[("1", "beta")] = "local"

    received: list[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        received.append(envelope)

    receiver_router = ServerRouter(cluster=cluster, transports=[], repid="1")
    receiver = NodeController("beta", receiver_router)
    receiver._deliver_local_event = record

    transport = InProcessTransport(nodes={"beta": receiver}, cluster=cluster)
    sender_router = ServerRouter(cluster=cluster, transports=[transport], repid="1")
    sender = NodeController("alpha", sender_router)

    sender.send_event("beta/main", epoch=5.0, data=b"via-transport")

    assert len(received) == 1
    envelope = received[0]
    assert envelope.target_node == "beta"
    assert envelope.target_simproc == "main"
    assert envelope.data == pickle.dumps(b"via-transport")
