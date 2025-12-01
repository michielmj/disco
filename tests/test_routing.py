from __future__ import annotations

import pickle
from typing import Any

import pytest

from disco.envelopes import EventEnvelope, PromiseEnvelope
from disco.node_controller import NodeController
from disco.router import Router
from disco.transports.inprocess import InProcessTransport


def test_local_event_delivery() -> None:
    router = Router()
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
    assert envelope.data == b"payload"
    assert envelope.headers == {"k": "v"}


def test_self_alias_uses_local_delivery() -> None:
    router = Router()
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
    router = Router()
    sender = NodeController("alpha", router)
    receiver = NodeController("beta", router)

    received: list[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        received.append(envelope)

    receiver._deliver_local_event = record

    calls: list[bytes] = []

    def serializer(obj: Any) -> bytes:
        calls.append(b"call")
        return pickle.dumps(obj)

    sender._serializer = serializer

    sender.send_event("beta/main", epoch=3.0, data=b"remote")

    assert len(calls) == 1
    assert len(received) == 1
    assert received[0].data == b"remote"


def test_remote_promise_via_transport() -> None:
    router_a = Router()
    router_b = Router()

    sender = NodeController("alpha", router_a)
    receiver = NodeController("beta", router_b)

    received_promises: list[PromiseEnvelope] = []

    def record_promise(envelope: PromiseEnvelope) -> None:
        received_promises.append(envelope)

    receiver._deliver_local_promise = record_promise

    transport = InProcessTransport({"beta": receiver})
    router_a.register_transport(transport)

    sender.send_promise("beta/control", seqnr=7, epoch=4.5, num_events=2)

    assert len(received_promises) == 1
    promise = received_promises[0]
    assert promise.seqnr == 7
    assert promise.target_simproc == "control"


def test_remote_event_through_transport() -> None:
    router_a = Router()
    router_b = Router()

    sender = NodeController("alpha", router_a)
    receiver = NodeController("beta", router_b)

    received: list[EventEnvelope] = []

    def record(envelope: EventEnvelope) -> None:
        received.append(envelope)

    receiver._deliver_local_event = record

    transport = InProcessTransport({"beta": receiver})
    router_a.register_transport(transport)

    sender.send_event("beta/main", epoch=5.0, data=b"via-transport")

    assert len(received) == 1
    envelope = received[0]
    assert envelope.target_node == "beta"
    assert envelope.data == b"via-transport"
