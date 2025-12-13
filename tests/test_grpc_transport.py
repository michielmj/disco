from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import grpc  # type: ignore[import]

from disco.config import GrpcSettings
from disco.envelopes import EventEnvelope, PromiseEnvelope
from disco.transports.grpc_transport import GrpcTransport
from disco.transports.proto import transport_pb2


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class FakeCluster:
    """
    Minimal Cluster substitute exposing only address_book.
    """

    def __init__(self) -> None:
        # (repid, node) -> address ("host:port")
        self.address_book: dict[tuple[str, str], str] = {}


@dataclass
class RecordedEventCall:
    target: str
    messages: list[transport_pb2.EventEnvelopeMsg]


@dataclass
class RecordedPromiseCall:
    target: str
    request: transport_pb2.PromiseEnvelopeMsg


class FakeRpcError(grpc.RpcError):  # type: ignore[misc]
    """
    Simple RpcError subclass we can use to trigger retries.
    """

    def __init__(self, details: str = "boom") -> None:
        super().__init__()
        self._details = details

    def details(self) -> str:  # type: ignore[override]
        return self._details


class FakeChannel:
    """
    Very small stand-in for grpc.Channel that only carries a target string.
    """

    def __init__(self, target: str) -> None:
        self.target = target


class FakeStub:
    """
    Fake DiscoTransport stub used to capture outgoing calls.

    - SendEvents: collects all EventEnvelopeMsg for a given remote address.
    - SendPromise: records PromiseEnvelopeMsg calls and optionally raises
      RpcError to trigger retry logic.
    """

    def __init__(
        self,
        target: str,
        *,
        events_sink: List[RecordedEventCall],
        promises_sink: List[RecordedPromiseCall],
        promise_failures_before_success: int = 0,
    ) -> None:
        self._target = target
        self._events_sink = events_sink
        self._promises_sink = promises_sink
        self._promise_failures_before_success = promise_failures_before_success
        self.send_promise_calls = 0

    # Client-streaming RPC: accept timeout kwarg like real stubs do.
    def SendEvents(
        self,
        request_iterator: Iterable[transport_pb2.EventEnvelopeMsg],
        timeout: float | None = None,
        **_kwargs,
    ) -> transport_pb2.TransportAck:  # type: ignore[override]
        messages = list(request_iterator)
        self._events_sink.append(
            RecordedEventCall(target=self._target, messages=messages)
        )
        return transport_pb2.TransportAck(message=f"received {len(messages)}")

    # Unary RPC: also accept timeout kwarg.
    def SendPromise(
        self,
        request: transport_pb2.PromiseEnvelopeMsg,
        timeout: float | None = None,
        **_kwargs,
    ) -> transport_pb2.TransportAck:  # type: ignore[override]
        self.send_promise_calls += 1
        if self.send_promise_calls <= self._promise_failures_before_success:
            # Simulate transient failure to trigger retry.
            raise FakeRpcError("transient promise failure")
        self._promises_sink.append(
            RecordedPromiseCall(target=self._target, request=request)
        )
        return transport_pb2.TransportAck(message="ok")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_handles_node_based_on_address_book() -> None:
    cluster = FakeCluster()
    settings = GrpcSettings()

    cluster.address_book[("exp1", "nodeA")] = "hostA:5001"

    def channel_factory(target: str, _settings: GrpcSettings) -> FakeChannel:
        return FakeChannel(target)

    def stub_factory(ch: FakeChannel) -> FakeStub:
        return FakeStub(
            target=ch.target,
            events_sink=[],
            promises_sink=[],
        )

    transport = GrpcTransport(
        cluster=cluster,
        settings=settings,
        channel_factory=channel_factory,  # type: ignore[arg-type]
        stub_factory=stub_factory,        # type: ignore[arg-type]
    )

    assert transport.handles_node("exp1", "nodeA")
    assert not transport.handles_node("exp1", "nodeB")


def test_send_event_creates_correct_proto_and_uses_target_address() -> None:
    cluster = FakeCluster()
    settings = GrpcSettings()

    # Map (repid, node) -> remote address
    cluster.address_book[("r1", "beta")] = "remote-host:6000"

    recorded_events: List[RecordedEventCall] = []
    recorded_promises: List[RecordedPromiseCall] = []

    def channel_factory(target: str, _settings: GrpcSettings) -> FakeChannel:
        return FakeChannel(target)

    def stub_factory(ch: FakeChannel) -> FakeStub:
        return FakeStub(
            target=ch.target,
            events_sink=recorded_events,
            promises_sink=recorded_promises,
        )

    transport = GrpcTransport(
        cluster=cluster,
        settings=settings,
        channel_factory=channel_factory,  # type: ignore[arg-type]
        stub_factory=stub_factory,        # type: ignore[arg-type]
    )

    envelope = EventEnvelope(
        repid="r1",
        target_node="beta",
        target_simproc="worker",
        epoch=1.23,
        data=b"hello",
        headers={"k": "v"},
    )

    transport.send_event(envelope)

    # Exactly one stream call
    assert len(recorded_events) == 1
    call = recorded_events[0]
    assert call.target == "remote-host:6000"
    assert len(call.messages) == 1

    msg = call.messages[0]
    assert msg.target_node == "beta"
    assert msg.target_simproc == "worker"
    assert msg.epoch == 1.23
    assert msg.data == b"hello"
    assert dict(msg.headers) == {"k": "v"}


def test_send_promise_success_without_retry() -> None:
    cluster = FakeCluster()
    settings = GrpcSettings()

    cluster.address_book[("r1", "beta")] = "remote-host:6001"

    recorded_events: List[RecordedEventCall] = []
    recorded_promises: List[RecordedPromiseCall] = []

    def channel_factory(target: str, _settings: GrpcSettings) -> FakeChannel:
        return FakeChannel(target)

    def stub_factory(ch: FakeChannel) -> FakeStub:
        # No failures; first call succeeds.
        return FakeStub(
            target=ch.target,
            events_sink=recorded_events,
            promises_sink=recorded_promises,
            promise_failures_before_success=0,
        )

    transport = GrpcTransport(
        cluster=cluster,
        settings=settings,
        channel_factory=channel_factory,  # type: ignore[arg-type]
        stub_factory=stub_factory,        # type: ignore[arg-type]
    )

    envelope = PromiseEnvelope(
        repid="r1",
        target_node="beta",
        target_simproc="control",
        seqnr=42,
        epoch=7.5,
        num_events=3,
    )

    transport.send_promise(envelope)

    # Exactly one successful promise call
    assert len(recorded_promises) == 1
    call = recorded_promises[0]
    assert call.target == "remote-host:6001"
    msg = call.request
    assert msg.target_node == "beta"
    assert msg.target_simproc == "control"
    assert msg.seqnr == 42
    assert msg.epoch == 7.5
    assert msg.num_events == 3
