from __future__ import annotations

"""
gRPC transport for delivering envelopes to remote workers.

Proto file: src/disco/transports/proto/transport.proto

service DiscoTransport {
  rpc SendEvents(stream EventEnvelopeMsg) returns (TransportAck);
  rpc SendPromise(PromiseEnvelopeMsg) returns (TransportAck);
}

Design (ENGINEERING_SPEC chapter 6):

- Events:
    - Sent via a client-streaming RPC (SendEvents).
    - For now, we open a short-lived stream per call; this can be
      optimized later into long-lived streams per remote address.

- Promises:
    - Always small; sent via unary RPC (SendPromise).
    - Delivery is retried with backoff, using GrpcSettings:
        - promise_retry_delays_s: list[float]
        - promise_retry_max_window_s: float
    - On persistent failure, the sending Worker transitions to BROKEN.

The transport is purely outbound. The corresponding gRPC server
("ingress") lives in a separate module and delivers envelopes into
NodeController queues on the receiving Worker.
"""

from dataclasses import dataclass
import time
from typing import Callable, Dict, Iterable, Optional

import grpc

from ..cluster import Cluster, WorkerState
from ..config import GrpcSettings
from ..envelopes import EventEnvelope, PromiseEnvelope
from .base import Transport

from .proto import transport_pb2, transport_pb2_grpc  # generated from transport.proto

from tools.mp_logging import getLogger

logger = getLogger(__name__)


@dataclass(slots=True)
class _RemoteEndpoint:
    """
    Holds channel and stubs for a single remote worker address.
    """
    address: str
    channel: grpc.Channel
    stub: transport_pb2_grpc.DiscoTransportStub


class GrpcTransport(Transport):
    """
    Transport that delivers envelopes to remote Workers via gRPC.

    Responsibilities (ENGINEERING_SPEC §6.7):

    - Decide whether it handles a node based on Cluster.address_book.
      Any node with an address in the address_book is considered
      reachable via gRPC; more efficient transports are selected
      earlier by the WorkerRouter.
    - Maintain per-address gRPC channels and stubs.
    - Send events via client-streaming RPC (SendEvents).
    - Send promises via unary RPC (SendPromise) with backoff and retry.
    - On persistent promise delivery failure, log and mark the sender
      Worker as BROKEN.
    """

    def __init__(
        self,
        cluster: Cluster,
        settings: GrpcSettings,
        *,
        # Optional injection points to ease testing or customization:
        channel_factory: Optional[Callable[[str, GrpcSettings], grpc.Channel]] = None,
        stub_factory: Optional[
            Callable[[grpc.Channel], transport_pb2_grpc.DiscoTransportStub]
        ] = None,
    ) -> None:
        self._cluster = cluster
        self._settings = settings
        self._channel_factory = channel_factory or _default_channel_factory
        self._stub_factory = stub_factory or _default_stub_factory

        # address -> _RemoteEndpoint
        self._endpoints: Dict[str, _RemoteEndpoint] = {}

        # Cache retry configuration locally for speed
        delays = list(settings.promise_retry_delays_s)
        self._promise_retry_delays_s: list[float] = delays or [0.05, 0.15, 0.5, 1.0, 2.0]
        self._promise_retry_max_window_s: float = settings.promise_retry_max_window_s

    # ------------------------------------------------------------------ #
    # Transport interface
    # ------------------------------------------------------------------ #

    def handles_node(self, repid: str, node: str) -> bool:
        """
        Returns True if the node has an address in the Cluster address_book.

        The WorkerRouter ensures that more efficient transports (in-process,
        IPC) are tried first. gRPC is the catch-all for any node with a
        known address that is not claimed by earlier transports.
        """
        return (repid, node) in self._cluster.address_book

    def send_event(self, envelope: EventEnvelope) -> None:
        """
        Send a single EventEnvelope via gRPC.

        For now we open a short-lived client-stream (SendEvents) carrying
        exactly one message. This keeps the implementation simple while
        matching the protobuf shape. Later this can be optimized to
        maintain long-lived streams per remote address.
        """
        addr = self._resolve_address(envelope.repid, envelope.target_node)
        endpoint = self._get_or_create_endpoint(addr)

        msg = transport_pb2.EventEnvelopeMsg(
            repid=envelope.repid,
            target_node=envelope.target_node,
            target_simproc=envelope.target_simproc,
            epoch=envelope.epoch,
            data=envelope.data,
            headers=envelope.headers,
        )

        logger.debug(
            "GrpcTransport.send_event: repid=%s node=%s simproc=%s addr=%s",
            envelope.repid,
            envelope.target_node,
            envelope.target_simproc,
            addr,
        )

        def _iter() -> Iterable[transport_pb2.EventEnvelopeMsg]:
            yield msg

        try:
            # We ignore the TransportAck payload for now.
            endpoint.stub.SendEvents(_iter(), timeout=self._settings.timeout_s)
        except grpc.RpcError as exc:
            logger.error(
                "gRPC SendEvents failed for addr=%s node=%s simproc=%s: %s",
                addr,
                envelope.target_node,
                envelope.target_simproc,
                exc,
            )
            # Let the caller / Worker decide how to react (possibly BROKEN).
            raise

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        """
        Send a PromiseEnvelope via unary gRPC with retry and backoff.

        If all retries within promise_retry_max_window_s fail, this
        method logs the failure, marks the sending Worker as BROKEN, and
        re-raises the final RpcError.
        """
        addr = self._resolve_address(envelope.repid, envelope.target_node)
        endpoint = self._get_or_create_endpoint(addr)

        msg = transport_pb2.PromiseEnvelopeMsg(
            repid=envelope.repid,
            target_node=envelope.target_node,
            target_simproc=envelope.target_simproc,
            seqnr=envelope.seqnr,
            epoch=envelope.epoch,
            num_events=envelope.num_events,
        )

        logger.debug(
            "GrpcTransport.send_promise: repid=%s node=%s simproc=%s seqnr=%s addr=%s",
            envelope.repid,
            envelope.target_node,
            envelope.target_simproc,
            envelope.seqnr,
            addr,
        )

        start = time.monotonic()
        delays = self._promise_retry_delays_s
        max_window = self._promise_retry_max_window_s
        attempts = 0

        while True:
            try:
                # We ignore the TransportAck payload for now.
                endpoint.stub.SendPromise(msg, timeout=self._settings.timeout_s)
                # Successful delivery → return.
                return
            except grpc.RpcError as exc:
                attempts += 1
                elapsed = time.monotonic() - start

                logger.warning(
                    "gRPC SendPromise attempt %d failed for addr=%s "
                    "(node=%s simproc=%s seqnr=%s, elapsed=%.3fs): %s",
                    attempts,
                    addr,
                    envelope.target_node,
                    envelope.target_simproc,
                    envelope.seqnr,
                    elapsed,
                    exc,
                )

                # Check if we should stop retrying.
                if elapsed >= max_window or attempts >= len(delays):
                    # Final failure: mark Worker as BROKEN and re-raise.
                    self._mark_worker_broken(addr, envelope, exc, attempts, elapsed)
                    raise

                # Sleep for the configured backoff delay, then retry.
                delay = delays[attempts - 1]
                time.sleep(delay)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _resolve_address(self, repid: str, node: str) -> str:
        """
        Resolve (repid, node) to a worker address using Cluster.address_book.
        """
        try:
            addr = self._cluster.address_book[(repid, node)]
        except KeyError:
            msg = f"No address for (repid={repid!r}, node={node!r}) in Cluster.address_book"
            logger.error(msg)
            raise KeyError(msg)
        return addr

    def _get_or_create_endpoint(self, addr: str) -> _RemoteEndpoint:
        endpoint = self._endpoints.get(addr)
        if endpoint is not None:
            return endpoint

        channel = self._channel_factory(addr, self._settings)
        stub = self._stub_factory(channel)
        endpoint = _RemoteEndpoint(address=addr, channel=channel, stub=stub)
        self._endpoints[addr] = endpoint

        logger.info("GrpcTransport created new endpoint for addr=%s", addr)
        return endpoint

    def _mark_worker_broken(
        self,
        addr: str,
        envelope: PromiseEnvelope,
        exc: BaseException,
        attempts: int,
        elapsed_s: float,
    ) -> None:
        """
        Log a persistent delivery failure and transition the worker to BROKEN.
        """
        logger.error(
            "Persistent gRPC SendPromise failure for addr=%s "
            "(node=%s simproc=%s seqnr=%s, attempts=%d, elapsed=%.3fs). "
            "Marking worker BROKEN. Error: %s",
            addr,
            envelope.target_node,
            envelope.target_simproc,
            envelope.seqnr,
            attempts,
            elapsed_s,
            exc,
        )
        try:
            self._cluster.set_worker_state(addr, WorkerState.BROKEN)
        except Exception:
            logger.exception(
                "Failed to set worker state to BROKEN for addr=%s after gRPC failure",
                addr,
            )


# ---------------------------------------------------------------------- #
# Default factories
# ---------------------------------------------------------------------- #

def _default_channel_factory(address: str, settings: GrpcSettings) -> grpc.Channel:
    """
    Create a gRPC channel with basic options derived from GrpcSettings.

    The given `address` is expected to be of the form "host:port".
    """
    options = []

    if settings.max_send_message_bytes is not None:
        options.append(("grpc.max_send_message_length", settings.max_send_message_bytes))
    if settings.max_receive_message_bytes is not None:
        options.append(("grpc.max_receive_message_length", settings.max_receive_message_bytes))

    compression = grpc.Compression.NoCompression
    if settings.compression == "gzip":
        compression = grpc.Compression.Gzip

    channel = grpc.insecure_channel(address, options=options, compression=compression)
    return channel


def _default_stub_factory(channel: grpc.Channel) -> transport_pb2_grpc.DiscoTransportStub:
    return transport_pb2_grpc.DiscoTransportStub(channel)
