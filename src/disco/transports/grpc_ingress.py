from __future__ import annotations

"""
gRPC ingress service for receiving envelopes from remote workers.

Implements the DiscoTransport service defined in
src/disco/transports/proto/transport.proto and delivers incoming
EventEnvelopeMsg / PromiseEnvelopeMsg into the local IPC queues
(event_queue, promise_queue).

The Worker is responsible for:
- Exposing its current WorkerState.
- Running IPCReceiver loops (in the runner) that consume from the local
  event/promise queues and deliver to NodeControllers.
- Prioritising promise delivery over events in the runner loop.
"""

from concurrent import futures
from multiprocessing import Queue
from typing import TYPE_CHECKING

import grpc

from tools.mp_logging import getLogger

from ..cluster import WorkerState
from ..config import GrpcSettings
from .ipc_messages import IPCEventMsg, IPCPromiseMsg
from .proto import transport_pb2, transport_pb2_grpc

if TYPE_CHECKING:
    from ..worker import Worker

logger = getLogger(__name__)


class DiscoTransportServicer(transport_pb2_grpc.DiscoTransportServicer):
    """
    gRPC servicer that forwards incoming messages to local IPC queues.

    The Worker is assumed to expose:
        - address: str  (e.g. "host:port")
        - state: WorkerState

    The servicer itself only:
        - Gates ingress on WorkerState (READY / ACTIVE / PAUSED).
        - Converts protobuf messages to IPCEventMsg / IPCPromiseMsg.
        - Puts them on the provided multiprocessing queues.
    """

    def __init__(
        self,
        worker: Worker,
        event_queue: Queue[IPCEventMsg],
        promise_queue: Queue[IPCPromiseMsg],
    ) -> None:
        self._worker = worker
        self._event_queue = event_queue
        self._promise_queue = promise_queue

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _check_ingress_allowed(self, context: grpc.ServicerContext) -> None:
        """
        Ensure that the Worker is in a state that accepts ingress.

        Allowed: READY, ACTIVE, PAUSED.
        Others: abort with FAILED_PRECONDITION.
        """
        state = self._worker.state
        if state not in (WorkerState.READY, WorkerState.ACTIVE, WorkerState.PAUSED):
            msg = f"Worker not accepting ingress in state={state.name}"
            logger.warning("Ingress rejected: %s", msg)
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, msg)

    # ------------------------------------------------------------------ #
    # RPC methods
    # ------------------------------------------------------------------ #

    def SendEvents(self, request_iterator, context):
        """
        Client-streaming RPC for events.

        We:
        - Check that ingress is allowed.
        - For each EventEnvelopeMsg:
            - Convert to IPCEventMsg (inline payload only; no SharedMemory).
            - Put onto the local event_queue.

        Any exception while enqueueing aborts the RPC with an INTERNAL
        error, making the failure visible to the sender.
        """
        self._check_ingress_allowed(context)

        count = 0
        try:
            for msg in request_iterator:
                headers = dict(msg.headers)
                data = bytes(msg.data)  # ensure bytes type
                ipc_msg = IPCEventMsg(
                    repid=msg.repid,
                    target_node=msg.target_node,
                    target_simproc=msg.target_simproc,
                    epoch=msg.epoch,
                    headers=headers,
                    data=data,
                    shm_name=None,
                    size=len(data),
                )

                logger.debug(
                    "Ingress gRPC event: node=%s simproc=%s epoch=%s size=%d",
                    ipc_msg.target_node,
                    ipc_msg.target_simproc,
                    ipc_msg.epoch,
                    ipc_msg.size,
                )

                self._event_queue.put(ipc_msg)
                count += 1

        except Exception as exc:
            logger.exception("Error while ingesting events via gRPC: %s", exc)
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"Error ingesting events: {exc!r}",
            )

        return transport_pb2.TransportAck(message=f"Received {count} events")

    def SendPromise(self, request, context):
        """
        Unary RPC for promises.

        We:
        - Check that ingress is allowed.
        - Convert PromiseEnvelopeMsg to IPCPromiseMsg.
        - Put onto the local promise_queue.
        - Return a TransportAck on success.

        If enqueueing fails (e.g. queue full), we abort the RPC with
        RESOURCE_EXHAUSTED so that the sender-side GrpcTransport can
        apply its retry policy.
        """
        self._check_ingress_allowed(context)

        ipc_msg = IPCPromiseMsg(
            repid=request.repid,
            target_node=request.target_node,
            target_simproc=request.target_simproc,
            seqnr=request.seqnr,
            epoch=request.epoch,
            num_events=request.num_events,
        )

        logger.debug(
            "Ingress gRPC promise: node=%s simproc=%s seqnr=%s epoch=%s num_events=%s",
            ipc_msg.target_node,
            ipc_msg.target_simproc,
            ipc_msg.seqnr,
            ipc_msg.epoch,
            ipc_msg.num_events,
        )

        try:
            self._promise_queue.put(ipc_msg)
        except Exception as exc:
            logger.exception("Error while ingesting promise via gRPC: %s", exc)
            context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                f"Error ingesting promise: {exc!r}",
            )

        return transport_pb2.TransportAck(message="Promise accepted")


# ---------------------------------------------------------------------- #
# Server bootstrap helper
# ---------------------------------------------------------------------- #

def start_grpc_server(
    worker: Worker,
    event_queue: Queue[IPCEventMsg],
    promise_queue: Queue[IPCPromiseMsg],
    settings: GrpcSettings,
) -> grpc.Server:
    """
    Create and start a gRPC server for the given Worker and settings.

    - Binds to the worker's address (worker.address, e.g. "host:port").
    - Uses settings.max_workers for the gRPC thread pool.
    - Applies send/receive size limits and compression from GrpcSettings.

    Returns the started grpc.Server instance; the caller is responsible
    for managing its lifecycle (e.g. graceful shutdown).
    """
    options: list[tuple[str, object]] = []

    if settings.max_send_message_bytes is not None:
        options.append(("grpc.max_send_message_length", settings.max_send_message_bytes))
    if settings.max_receive_message_bytes is not None:
        options.append(("grpc.max_receive_message_length", settings.max_receive_message_bytes))

    # NOTE: keepalive_* settings would typically be configured via channel
    # options on the client side. Server-side keepalive configuration is
    # left as future work if needed.

    compression = grpc.Compression.NoCompression
    if settings.compression == "gzip":
        compression = grpc.Compression.Gzip

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=settings.max_workers),
        options=options,
        compression=compression,
    )

    transport_pb2_grpc.add_DiscoTransportServicer_to_server(
        DiscoTransportServicer(worker, event_queue, promise_queue),
        server,
    )

    # Bind to the worker's logical address, e.g. "0.0.0.0:50051" or "host:port".
    bind_addr = worker.address
    server.add_insecure_port(bind_addr)

    logger.info("Starting gRPC DiscoTransport server for worker %s on %s", worker.address, bind_addr)
    server.start()
    return server
