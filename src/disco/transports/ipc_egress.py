from __future__ import annotations

"""IPC transport for sending envelopes to peer processes."""

from multiprocessing import Queue
from multiprocessing.shared_memory import SharedMemory
from typing import Mapping, Optional

from ..cluster import Cluster
from ..envelopes import EventEnvelope, PromiseEnvelope
from .base import Transport
from .ipc_messages import IPCEventMsg, IPCPromiseMsg


class IPCTransport(Transport):
    """Transport that delivers envelopes via queues and shared memory."""
    def __init__(
        self,
        cluster: Cluster,
        event_queues: Mapping[str, Queue[IPCEventMsg]],
        promise_queues: Mapping[str, Queue[IPCPromiseMsg]],
        large_payload_threshold: int = 64 * 1024,
    ) -> None:
        self._cluster = cluster
        self._event_queues = event_queues
        self._promise_queues = promise_queues
        self._large_payload_threshold = large_payload_threshold
        self._last_repid: Optional[str] = None

    def handles_node(self, repid: str, node: str) -> bool:
        addr = self._cluster.address_book.get((repid, node))
        if addr is None:
            return False
        can_handle = addr in self._event_queues and addr in self._promise_queues
        if can_handle:
            self._last_repid = repid
        return can_handle

    def send_event(self, envelope: EventEnvelope) -> None:
        repid = self._require_repid()
        addr = self._cluster.address_book[(repid, envelope.target_node)]
        queue = self._event_queues[addr]
        if len(envelope.data) <= self._large_payload_threshold:
            msg = IPCEventMsg(
                target_node=envelope.target_node,
                target_simproc=envelope.target_simproc,
                epoch=envelope.epoch,
                headers=envelope.headers,
                data=envelope.data,
                shm_name=None,
                size=len(envelope.data),
            )
        else:
            shm = SharedMemory(create=True, size=len(envelope.data))
            shm.buf[: len(envelope.data)] = envelope.data
            msg = IPCEventMsg(
                target_node=envelope.target_node,
                target_simproc=envelope.target_simproc,
                epoch=envelope.epoch,
                headers=envelope.headers,
                data=None,
                shm_name=shm.name,
                size=len(envelope.data),
            )
        queue.put(msg)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        repid = self._require_repid()
        addr = self._cluster.address_book[(repid, envelope.target_node)]
        queue = self._promise_queues[addr]
        msg = IPCPromiseMsg(
            target_node=envelope.target_node,
            target_simproc=envelope.target_simproc,
            seqnr=envelope.seqnr,
            epoch=envelope.epoch,
            num_events=envelope.num_events,
        )
        queue.put(msg)

    def _require_repid(self) -> str:
        if self._last_repid is None:
            raise KeyError("Replication id not set before send")
        return self._last_repid
