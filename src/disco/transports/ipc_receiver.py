from __future__ import annotations

"""Receiver loops for IPC queues."""

from multiprocessing import Queue
from multiprocessing.shared_memory import SharedMemory
from typing import Mapping

from ..envelopes import EventEnvelope, PromiseEnvelope
from ..node_controller import NodeController
from .ipc_messages import IPCEventMsg, IPCPromiseMsg


class IPCReceiver:
    def __init__(
        self,
        nodes: Mapping[str, NodeController],
        event_queue: Queue[IPCEventMsg],
        promise_queue: Queue[IPCPromiseMsg],
    ) -> None:
        self._nodes = nodes
        self._event_queue = event_queue
        self._promise_queue = promise_queue

    def run_event_loop(self) -> None:
        while True:
            msg: IPCEventMsg = self._event_queue.get()
            self._process_event(msg)

    def run_promise_loop(self) -> None:
        while True:
            msg: IPCPromiseMsg = self._promise_queue.get()
            self._process_promise(msg)

    def _process_event(self, msg: IPCEventMsg) -> None:
        data = self._extract_event_data(msg)
        envelope = EventEnvelope(
            target_node=msg.target_node,
            target_simproc=msg.target_simproc,
            epoch=msg.epoch,
            headers=msg.headers,
            data=data,
        )
        node = self._nodes.get(msg.target_node)
        if node is None:
            raise KeyError(msg.target_node)
        node.receive_event(envelope)

    def _process_promise(self, msg: IPCPromiseMsg) -> None:
        envelope = PromiseEnvelope(
            target_node=msg.target_node,
            target_simproc=msg.target_simproc,
            seqnr=msg.seqnr,
            epoch=msg.epoch,
            num_events=msg.num_events,
        )
        node = self._nodes.get(msg.target_node)
        if node is None:
            raise KeyError(msg.target_node)
        node.receive_promise(envelope)

    def _extract_event_data(self, msg: IPCEventMsg) -> bytes:
        if msg.shm_name is None:
            if msg.data is None:
                raise ValueError("IPCEventMsg missing payload")
            return msg.data
        shm = SharedMemory(name=msg.shm_name)
        try:
            data = bytes(shm.buf[: msg.size])
        finally:
            shm.close()
            shm.unlink()
        return data
