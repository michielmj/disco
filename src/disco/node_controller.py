from __future__ import annotations

"""
NodeController placeholder implementation.

Responsibilities (ENGINEERING_SPEC):

- Own all simprocs for a single node.
- Provide the public send API used by model code:
    - send_event(...)
    - send_promise(...)
- Decide whether a target is local or remote:
    - Local → deliver directly to local queues (not implemented yet).
    - Remote → build EventEnvelope / PromiseEnvelope and delegate to WorkerRouter.
- Provide ingress hooks for transports:
    - receive_event(envelope)
    - receive_promise(envelope)
- Expose a step() method used by the Worker runner to advance the node.

This module intentionally does NOT implement:
- EventQueue internals.
- Simproc execution.
Those will be added later; for now, NodeController stores incoming
envelopes in simple lists to keep the system testable.
"""

from typing import Any, Callable, Dict, List

from tools.mp_logging import getLogger

from .envelopes import EventEnvelope, PromiseEnvelope
from .router import WorkerRouter  # adjust import if router lives elsewhere

logger = getLogger(__name__)


class NodeController:
    """
    Controller for a single logical node in the simulation.

    This is a placeholder / skeleton implementation:

    - Outgoing events:
        - serialize payload to bytes
        - resolve "self/<simproc>" targets
        - route via WorkerRouter (remote) or deliver locally (TODO)
    - Incoming events/promises:
        - appended to internal lists
        - step() currently just logs and no-ops (no EventQueue yet)

    Once the EventQueue and simproc execution are implemented, the
    receive_* and step() methods should be adapted to use those.
    """

    def __init__(
        self,
        node_name: str,
        expid: str,
        repid: str,
        partition: int,
        router: WorkerRouter,
        serializer: Callable[[Any], bytes] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        node_name:
            Logical name of this node (must match names used in Cluster
            and address_book).
        router:
            WorkerRouter used for routing non-local envelopes.
        serializer:
            Callable for serializing event payloads to bytes. Defaults
            to pickle.dumps or any other application-wide default that
            the caller chooses.
        """
        self._node_name = node_name
        self._expid = expid
        self._repid = repid
        self._partition = partition
        self._router = router
        self._serializer = serializer or self._default_serializer

        # Simple placeholder buffers for inbound messages.
        # Later, these will be replaced by a proper EventQueue.
        self._pending_events: List[EventEnvelope] = []
        self._pending_promises: List[PromiseEnvelope] = []

        logger.info("NodeController created for node=%s repid=%s", node_name, repid)

    # ------------------------------------------------------------------ #
    # Properties
    # ------------------------------------------------------------------ #

    @property
    def node_name(self) -> str:
        return self._node_name

    # ------------------------------------------------------------------ #
    # Public send API (used by model / simprocs)
    # ------------------------------------------------------------------ #

    def send_event(
        self,
        target: str,                # "<node>/<simproc>" or "self/<simproc>"
        epoch: float,
        data: Any,
        headers: Dict[str, str] | None = None,
    ) -> None:
        """
        Send an event to another simproc (possibly on another node).

        - target: "<node>/<simproc>" or "self/<simproc>"
        - epoch: simulation time
        - data: arbitrary payload, serialized to bytes
        - headers: optional string-keyed metadata
        """
        target_node, target_simproc = self._parse_target(target)
        if target_node == "self":
            target_node = self._node_name

        payload = self._serializer(data)
        envelope = EventEnvelope(
            repid=self._repid,
            target_node=target_node,
            target_simproc=target_simproc,
            epoch=epoch,
            data=payload,
            headers=headers or {},
        )

        if target_node == self._node_name:
            # Local delivery (same node / same worker).
            self._deliver_local_event(envelope)
        else:
            # Remote delivery via router.
            self._router.send_event(envelope)

    def send_promise(
        self,
        target: str,                # "<node>/<simproc>" or "self/<simproc>"
        seqnr: int,
        epoch: float,
        num_events: int,
    ) -> None:
        """
        Send a promise for a sequence of events.

        - target: "<node>/<simproc>" or "self/<simproc>"
        - seqnr: sequence number (monotonic per sender/target pair)
        - epoch: simulation time of the promise
        - num_events: number of events covered by this promise
        """
        target_node, target_simproc = self._parse_target(target)
        if target_node == "self":
            target_node = self._node_name

        envelope = PromiseEnvelope(
            repid=self._repid,
            target_node=target_node,
            target_simproc=target_simproc,
            seqnr=seqnr,
            epoch=epoch,
            num_events=num_events,
        )

        if target_node == self._node_name:
            # Local delivery (same node / same worker).
            self._deliver_local_promise(envelope)
        else:
            # Remote delivery via router.
            self._router.send_promise(envelope)

    # ------------------------------------------------------------------ #
    # Ingress API (used by transports / Worker)
    # ------------------------------------------------------------------ #

    def receive_event(self, envelope: EventEnvelope) -> None:
        """
        Ingress hook for transports: deliver a remote or IPC event.

        For now, we just store the envelope in a buffer. In the future,
        this will enqueue into an EventQueue.
        """
        logger.debug(
            "NodeController[%s] received event: simproc=%s epoch=%s",
            self._node_name,
            envelope.target_simproc,
            envelope.epoch,
        )
        self._pending_events.append(envelope)

    def receive_promise(self, envelope: PromiseEnvelope) -> None:
        """
        Ingress hook for transports: deliver a remote or IPC promise.

        For now, we just store the envelope in a buffer. In the future,
        this will enqueue into an EventQueue with strict ordering rules.
        """
        logger.debug(
            "NodeController[%s] received promise: simproc=%s seqnr=%s epoch=%s num_events=%s",
            self._node_name,
            envelope.target_simproc,
            envelope.seqnr,
            envelope.epoch,
            envelope.num_events,
        )
        self._pending_promises.append(envelope)

    # ------------------------------------------------------------------ #
    # Runner hook
    # ------------------------------------------------------------------ #

    def step(self) -> None:
        """
        Called by the Worker runner in ACTIVE state.

        Placeholder behavior:
        - Logs how many pending events/promises exist.
        - Does not execute any simulation logic.
        - Clears the buffers to avoid unbounded growth.

        Once EventQueue and simprocs are implemented, this method should:
        - Drain a bounded portion of its queue(s).
        - Execute simproc logic for the node.
        """
        if not self._pending_events and not self._pending_promises:
            return

        logger.debug(
            "NodeController[%s] step: %d pending promises, %d pending events "
            "(no EventQueue implemented yet)",
            self._node_name,
            len(self._pending_promises),
            len(self._pending_events),
        )

        # Placeholder: discard everything.
        self._pending_promises.clear()
        self._pending_events.clear()

    # ------------------------------------------------------------------ #
    # Local delivery helpers
    # ------------------------------------------------------------------ #

    def _deliver_local_event(self, envelope: EventEnvelope) -> None:
        """
        Deliver an event to this node's local queue.

        Currently routed to receive_event() directly. Once a proper
        EventQueue is implemented, this should enqueue instead of
        calling receive_event.
        """
        self.receive_event(envelope)

    def _deliver_local_promise(self, envelope: PromiseEnvelope) -> None:
        """
        Deliver a promise to this node's local queue.

        Currently routed to receive_promise() directly. Once a proper
        EventQueue is implemented, this should enqueue instead of
        calling receive_promise.
        """
        self.receive_promise(envelope)

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_target(target: str) -> tuple[str, str]:
        """
        Split a target string "<node>/<simproc>" into components.

        Raises ValueError if the format is invalid.
        """
        try:
            node, simproc = target.split("/", 1)
        except ValueError as exc:
            raise ValueError(
                f"Invalid target format {target!r}; expected '<node>/<simproc>'"
            ) from exc
        if not node or not simproc:
            raise ValueError(
                f"Invalid target {target!r}; node and simproc must be non-empty"
            )
        return node, simproc

    @staticmethod
    def _default_serializer(value: Any) -> bytes:
        """
        Default serializer used when none is provided.

        Intentionally kept trivial here; the application is expected to
        inject a real serializer (e.g. pickle, msgpack, custom).
        """
        import pickle

        return pickle.dumps(value)
