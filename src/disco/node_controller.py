from __future__ import annotations

"""NodeController skeleton responsible for serialization and routing."""

from dataclasses import dataclass, field
import pickle
from typing import Any, Callable

from .envelopes import EventEnvelope, PromiseEnvelope
from .router import Router


@dataclass(slots=True)
class NodeController:
    """Manage sending and receiving events and promises for a node."""

    node_name: str
    router: Router
    _serializer: Callable[[Any], bytes] = field(default=pickle.dumps)
    _deserializer: Callable[[bytes], Any] = field(default=pickle.loads)

    def __post_init__(self) -> None:
        self.router.register_node(self)

    def send_event(
        self,
        target: str,
        epoch: float,
        data: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send an event to ``target`` ("<node>/<simproc>" or ``"self"`` alias)."""

        target_node, target_simproc = self._parse_target(target)
        envelope = EventEnvelope(
            target_node=target_node,
            target_simproc=target_simproc,
            epoch=epoch,
            data=data,
            headers=headers or {},
        )
        if target_node == self.node_name:
            self._deliver_local_event(envelope)
        else:
            payload = self._serialize_event_envelope(envelope)
            self.router.route_event(target_node, payload)

    def send_promise(self, target: str, seqnr: int, epoch: float, num_events: int) -> None:
        """Send a promise to ``target`` ("<node>/<simproc>" or ``"self"`` alias)."""

        target_node, target_simproc = self._parse_target(target)
        envelope = PromiseEnvelope(
            target_node=target_node,
            target_simproc=target_simproc,
            seqnr=seqnr,
            epoch=epoch,
            num_events=num_events,
        )
        if target_node == self.node_name:
            self._deliver_local_promise(envelope)
        else:
            payload = self._serialize_promise_envelope(envelope)
            self.router.route_promise(target_node, payload)

    def receive_event(self, payload: bytes) -> None:
        """Receive a serialized event from a transport or router."""

        envelope = self._deserialize_event_envelope(payload)
        self._deliver_local_event(envelope)

    def receive_promise(self, payload: bytes) -> None:
        """Receive a serialized promise from a transport or router."""

        envelope = self._deserialize_promise_envelope(payload)
        self._deliver_local_promise(envelope)

    def _parse_target(self, target: str) -> tuple[str, str]:
        if "/" not in target:
            raise ValueError("target must be in '<node>/<simproc>' format")
        target_node, target_simproc = target.split("/", maxsplit=1)
        resolved_node = self.node_name if target_node == "self" else target_node
        if not target_simproc:
            raise ValueError("simproc must be provided")
        return resolved_node, target_simproc

    def _serialize_event_envelope(self, envelope: EventEnvelope) -> bytes:
        return self._serializer(envelope)

    def _serialize_promise_envelope(self, envelope: PromiseEnvelope) -> bytes:
        return self._serializer(envelope)

    def _deserialize_event_envelope(self, payload: bytes) -> EventEnvelope:
        envelope = self._deserializer(payload)
        if not isinstance(envelope, EventEnvelope):
            raise TypeError("deserialized payload is not EventEnvelope")
        return envelope

    def _deserialize_promise_envelope(self, payload: bytes) -> PromiseEnvelope:
        envelope = self._deserializer(payload)
        if not isinstance(envelope, PromiseEnvelope):
            raise TypeError("deserialized payload is not PromiseEnvelope")
        return envelope

    def _deliver_local_event(self, envelope: EventEnvelope) -> None:
        # Implemented in later iterations.
        return

    def _deliver_local_promise(self, envelope: PromiseEnvelope) -> None:
        # Implemented in later iterations.
        return
