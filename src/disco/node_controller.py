"""NodeController responsible for serialization and routing."""

from __future__ import annotations

from dataclasses import dataclass, field
import pickle
from typing import Any, Callable

from .envelopes import EventEnvelope, PromiseEnvelope
from .router import ServerRouter


def _noop_event(_: EventEnvelope) -> None:
    """Default no-op for local event handling."""


def _noop_promise(_: PromiseEnvelope) -> None:
    """Default no-op for local promise handling."""


@dataclass(slots=True)
class NodeController:
    """Manage sending and receiving events and promises for a node."""

    node_name: str
    router: ServerRouter
    serializer: Callable[[Any], bytes] = field(default=pickle.dumps)
    _deliver_local_event: Callable[[EventEnvelope], None] = field(
        default=_noop_event, repr=False
    )
    _deliver_local_promise: Callable[[PromiseEnvelope], None] = field(
        default=_noop_promise, repr=False
    )

    def __post_init__(self) -> None:
        pass

    def send_event(
        self,
        target: str,
        epoch: float,
        data: Any,
        headers: dict[str, str] | None = None,
    ) -> None:
        target_node, target_simproc = self._parse_target(target)
        payload = self.serializer(data)
        envelope = EventEnvelope(
            target_node=target_node,
            target_simproc=target_simproc,
            epoch=epoch,
            data=payload,
            headers=headers or {},
        )
        if target_node == self.node_name:
            self._deliver_local_event(envelope)
        else:
            self.router.send_event(envelope)

    def send_promise(self, target: str, seqnr: int, epoch: float, num_events: int) -> None:
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
            self.router.send_promise(envelope)

    def receive_event(self, envelope: EventEnvelope) -> None:
        self._deliver_local_event(envelope)

    def receive_promise(self, envelope: PromiseEnvelope) -> None:
        self._deliver_local_promise(envelope)

    def _parse_target(self, target: str) -> tuple[str, str]:
        if "/" not in target:
            raise ValueError("target must be in '<node>/<simproc>' format")
        target_node, target_simproc = target.split("/", maxsplit=1)
        resolved_node = self.node_name if target_node == "self" else target_node
        if not target_simproc:
            raise ValueError("simproc must be provided")
        return resolved_node, target_simproc
