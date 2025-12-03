from __future__ import annotations

"""Transport protocol for routing envelopes."""

from typing import Protocol

from ..envelopes import EventEnvelope, PromiseEnvelope


class Transport(Protocol):
    """Protocol for transports capable of delivering envelopes."""

    def handles_node(self, repid: str, node: str) -> bool:
        """Return whether this transport can reach ``(repid, node)``."""

    def send_event(self, envelope: EventEnvelope) -> None:
        """Send an :class:`~disco.envelopes.EventEnvelope`."""

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        """Send a :class:`~disco.envelopes.PromiseEnvelope`."""
