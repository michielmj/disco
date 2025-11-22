from __future__ import annotations

"""Transport interfaces."""

from abc import ABC, abstractmethod


class Transport(ABC):
    """Abstract transport for sending serialized envelopes."""

    @abstractmethod
    def handles_node(self, node_name: str) -> bool:
        """Return whether this transport can reach ``node_name``."""

    @abstractmethod
    def send_event(self, target_node: str, payload: bytes) -> None:
        """Send a serialized event payload to ``target_node``."""

    @abstractmethod
    def send_promise(self, target_node: str, payload: bytes) -> None:
        """Send a serialized promise payload to ``target_node``."""
