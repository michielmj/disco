from __future__ import annotations

"""In-process transport implementation."""

from dataclasses import dataclass
from typing import Mapping

from ..node_controller import NodeController
from .base import Transport


@dataclass(slots=True)
class InProcessTransport(Transport):
    """Transport that directly invokes peer :class:`NodeController`s."""

    peers: Mapping[str, NodeController]

    def handles_node(self, node_name: str) -> bool:
        return node_name in self.peers

    def send_event(self, target_node: str, payload: bytes) -> None:
        peer = self._resolve_peer(target_node)
        peer.receive_event(payload)

    def send_promise(self, target_node: str, payload: bytes) -> None:
        peer = self._resolve_peer(target_node)
        peer.receive_promise(payload)

    def _resolve_peer(self, target_node: str) -> NodeController:
        if target_node not in self.peers:
            raise ValueError(f"unknown peer node {target_node}")
        return self.peers[target_node]
