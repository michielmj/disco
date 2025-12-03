from __future__ import annotations

"""Routing layer for delivering envelopes via registered transports."""

from dataclasses import dataclass
from typing import Iterable, Mapping

from .cluster import Cluster
from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.base import Transport


@dataclass(slots=True)
class ServerRouter:
    """Choose transports for outbound envelopes based on cluster metadata."""

    cluster: Cluster
    transports: list[Transport]
    repid: str

    def _choose_transport(self, target_node: str) -> Transport:
        for transport in self.transports:
            if transport.handles_node(self.repid, target_node):
                return transport
        raise KeyError(
            f"No transport can handle node {target_node!r} for repid {self.repid}"
        )

    def send_event(self, envelope: EventEnvelope) -> None:
        transport = self._choose_transport(envelope.target_node)
        transport.send_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        transport = self._choose_transport(envelope.target_node)
        transport.send_promise(envelope)


@dataclass(slots=True)
class LocalNodeRegistry:
    """A simple registry of locally reachable nodes."""

    nodes: Mapping[str, object]

    def known_nodes(self) -> Iterable[str]:
        return self.nodes.keys()
