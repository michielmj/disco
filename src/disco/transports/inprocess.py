from __future__ import annotations

"""In-process transport that routes to local NodeControllers."""

from dataclasses import dataclass
from typing import Mapping

from ..cluster import Cluster
from ..envelopes import EventEnvelope, PromiseEnvelope
from ..node_controller import NodeController
from .base import Transport


@dataclass(slots=True)
class InProcessTransport(Transport):
    """Deliver envelopes to NodeControllers registered in the same process."""

    nodes: Mapping[str, NodeController]
    repid: str
    cluster: Cluster

    def handles_node(self, repid: str, node: str) -> bool:
        if node not in self.nodes:
            return False
        return (repid, node) in self.cluster.address_book

    def send_event(self, envelope: EventEnvelope) -> None:
        node = self.nodes[envelope.target_node]
        node.receive_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        node = self.nodes[envelope.target_node]
        node.receive_promise(envelope)
