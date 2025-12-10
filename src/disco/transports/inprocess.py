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
    cluster: Cluster

    def handles_node(self, repid: str, node: str) -> bool:
        if node not in self.nodes:
            return False
        return (repid, node) in self.cluster.address_book

    def send_event(self, repid: str, envelope: EventEnvelope) -> None:
        try:
            node = self.nodes[envelope.target_node]
        except KeyError as exc:
            # This is a serious internal misconfig → likely BROKEN worker.
            raise KeyError(f"InProcessTransport: unknown node {envelope.target_node!r}") from exc
        node.receive_event(envelope)

    def send_promise(self, repid: str, envelope: PromiseEnvelope) -> None:
        try:
            node = self.nodes[envelope.target_node]
        except KeyError as exc:
            # This is a serious internal misconfig → likely BROKEN worker.
            raise KeyError(f"InProcessTransport: unknown node {envelope.target_node!r}") from exc
        node.receive_promise(envelope)
