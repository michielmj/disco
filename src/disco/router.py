from __future__ import annotations

"""Routing layer for delivering envelopes to local or remote nodes."""

from dataclasses import dataclass, field
from typing import Dict, Iterable, TYPE_CHECKING

from .transports.base import Transport

if TYPE_CHECKING:
    from .node_controller import NodeController


@dataclass(slots=True)
class Router:
    """Routes serialized envelopes to local nodes or transports."""

    _transports: list[Transport] = field(default_factory=list)
    _nodes: Dict[str, NodeController] = field(default_factory=dict)

    def register_node(self, controller: NodeController) -> None:
        """Register a locally reachable :class:`NodeController`.

        Raises:
            ValueError: if a node with the same name is already registered.
        """

        if controller.node_name in self._nodes:
            raise ValueError(f"node {controller.node_name} already registered")
        self._nodes[controller.node_name] = controller

    def register_transport(self, transport: Transport) -> None:
        """Register a transport used for remote delivery."""

        self._transports.append(transport)

    def known_nodes(self) -> Iterable[str]:
        """Return names of nodes registered locally."""

        return self._nodes.keys()

    def route_event(self, target_node: str, payload: bytes) -> None:
        """Route a serialized event envelope to the appropriate destination."""

        if target_node in self._nodes:
            self._nodes[target_node].receive_event(payload)
            return
        self._route_remote("event", target_node, payload)

    def route_promise(self, target_node: str, payload: bytes) -> None:
        """Route a serialized promise envelope to the appropriate destination."""

        if target_node in self._nodes:
            self._nodes[target_node].receive_promise(payload)
            return
        self._route_remote("promise", target_node, payload)

    def _route_remote(self, kind: str, target_node: str, payload: bytes) -> None:
        for transport in self._transports:
            if transport.handles_node(target_node):
                if kind == "event":
                    transport.send_event(target_node, payload)
                else:
                    transport.send_promise(target_node, payload)
                return
        raise ValueError(f"no transport for node {target_node}")
