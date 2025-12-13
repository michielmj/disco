from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence

from tools.mp_logging import getLogger
from .cluster import Cluster
from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.base import Transport  # Protocol

logger = getLogger(__name__)


class RouterError(Exception):
    """Base exception for routing errors."""
    pass


@dataclass(slots=True)
class TransportInfo:
    """
    Simple wrapper to keep some metadata about transports.

    For now this is mostly useful for debugging and introspection; the
    Router uses the original order of the transports list to determine
    priority (InProcess -> IPC -> gRPC).
    """
    transport: Transport
    name: str


class WorkerRouter:
    """
    Worker-local router that selects a transport for each envelope.

    Responsibilities (ENGINEERING_SPEC chapter 6):

    - Own an ordered list of Transports (InProcess, IPC, Grpc, ...).
    - For each outgoing envelope:
        - Ask each transport in order whether it handles the target node
          for the current replication id.
        - Use the first matching transport.
        - Raise RouterError if no transport can handle the node.

    The Worker is responsible for configuring the router with transports
    in the correct priority order.
    """

    def __init__(
        self,
        cluster: Cluster,
        transports: Sequence[Transport],
    ) -> None:
        """
        Parameters
        ----------
        cluster:
            Cluster instance providing the address_book and locality info.
            The Router itself does not look directly at the metastore.
        transports:
            Ordered sequence of Transport instances. The first transport
            whose `handles_node(repid, node)` returns True will be used
        """
        self._cluster = cluster

        # Wrap transports with a name for debugging; fall back to class name.
        self._transports: List[TransportInfo] = []
        for t in transports:
            name = getattr(t, "name", t.__class__.__name__)
            self._transports.append(TransportInfo(transport=t, name=name))

        logger.info(
            "WorkerRouter created with transports=%s",
            [ti.name for ti in self._transports],
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def send_event(self, envelope: EventEnvelope) -> None:
        """
        Route an EventEnvelope via the first Transport that claims to
        handle the target node for the current repid.
        """
        transport = self._choose_transport(envelope.repid, envelope.target_node)
        logger.debug(
            "Routing event to node=%s simproc=%s via transport=%s repid=%s",
            envelope.target_node,
            envelope.target_simproc,
            transport.name,
            envelope.repid,
        )
        transport.transport.send_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        """
        Route a PromiseEnvelope via the first Transport that claims to
        handle the target node for the current repid.
        """
        transport = self._choose_transport(envelope.repid, envelope.target_node)
        logger.debug(
            "Routing promise to node=%s simproc=%s seqnr=%s via transport=%s repid=%s",
            envelope.target_node,
            envelope.target_simproc,
            envelope.seqnr,
            transport.name,
            envelope.repid,
        )
        transport.transport.send_promise(envelope)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _choose_transport(self, repid: str, target_node: str) -> TransportInfo:
        """
        Select the first transport that can handle the given node.

        Raises
        ------
        RouterError
            If no transport can handle the target node for this repid.
        """
        # Optional: we can sanity-check that the node exists in the
        # address_book here, but we leave that to transports so that
        # purely in-process setups without address_book entries can work.

        for info in self._transports:
            try:
                if info.transport.handles_node(repid, target_node):
                    return info
            except Exception as exc:
                # A transport throwing in handles_node is considered a bug
                # in that transport; we log and continue to the next one.
                logger.exception(
                    "Transport %s.handles_node(repid=%r, node=%r) raised: %r",
                    info.name,
                    repid,
                    target_node,
                    exc,
                )

        # No transport claimed responsibility; this is a configuration error.
        msg = (
            f"No transport available for node={target_node!r}, "
            f"repid={repid!r}. Known transports={ [ti.name for ti in self._transports] }"
        )
        logger.error(msg)
        raise RouterError(msg)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #

    def transports(self) -> Iterable[Transport]:
        """
        Return the underlying Transport instances in priority order.
        """
        return (ti.transport for ti in self._transports)

    def transport_names(self) -> list[str]:
        """
        Return the list of transport names in priority order.
        """
        return [ti.name for ti in self._transports]
