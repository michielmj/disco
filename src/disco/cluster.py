from functools import partial
from threading import Condition, Lock
from dataclasses import dataclass, field
from typing import Mapping

from enum import IntEnum
from types import MappingProxyType

from tools.mp_logging import getLogger
from disco.metastore import Metastore

logger = getLogger(__name__)


class State(IntEnum):
    CREATED = 1
    INITIALIZING = 2
    AVAILABLE = 3
    ALLOCATED = 4
    ACTIVE = 5
    PAUSED = 6
    DISPOSING = 7
    BROKEN = 8


@dataclass
class ServerStatus:
    expid: str | None = None
    repid: str | None = None
    partition: int | None = None
    nodes: list[str] = field(default_factory=list)
    state: State = State.CREATED


PROCESSES = "/simulation/processes"
NPARTITIONS = "/simulation/npartitions"
ACTIVE_SERVERS = "/simulation/active_servers"
SERVERS = "/simulation/servers"
EXPERIMENTS = "/simulation/experiments"
SCHEDULER_QUEUE = "/simulation/queue"
ADDRESS_BOOK = "/simulation/address_book"
NODES = "/simulation/nodes"
RUNS = "/simulation/runs"
MODULES = "/simulation/modules"
PARTITIONINGS = "/simulation/partitionings"
HANDLERS = "/simulation/handlers"
SCHEDULER_ELECTION = "/simulation/scheduler_election"
SCHEDULER_HEALTH = "/simulation/scheduler_health"
PARAM_SETS = "/simulation/param_sets"
LOCKS = "/locks"


BASE_STRUCTURE = [
    PROCESSES,
    NPARTITIONS,
    ACTIVE_SERVERS,
    SERVERS,
    ADDRESS_BOOK,
    NODES,
    RUNS,
    MODULES,
    PARTITIONINGS,
    PARAM_SETS,
    HANDLERS,
    EXPERIMENTS,
    SCHEDULER_QUEUE,
    SCHEDULER_ELECTION,
    SCHEDULER_HEALTH,
    LOCKS,
]


class Cluster:
    def __init__(self, meta: Metastore):
        self.meta = meta
        # Ensure base structure is present
        self.meta.ensure_structure(BASE_STRUCTURE)

        self._lock = Lock()
        # Condition uses the same lock as server_state for consistency
        self._available_condition = Condition(self._lock)

        # Internal state
        self._server_state: dict[str, State] = {}
        self._server_nodes: dict[str, list[str]] = {}
        self._server_repids: dict[str, str] = {}

        self._address_book: dict[tuple[str, str], str] = {}
        self._address_book_uptodate = False

        # Watch active servers list
        self.meta.watch_members_with_callback(ACTIVE_SERVERS, self._watch_children)

    # ------------------------------------------------------------------ #
    # ZooKeeper watch callbacks
    # ------------------------------------------------------------------ #

    def _watch_children(self, children: list[str], _: str) -> bool:
        """
        Children of ACTIVE_SERVERS changed:
        - Remove servers that disappeared.
        - Add new servers and start watching their state/nodes/repid.
        """
        if self.meta.stopped:
            return True  # keep watch, but do nothing

        with self._lock:
            current = set(self._server_state.keys())
            incoming = set(children)

            deletes = current - incoming
            appends = incoming - current

            for address in deletes:
                self._server_state.pop(address, None)
                self._server_nodes.pop(address, None)
                self._server_repids.pop(address, None)
                self._address_book_uptodate = False

            for address in appends:
                # New server becomes AVAILABLE by default
                self._server_state[address] = State.AVAILABLE

                # Watch individual paths (logical paths, no leading "/")
                self.meta.watch_with_callback(
                    f"{SERVERS}/{address}/state",
                    partial(self._watch_server_state, address),
                )
                self.meta.watch_with_callback(
                    f"{SERVERS}/{address}/nodes",
                    partial(self._watch_server_nodes, address),
                )
                self.meta.watch_with_callback(
                    f"{SERVERS}/{address}/repid",
                    partial(self._watch_server_repid, address),
                )

            # Any change might affect address_book and availability
            self._address_book_uptodate = False
            self._available_condition.notify_all()

        return True

    def _watch_server_state(self, address: str, state: State, _path: str) -> bool:
        """
        Called with decoded `state` (State enum) by Metastore.
        """
        if self.meta.stopped:
            return False

        with self._lock:
            if address not in self._server_state:
                # Server has been removed; stop watching
                return False

            self._server_state[address] = state
            self._available_condition.notify_all()

        return True

    def _watch_server_nodes(self, address: str, nodes: list[str] | None, _path: str) -> bool:
        """
        Called with decoded `nodes` (list[str]) by Metastore.
        """
        if self.meta.stopped:
            return False

        with self._lock:
            if address not in self._server_state:
                return False

            self._server_nodes[address] = nodes or []
            self._address_book_uptodate = False

        return True

    def _watch_server_repid(self, address: str, repid: str | None, _path: str) -> bool:
        """
        Called with decoded `repid` (str) by Metastore.
        """
        if self.meta.stopped:
            return False

        with self._lock:
            if address not in self._server_state:
                return False

            self._server_repids[address] = repid or ""
            self._address_book_uptodate = False

        return True

    # ------------------------------------------------------------------ #
    # Public properties
    # ------------------------------------------------------------------ #

    @property
    def address_book(self) -> Mapping[tuple[str, str], str]:
        """
        Mapping (repid, node) -> server address.
        """
        with self._lock:
            if not self._address_book_uptodate:
                address_book: dict[tuple[str, str], str] = {}
                for address, nodes in self._server_nodes.items():
                    repid = self._server_repids.get(address, "")
                    for node in nodes:
                        address_book[(repid, node)] = address

                self._address_book = address_book
                self._address_book_uptodate = True

            # Return a read-only view
            return MappingProxyType(dict(self._address_book))

    @property
    def server_status(self) -> Mapping[str, State]:
        if self.meta.stopped:
            raise RuntimeError("Metastore stopped.")

        with self._lock:
            return MappingProxyType(dict(self._server_state))

    # ------------------------------------------------------------------ #
    # Availability / selection
    # ------------------------------------------------------------------ #

    def await_available(self, timeout: float | None = None) -> bool:
        """
        Wait until a notification that something changed in availability.

        Returns True if notified, False on timeout.
        """
        if self.meta.stopped:
            raise RuntimeError("Metastore stopped.")

        with self._available_condition:
            return self._available_condition.wait(timeout=timeout)

    def get_available(self, expid: str = "") -> tuple[list[str], list[int]]:
        """
        Returns:
          - list of server addresses (preferred first),
          - list of partitions of preferred servers.

        Preferred servers:
          - state == AVAILABLE
          - full_status.expid == expid
          - each partition used at most once in the preferred list
        """
        if self.meta.stopped:
            raise RuntimeError("Metastore already stopped.")

        preferred: list[str] = []
        others: list[str] = []
        partitions: list[int] = []

        with self._lock:
            for address, state in self._server_state.items():
                if state == State.AVAILABLE:
                    full_status = self.load_server_status(address)
                    if (
                        full_status.expid == expid
                        and full_status.partition is not None
                        and full_status.partition not in partitions
                    ):
                        preferred.append(address)
                        partitions.append(full_status.partition)
                    else:
                        others.append(address)

        return preferred + others, partitions

    # ------------------------------------------------------------------ #
    # Metadata helpers
    # ------------------------------------------------------------------ #

    def list_active_servers(self) -> list[str]:
        return self.meta.list_members(ACTIVE_SERVERS)

    def load_server_status(self, address: str) -> ServerStatus:
        path = f"{SERVERS}/{address}"
        if path not in self.meta:
            raise KeyError(f"Server `{address}` never registered.")

        data = self.meta.get_keys(path)
        if data is None:
            raise KeyError(f"Server `{address}` has no status data.")
        return ServerStatus(**data)

    def update_server_status(
        self,
        server: str,
        register: bool | None = None,
        partition: int | None = None,
        expid: str | None = None,
        repid: str | None = None,
        nodes: list[str] | None = None,
        state: State | None = None,
    ) -> None:
        """
        Registers/unregisters a server and updates individual attributes.
        """
        # Register / unregister in ACTIVE_SERVERS
        if register is not None:
            active_path = f"{ACTIVE_SERVERS}/{server}"
            if active_path in self.meta:
                self.meta.drop_key(active_path)
            if register:
                # Initialize default status under SERVERS/<server>
                self.meta.update_keys(
                    f"{SERVERS}/{server}", ServerStatus().__dict__
                )
                # Ephemeral node marks this server as active
                self.meta.update_key(active_path, 0, ephemeral=True)

        # Update individual attributes
        for att, name in (
            (partition, "partition"),
            (expid, "expid"),
            (repid, "repid"),
            (nodes, "nodes"),
            (state, "state"),
        ):
            if att is not None:
                self.meta.update_key(f"{SERVERS}/{server}/{name}", att)

    # ------------------------------------------------------------------ #
    # Logging hook
    # ------------------------------------------------------------------ #

    # noinspection PyUnusedLocal
    def log_timings(
        self,
        process_time: float,
        thread_times: dict[str, float],
        meta: Metastore | None = None,
    ) -> None:
        ...
    