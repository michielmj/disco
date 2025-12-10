from __future__ import annotations

from enum import IntEnum
from types import MappingProxyType
import uuid
from functools import partial
from threading import Condition, RLock
from dataclasses import asdict, dataclass, field
from typing import Mapping, cast, Callable, Any


from tools.mp_logging import getLogger
from disco.metastore import Metastore

logger = getLogger(__name__)


class WorkerState(IntEnum):
    CREATED = 0
    AVAILABLE = 1
    INITIALIZING = 2
    READY = 3
    ACTIVE = 4
    PAUSED = 5
    TERMINATED = 6
    BROKEN = 9


@dataclass(slots=True)
class WorkerInfo:
    expid: str | None = None
    repid: str | None = None
    partition: int | None = None
    nodes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DesiredWorkerState:
    request_id: str
    expid: str | None
    repid: str | None
    partition: int | None
    nodes: list[str] | None
    state: WorkerState


PROCESSES = "/simulation/processes"
NPARTITIONS = "/simulation/npartitions"
REGISTERED_WORKERS = "/simulation/registered_workers"
WORKERS = "/simulation/workers"
DESIRED_STATE = "/simulation/desired_state"
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
    REGISTERED_WORKERS,
    WORKERS,
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

        self._lock = RLock()
        # Condition uses the same lock as worker_state for consistency
        self._available_condition = Condition(self._lock)

        # Internal state
        self._worker_state: dict[str, WorkerState] = {}
        self._worker_nodes: dict[str, list[str]] = {}
        self._worker_repids: dict[str, str] = {}

        self._address_book: dict[tuple[str, str], str] = {}
        self._address_book_uptodate = False

        # Watch registered workers list
        self.meta.watch_members_with_callback(
            REGISTERED_WORKERS, self._watch_children
        )

    # ------------------------------------------------------------------ #
    # ZooKeeper watch callbacks
    # ------------------------------------------------------------------ #

    def _watch_children(self, children: list[str], _: str) -> bool:
        """
        Children of REGISTERED_WORKERS changed:
        - Remove workers that disappeared.
        - Add new workers and start watching their state/nodes/repid.
        """
        with self._lock:
            current = set(self._worker_state.keys())
            incoming = set(children)

            deletes = current - incoming
            appends = incoming - current

            for address in deletes:
                self._worker_state.pop(address, None)
                self._worker_nodes.pop(address, None)
                self._worker_repids.pop(address, None)
                self._address_book_uptodate = False

            for address in appends:
                # Seed in-memory structures so watch callbacks don't early-return.
                self._worker_state[address] = WorkerState.CREATED
                self._worker_nodes[address] = []
                self._worker_repids[address] = ""

                # Watch individual paths (logical paths, no leading "/")
                self.meta.watch_with_callback(
                    f"{REGISTERED_WORKERS}/{address}",
                    partial(self._watch_worker_state, address),
                )
                self.meta.watch_with_callback(
                    f"{WORKERS}/{address}/nodes",
                    partial(self._watch_worker_nodes, address),
                )
                self.meta.watch_with_callback(
                    f"{WORKERS}/{address}/repid",
                    partial(self._watch_worker_repid, address),
                )

            # Any change might affect address_book and availability
            self._address_book_uptodate = False
            self._available_condition.notify_all()

        return True

    def _watch_worker_state(self, address: str, state: WorkerState, _path: str) -> bool:
        """
        Called with decoded `state` (WorkerState enum) by Metastore.
        """
        with self._lock:
            if address not in self._worker_state:
                # Worker has been removed; stop watching
                return False

            self._worker_state[address] = state
            self._available_condition.notify_all()

        return True

    def _watch_worker_nodes(self, address: str, nodes: list[str] | None, _path: str) -> bool:
        """
        Called with decoded `nodes` (list[str]) by Metastore.
        """
        with self._lock:
            if address not in self._worker_state:
                return False

            self._worker_nodes[address] = nodes or []
            self._address_book_uptodate = False

        return True

    def _watch_worker_repid(self, address: str, repid: str | None, _path: str) -> bool:
        """
        Called with decoded `repid` (str) by Metastore.
        """
        with self._lock:
            if address not in self._worker_state:
                return False

            self._worker_repids[address] = repid or ""
            self._address_book_uptodate = False

        return True

    # ------------------------------------------------------------------ #
    # Public properties
    # ------------------------------------------------------------------ #

    @property
    def address_book(self) -> Mapping[tuple[str, str], str]:
        """
        Mapping (repid, node) -> worker address.
        """
        with self._lock:
            if not self._address_book_uptodate:
                address_book: dict[tuple[str, str], str] = {}
                for address, nodes in self._worker_nodes.items():
                    repid = self._worker_repids.get(address, "")
                    for node in nodes:
                        address_book[(repid, node)] = address

                self._address_book = address_book
                self._address_book_uptodate = True

            # Return a read-only view
            return MappingProxyType(dict(self._address_book))

    @property
    def worker_states(self) -> Mapping[str, WorkerState]:
        with self._lock:
            return MappingProxyType(dict(self._worker_state))

    # ------------------------------------------------------------------ #
    # Availability / selection
    # ------------------------------------------------------------------ #

    def await_available(self, timeout: float | None = None) -> bool:
        """
        Wait until a notification that something changed in availability.

        Returns True if notified, False on timeout.
        """
        with self._available_condition:
            return self._available_condition.wait(timeout=timeout)

    def get_available(self, expid: str = "") -> tuple[list[str], list[int]]:
        """
        Returns:
          - list of worker addresses (preferred first),
          - list of partitions of preferred workers.

        Preferred workers:
          - state == AVAILABLE
          - full_status.expid == expid
          - each partition used at most once in the preferred list
        """
        preferred: list[str] = []
        others: list[str] = []
        partitions: list[int] = []

        with self._lock:
            for address, state in self._worker_state.items():
                if state == WorkerState.AVAILABLE:
                    full_status = self.get_worker_info(address)
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

    def get_worker_info(self, address: str) -> WorkerInfo:
        path = f"{WORKERS}/{address}"
        if path not in self.meta:
            raise KeyError(f"Worker `{address}` not registered.")

        data = self.meta.get_keys(path)
        if data is None:
            raise KeyError(f"Worker `{address}` has no info.")
        return WorkerInfo(**data)

    def register_worker(self, worker: str, state: WorkerState = WorkerState.CREATED) -> None:
        """
        Registers a worker.
        """
        registered_path = f"{REGISTERED_WORKERS}/{worker}"
        if registered_path in self.meta:
            raise RuntimeError(f"Worker {worker} already registered.")

        # Initialize default status under WORKERS/<worker>
        self.meta.update_keys(f"{WORKERS}/{worker}", asdict(WorkerInfo()))
        # Ephemeral node marks this worker as registered
        self.meta.update_key(registered_path, state, ephemeral=True)

    def unregister_worker(self, worker: str) -> None:
        registered_path = f"{REGISTERED_WORKERS}/{worker}"
        if registered_path in self.meta:
            self.meta.drop_key(registered_path)
        else:
            raise RuntimeError(f"Worker {worker} not registered.")

    def set_worker_state(self, worker: str, state: WorkerState) -> None:
        registered_path = f"{REGISTERED_WORKERS}/{worker}"
        if registered_path not in self.meta:
            raise RuntimeError(f"Worker {worker} not registered.")

        self.meta.update_key(registered_path, state)

    def get_worker_state(self, worker: str) -> WorkerState:
        registered_path = f"{REGISTERED_WORKERS}/{worker}"
        if registered_path not in self.meta:
            raise RuntimeError(f"Worker {worker} not registered.")

        raw = self.meta.get_key(registered_path)
        if raw is None:
            raise RuntimeError(f"Worker {worker} has no state set.")

        return cast(WorkerState, raw)

    def update_worker_info(
        self,
        worker: str,
        partition: int | None = None,
        expid: str | None = None,
        repid: str | None = None,
        nodes: list[str] | None = None,
    ) -> None:
        """
        Updates worker info.
        """
        worker_path = f"{WORKERS}/{worker}"

        for att, name in (
            (partition, "partition"),
            (expid, "expid"),
            (repid, "repid"),
            (nodes, "nodes"),
        ):
            if att is not None:
                self.meta.update_key(f"{worker_path}/{name}", att)

    # ------------------------------------------------------------------ #
    # Handle desired state changes
    # ------------------------------------------------------------------ #

    def on_desired_state_change(
            self,
            worker_address: str,
            handler: Callable[[DesiredWorkerState], str | None],
    ) -> uuid.UUID:
        """
        Register a callback that receives desired-state changes for `worker_address`.

        The handler is called with a DesiredWorkerState and must return:
          - None    -> success
          - str(...) -> error message
        An ack object is written to the corresponding ack path.
        """
        desired_path = f"{DESIRED_STATE}/{worker_address}/desired"
        ack_path = f"{DESIRED_STATE}/{worker_address}/ack"

        def _callback(value: Any, _) -> bool:
            # `value` is already decoded by Metastore (likely a dict).

            try:
                error_msg = handler(cast(DesiredWorkerState, value))
            except Exception as exc:
                error_msg = f"Handler error: {exc!r}"

            self.meta.update_key(
                ack_path,
                {
                    "request_id": cast(DesiredWorkerState, value).request_id,
                    "success": error_msg is None,
                    "error": error_msg,
                },
            )

            return True  # continue watching

        return self.meta.watch_with_callback(desired_path, _callback)

    def set_desired_state(
            self,
            worker_address: str,
            state: WorkerState,
            expid: str | None = None,
            repid: str | None = None,
            partition: int | None = None,
            nodes: list[str] | None = None,
    ) -> str:
        desired_path = f"{DESIRED_STATE}/{worker_address}/desired"
        request_id = str(uuid.uuid4())

        desired = DesiredWorkerState(
            request_id=request_id,
            state=state,
            expid=expid,
            repid=repid,
            partition=partition,
            nodes=nodes,
        )

        self.meta.update_key(desired_path, desired)
        return request_id

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
