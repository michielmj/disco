from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from threading import Condition, RLock
from typing import Dict, Iterable, Optional, Sequence, TYPE_CHECKING, Mapping

from tools.mp_logging import getLogger

from .cluster import Cluster, WorkerState, DesiredWorkerState
from .config import AppSettings
from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.grpc_ingress import start_grpc_server
from .transports.grpc_transport import GrpcTransport
from .transports.inprocess import InProcessTransport
from .transports.ipc_egress import IPCTransport
from .transports.ipc_messages import IPCEventMsg, IPCPromiseMsg

logger = getLogger(__name__)

if TYPE_CHECKING:
    from .node_controller import NodeController
    from .router import WorkerRouter


class WorkerError(Exception):
    """Base exception for Worker-related failures."""
    pass


@dataclass(slots=True)
class Assignment:
    """
    Current logical assignment of this worker.

    This mirrors WorkerInfo in cluster.py, but is kept locally so we can
    handle pending desired-state changes before publishing to Cluster.
    """
    expid: Optional[str] = None
    repid: Optional[str] = None
    partition: Optional[int] = None
    nodes: Sequence[str] = ()


class Worker:
    """
    Long-lived simulation worker hosting NodeControllers and running a
    single-threaded runner loop in the *main thread* of the worker process.

    Control-plane changes come via Metastore/Kazoo callbacks and wake the
    runner via a Condition. Ingress (gRPC + IPC) is funneled into local
    IPC queues; the runner drains promises first, then events.
    """

    def __init__(
        self,
        address: str,
        cluster: Cluster,
        event_queues: Mapping[str, Queue[IPCEventMsg]],
        promise_queues: Mapping[str, Queue[IPCPromiseMsg]],
        settings: AppSettings,
        name: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        address:
            Logical worker address (matches keys used in Cluster/address_book).
        cluster:
            Cluster instance used for registration, state updates and
            desired-state watches.
        event_queues:
            Mapping of worker-address -> IPC event Queue
            (multiprocessing.Queue in the parent process).
        promise_queues:
            Mapping of worker-address -> IPC promise Queue.
        settings:
            Application settings (including gRPC settings).
        name:
            Optional human-readable name for logging.
        """
        self.address = address
        self._cluster = cluster
        self._name = name or address
        self._settings: AppSettings = settings

        # Global IPC queues (all workers)
        self._event_queues: Mapping[str, Queue[IPCEventMsg]] = event_queues
        self._promise_queues: Mapping[str, Queue[IPCPromiseMsg]] = promise_queues

        # Our own ingress queues must be present in the mappings.
        ingress_event_q = self._event_queues.get(self.address)
        ingress_promise_q = self._promise_queues.get(self.address)
        if ingress_event_q is None or ingress_promise_q is None:
            raise WorkerError(
                f"No IPC queues configured for worker address {self.address!r}"
            )
        self._ingress_event_queue: Queue[IPCEventMsg] = ingress_event_q
        self._ingress_promise_queue: Queue[IPCPromiseMsg] = ingress_promise_q

        # Lock + condition to coordinate runner and control-plane callbacks.
        self._lock = RLock()
        self._condition = Condition(self._lock)

        # Internal state
        self._state: WorkerState = WorkerState.CREATED
        self._assignment: Assignment = Assignment()
        self._nodes: Dict[str, NodeController] = {}

        # Router (lifetime = lifetime of Worker)
        self._router: Optional[WorkerRouter] = None

        # Desired-state handling
        self._pending_desired: Optional[DesiredWorkerState] = None

        # Runner control flag
        self._running: bool = False

        # Cluster registration + desired-state watch
        self._register_with_cluster()

        # Build transports and router (no repid here; repid lives in envelopes).
        inproc = InProcessTransport(nodes=self._nodes, cluster=self._cluster)
        ipc = IPCTransport(
            cluster=self._cluster,
            event_queues=self._event_queues,
            promise_queues=self._promise_queues,
        )
        grpc = GrpcTransport(
            cluster=self._cluster,
            settings=self._settings.grpc,
        )

        from .router import WorkerRouter  # avoid circular import at module import time
        self._router = WorkerRouter(
            cluster=self._cluster,
            transports=[inproc, ipc, grpc],
        )

        # Start gRPC server; implementation should enqueue IPCEventMsg /
        # IPCPromiseMsg into our ingress queues.
        self._grpc_server = start_grpc_server(
            worker=self,
            event_queue=self._ingress_event_queue,
            promise_queue=self._ingress_promise_queue,
            settings=self._settings.grpc,
        )

        logger.info("Worker %s created with initial state %s", self._name, self._state)

    # ------------------------------------------------------------------ #
    # Runner entrypoint (main thread of worker process)
    # ------------------------------------------------------------------ #

    def run_forever(self) -> WorkerState:
        """
        Run the worker's main loop in the current thread.

        Typical usage in the worker process:

            worker = Worker(...)
            exit_state = worker.run_forever()
            # parent process can inspect exit_state to decide on restart.

        Returns
        -------
        WorkerState
            Final WorkerState when the runner loop exits (e.g. EXITED, BROKEN).
        """
        with self._lock:
            if self._running:
                raise WorkerError("Worker runner already running")
            self._running = True

        self._runner_loop()

        with self._lock:
            final_state = self._state
        return final_state

    def request_stop(self) -> None:
        """
        Local helper to request a graceful stop.

        This does *not* change WorkerState; it's mainly useful for signal
        handlers inside the worker process. Normal control should use
        desired-state EXITED via the Cluster.
        """
        with self._lock:
            self._running = False
            self._condition.notify_all()

    # ------------------------------------------------------------------ #
    # Ingress API (optional helpers if someone wants to enqueue directly)
    # ------------------------------------------------------------------ #

    def enqueue_ingress_event(self, msg: IPCEventMsg) -> None:
        with self._lock:
            if self._state not in (
                WorkerState.READY,
                WorkerState.ACTIVE,
                WorkerState.PAUSED,
            ):
                raise WorkerError(
                    f"Worker {self._name} not accepting events in state={self._state.name}"
                )

            try:
                self._ingress_event_queue.put_nowait(msg)
            except Exception as exc:
                raise WorkerError(
                    f"Worker {self._name} could not enqueue event: {exc!r}"
                ) from exc

            self._condition.notify_all()

    def enqueue_ingress_promise(self, msg: IPCPromiseMsg) -> None:
        with self._lock:
            if self._state not in (
                WorkerState.READY,
                WorkerState.ACTIVE,
                WorkerState.PAUSED,
            ):
                raise WorkerError(
                    f"Worker {self._name} not accepting promises in state={self._state.name}"
                )

            try:
                self._ingress_promise_queue.put_nowait(msg)
            except Exception as exc:
                raise WorkerError(
                    f"Worker {self._name} could not enqueue promise: {exc!r}"
                ) from exc

            self._condition.notify_all()

    # ------------------------------------------------------------------ #
    # Cluster / registration
    # ------------------------------------------------------------------ #

    def _register_with_cluster(self) -> None:
        self._cluster.register_worker(self.address, state=self._state)
        self._cluster.on_desired_state_change(
            self.address, self._on_desired_state_change
        )

    def _safe_unregister(self) -> None:
        try:
            self._cluster.unregister_worker(self.address)
        except Exception as exc:
            logger.warning("Worker %s unregister failed: %r", self._name, exc)

    # ------------------------------------------------------------------ #
    # Desired state handling (Metastore/Kazoo callback thread)
    # ------------------------------------------------------------------ #

    def _on_desired_state_change(self, desired: DesiredWorkerState) -> str | None:
        with self._lock:
            if self._state == WorkerState.BROKEN:
                msg = (
                    f"Worker {self._name} is BROKEN; cannot process desired-state "
                    f"{desired.request_id}"
                )
                logger.error(msg)
                return msg

            self._pending_desired = desired
            self._condition.notify_all()
            logger.debug(
                "Worker %s received desired-state %s -> target=%s expid=%s repid=%s "
                "partition=%s nodes=%s",
                self._name,
                desired.request_id,
                desired.state,
                desired.expid,
                desired.repid,
                desired.partition,
                desired.nodes,
            )
            return None

    # ------------------------------------------------------------------ #
    # Runner loop (single-threaded execution in current thread)
    # ------------------------------------------------------------------ #

    def _runner_loop(self) -> None:
        try:
            while True:
                with self._lock:
                    if not self._running:
                        break

                    self._apply_pending_desired_locked()

                    if not self._running:
                        break

                    state = self._state

                    # Drain ingress in READY / ACTIVE / PAUSED.
                    if state in (
                        WorkerState.READY,
                        WorkerState.ACTIVE,
                        WorkerState.PAUSED,
                    ):
                        self._drain_ingress_locked()

                    if state == WorkerState.ACTIVE:
                        controllers: Iterable[NodeController] = list(
                            self._nodes.values()
                        )
                    else:
                        # Not ACTIVE → block if nothing pending.
                        if (
                            self._pending_desired is None
                            and self._ingress_event_queue.empty()
                            and self._ingress_promise_queue.empty()
                            and self._running
                        ):
                            self._condition.wait()
                        continue

                # Outside lock: step controllers.
                for controller in controllers:
                    try:
                        controller.step()
                    except Exception as exc:
                        logger.exception(
                            "Worker %s controller %r step failed: %r",
                            self._name,
                            controller,
                            exc,
                        )
                        with self._lock:
                            self._transition_to_broken_locked(
                                reason=f"controller step failed: {exc!r}"
                            )
                        break

        finally:
            logger.info("Worker %s runner loop exited.", self._name)
            # Best-effort unregister; process is about to end anyway.
            self._safe_unregister()

    # ------------------------------------------------------------------ #
    # Ingress draining (runner thread only)
    # ------------------------------------------------------------------ #

    def _drain_ingress_locked(self) -> None:
        # Promises first
        while True:
            try:
                msg = self._ingress_promise_queue.get_nowait()
            except Empty:
                break
            self._deliver_ingress_promise_locked(msg)

        # Then events
        while True:
            try:
                msg = self._ingress_event_queue.get_nowait()
            except Empty:
                break
            self._deliver_ingress_event_locked(msg)

    def _deliver_ingress_event_locked(self, msg: IPCEventMsg) -> None:
        node = self._nodes.get(msg.target_node)
        if node is None:
            self._transition_to_broken_locked(
                reason=f"Received event for unknown node {msg.target_node!r}"
            )
            return

        if msg.shm_name is not None:
            self._transition_to_broken_locked(
                reason=f"Unexpected shared-memory event payload for node {msg.target_node!r}"
            )
            return

        if msg.data is None:
            self._transition_to_broken_locked(
                reason=f"IPCEventMsg missing payload for node {msg.target_node!r}"
            )
            return

        envelope = EventEnvelope(
            repid=msg.repid,
            target_node=msg.target_node,
            target_simproc=msg.target_simproc,
            epoch=msg.epoch,
            data=msg.data,
            headers=msg.headers,
        )

        try:
            node.receive_event(envelope)
        except Exception as exc:
            self._transition_to_broken_locked(
                reason=(
                    "NodeController.receive_event failed for "
                    f"node {msg.target_node!r}: {exc!r}"
                )
            )

    def _deliver_ingress_promise_locked(self, msg: IPCPromiseMsg) -> None:
        node = self._nodes.get(msg.target_node)
        if node is None:
            self._transition_to_broken_locked(
                reason=f"Received promise for unknown node {msg.target_node!r}"
            )
            return

        envelope = PromiseEnvelope(
            repid=msg.repid,
            target_node=msg.target_node,
            target_simproc=msg.target_simproc,
            seqnr=msg.seqnr,
            epoch=msg.epoch,
            num_events=msg.num_events,
        )

        try:
            node.receive_promise(envelope)
        except Exception as exc:
            self._transition_to_broken_locked(
                reason=(
                    "NodeController.receive_promise failed for "
                    f"node {msg.target_node!r}: {exc!r}"
                )
            )

    def _clear_ingress_queues_locked(self) -> None:
        while True:
            try:
                self._ingress_promise_queue.get_nowait()
            except Empty:
                break

        while True:
            try:
                self._ingress_event_queue.get_nowait()
            except Empty:
                break

    # ------------------------------------------------------------------ #
    # Desired-state application (runner thread only)
    # ------------------------------------------------------------------ #

    def _apply_pending_desired_locked(self) -> None:
        if self._pending_desired is None:
            return

        desired = self._pending_desired
        self._pending_desired = None

        try:
            self._apply_desired_locked(desired)
        except Exception as exc:
            logger.exception(
                "Worker %s failed to apply desired-state %s: %r",
                self._name,
                desired.request_id,
                exc,
            )
            self._transition_to_broken_locked(
                reason=f"failed to apply desired-state {desired.request_id}: {exc!r}"
            )

    def _apply_desired_locked(self, desired: DesiredWorkerState) -> None:
        """
        Core desired-state handling logic, including EXITED.

        EXITED:
            - Publish EXITED to Cluster.
            - Tear down current run.
            - Stop the runner loop (run_forever returns EXITED).
        """
        target = desired.state

        new_assignment = Assignment(
            expid=desired.expid,
            repid=desired.repid,
            partition=desired.partition,
            nodes=tuple(desired.nodes or ()),
        )

        logger.info(
            "Worker %s applying desired-state %s: target=%s assignment=%s current=%s",
            self._name,
            desired.request_id,
            target.name,
            new_assignment,
            self._state.name,
        )

        # EXITED: cluster-driven process exit
        if target == WorkerState.EXITED:
            # Optional: clean up any active run before exit.
            self._teardown_run_locked()
            # Publish EXITED to the cluster.
            self._set_state_locked(WorkerState.EXITED)
            # Stop the runner; run_forever() will return EXITED.
            self._running = False
            self._condition.notify_all()
            return

        if target == WorkerState.AVAILABLE:
            self._teardown_run_locked()
            self._assignment = Assignment()
            self._update_worker_info_locked()
            self._set_state_locked(WorkerState.AVAILABLE)
            return

        if target == WorkerState.READY:
            if self._state not in (
                WorkerState.CREATED,
                WorkerState.AVAILABLE,
                WorkerState.TERMINATED,
            ):
                raise WorkerError(
                    f"Cannot transition to READY from {self._state.name}"
                )

            self._assignment = new_assignment
            self._set_state_locked(WorkerState.INITIALIZING)

            self._setup_run_locked()
            self._update_worker_info_locked()

            self._set_state_locked(WorkerState.READY)
            return

        if target == WorkerState.ACTIVE:
            if self._state not in (WorkerState.READY, WorkerState.PAUSED):
                raise WorkerError(
                    f"Cannot transition to ACTIVE from {self._state.name}"
                )
            self._assignment = (
                new_assignment if new_assignment.nodes else self._assignment
            )
            self._update_worker_info_locked()
            self._set_state_locked(WorkerState.ACTIVE)
            return

        if target == WorkerState.PAUSED:
            if self._state != WorkerState.ACTIVE:
                raise WorkerError(
                    f"Cannot transition to PAUSED from {self._state.name}"
                )
            self._set_state_locked(WorkerState.PAUSED)
            return

        if target == WorkerState.TERMINATED:
            if self._state not in (
                WorkerState.ACTIVE,
                WorkerState.PAUSED,
                WorkerState.READY,
            ):
                raise WorkerError(
                    f"Cannot terminate from {self._state.name}"
                )

            self._set_state_locked(WorkerState.TERMINATED)
            self._teardown_run_locked()
            self._assignment = Assignment()
            self._update_worker_info_locked()
            self._set_state_locked(WorkerState.AVAILABLE)
            return

        if target == WorkerState.BROKEN:
            raise WorkerError("BROKEN state cannot be requested via desired-state.")

        if target == WorkerState.CREATED:
            raise WorkerError("CREATED state cannot be requested via desired-state.")

        raise WorkerError(f"Unsupported target state: {target}")

    # ------------------------------------------------------------------ #
    # Run setup/teardown (runner thread only)
    # ------------------------------------------------------------------ #

    def _setup_run_locked(self) -> None:
        from .node_controller import NodeController

        self._teardown_run_locked()

        nodes = list(self._assignment.nodes)
        logger.info(
            "Worker %s setting up run for expid=%s repid=%s partition=%s nodes=%s",
            self._name,
            self._assignment.expid,
            self._assignment.repid,
            self._assignment.partition,
            nodes,
        )

        self._nodes = {}

        for node_name in nodes:
            if node_name in self._nodes:
                continue
            controller = NodeController(
                node_name,
                self._assignment.expid,
                self._assignment.repid,
                self._assignment.partition,
                self._router,
            )
            self._nodes[node_name] = controller

    def _teardown_run_locked(self) -> None:
        if self._nodes:
            logger.info(
                "Worker %s tearing down %d NodeControllers.",
                self._name,
                len(self._nodes),
            )
            self._nodes.clear()

        # Router & transports stay alive for the lifetime of the Worker.
        self._clear_ingress_queues_locked()

    # ------------------------------------------------------------------ #
    # Cluster state helpers / BROKEN transition
    # ------------------------------------------------------------------ #

    def _update_worker_info_locked(self) -> None:
        try:
            self._cluster.update_worker_info(
                worker=self.address,
                partition=self._assignment.partition,
                expid=self._assignment.expid,
                repid=self._assignment.repid,
                nodes=list(self._assignment.nodes),
            )
        except Exception as exc:
            raise WorkerError(
                f"Failed to update WorkerInfo in Cluster: {exc!r}"
            ) from exc

    def _set_state_locked(self, new_state: WorkerState) -> None:
        if new_state == self._state:
            return

        logger.info(
            "Worker %s state transition: %s -> %s",
            self._name,
            self._state.name,
            new_state.name,
        )
        self._state = new_state

        try:
            self._cluster.set_worker_state(self.address, new_state)
        except Exception as exc:
            logger.exception(
                "Worker %s failed to publish state %s to Cluster: %r",
                self._name,
                new_state.name,
                exc,
            )
            self._state = WorkerState.BROKEN

        self._condition.notify_all()

    def _transition_to_broken_locked(self, reason: str) -> None:
        logger.error("Worker %s entering BROKEN state: %s", self._name, reason)
        self._state = WorkerState.BROKEN
        try:
            self._cluster.set_worker_state(self.address, WorkerState.BROKEN)
        except Exception as exc:
            logger.exception(
                "Worker %s failed to publish BROKEN state to Cluster: %r",
                self._name,
                exc,
            )

        self._running = False
        self._condition.notify_all()
        