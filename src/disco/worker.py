from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from threading import Condition, RLock, Thread
from typing import Callable, Dict, Iterable, Optional, Sequence, TYPE_CHECKING

from tools.mp_logging import getLogger

from .cluster import Cluster, WorkerState, DesiredWorkerState
from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.ipc_messages import IPCEventMsg, IPCPromiseMsg

if TYPE_CHECKING:
    # Only needed for type checking; avoids import cycles at runtime.
    from .node_controller import NodeController

logger = getLogger(__name__)


class WorkerError(Exception):
    """Base exception for Worker-related failures."""
    pass


NodeFactory = Callable[[str], "NodeController"]


@dataclass(slots=True)
class Assignment:
    """
    Current logical assignment of this worker.

    This mirrors WorkerInfo in cluster.py, but kept locally so we can
    handle pending desired-state changes before publishing to Cluster.
    """
    expid: Optional[str] = None
    repid: Optional[str] = None
    partition: Optional[int] = None
    nodes: Sequence[str] = ()


class Worker:
    """
    Long-lived simulation worker hosting NodeControllers and running a
    single-threaded runner loop.

    Responsibilities (as per ENGINEERING_SPEC chapter 5):

    - Hosting NodeControllers for all assigned nodes.
    - Running the deterministic runner loop (single thread).
    - Maintaining WorkerState and applying desired-state changes.
    - Configuring routing/transports (via NodeControllers and external
      setup; not implemented here).
    - Providing a thread-safe ingress surface that collects IPCEventMsg
      / IPCPromiseMsg into local queues; the runner loop drains these,
      prioritizing promises over events, and delivers them to
      NodeControllers.

    Notes:
    - All state transitions and NodeController lifecycle changes happen
      in the runner thread, not in the Metastore / watch callback thread.
    - Desired-state updates are delivered via Cluster.on_desired_state_change
      and stored as "pending" until the runner applies them.
    - Ingress of events/promises is allowed only in READY, ACTIVE, and
      PAUSED states.
    """

    def __init__(
        self,
        address: str,
        cluster: Cluster,
        node_factory: NodeFactory,
        *,
        name: Optional[str] = None,
    ) -> None:
        """
        Parameters
        ----------
        address:
            Logical worker address (matches keys used in Cluster).
        cluster:
            Cluster instance used for registration, state updates and
            desired-state watches.
        node_factory:
            Callable used to construct NodeController instances for
            given node names. The Worker itself does not know how
            NodeControllers are configured internally.
        name:
            Optional human-readable name for logging / thread naming.
        """
        self.address = address
        self._cluster = cluster
        self._node_factory = node_factory
        self._name = name or address

        # Lock + condition for coordinating runner and control-plane.
        self._lock = RLock()
        self._condition = Condition(self._lock)

        # Internal state
        self._state: WorkerState = WorkerState.CREATED
        self._assignment: Assignment = Assignment()
        self._nodes: Dict[str, "NodeController"] = {}

        # Desired-state handling
        self._pending_desired: Optional[DesiredWorkerState] = None

        # Ingress queues (local to this process, fed by gRPC ingress
        # and, eventually, by IPC receivers).
        self._ingress_event_queue: Queue[IPCEventMsg] = Queue()
        self._ingress_promise_queue: Queue[IPCPromiseMsg] = Queue()

        # Runner thread control
        self._running: bool = False
        self._runner_thread: Optional[Thread] = None

        # Register worker in Cluster and install desired-state watcher.
        self._register_with_cluster()

        logger.info("Worker %s created with initial state %s", self._name, self._state)

    # ------------------------------------------------------------------ #
    # Public lifecycle API
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """
        Start the worker runner thread if not already running.
        """
        with self._lock:
            if self._running:
                return
            self._running = True

            self._runner_thread = Thread(
                target=self._runner_loop,
                name=f"WorkerRunner-{self._name}",
                daemon=True,
            )
            self._runner_thread.start()
            logger.info("Worker %s runner started.", self._name)

    def stop(self, *, unregister: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the runner thread and optionally unregister from the cluster.

        Parameters
        ----------
        unregister:
            If True, unregister the worker from the Cluster after the
            runner has stopped.
        timeout:
            Optional join timeout for the runner thread.
        """
        with self._lock:
            if not self._running:
                if unregister:
                    self._safe_unregister()
                return
            self._running = False
            self._condition.notify_all()

        if self._runner_thread is not None:
            self._runner_thread.join(timeout=timeout)

        if unregister:
            self._safe_unregister()

        logger.info("Worker %s stopped.", self._name)

    @property
    def state(self) -> WorkerState:
        with self._lock:
            return self._state

    @property
    def nodes(self) -> Dict[str, "NodeController"]:
        """
        Expose a read-only view of the current node controllers.
        This is primarily intended for transport setup (InProcessTransport).
        """
        with self._lock:
            return dict(self._nodes)

    # ------------------------------------------------------------------ #
    # Ingress API (used by gRPC ingress / IPC receivers)
    # ------------------------------------------------------------------ #

    def enqueue_ingress_event(self, msg: IPCEventMsg) -> None:
        """
        Enqueue an IPCEventMsg for delivery in the runner loop.

        Allowed Worker states for ingress:
            - READY
            - ACTIVE
            - PAUSED

        In other states, this raises WorkerError so upstream callers
        (e.g. gRPC servicer) can translate it into an appropriate
        failure / retry signal.
        """
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
            except Exception as exc:  # queue.Full or other
                raise WorkerError(
                    f"Worker {self._name} could not enqueue event: {exc!r}"
                ) from exc

            # Wake the runner so it can pick up the new work promptly.
            self._condition.notify_all()

    def enqueue_ingress_promise(self, msg: IPCPromiseMsg) -> None:
        """
        Enqueue an IPCPromiseMsg for delivery in the runner loop.

        Same state rules as for events, but promises are strictly
        prioritized when draining.
        """
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
            except Exception as exc:  # queue.Full or other
                raise WorkerError(
                    f"Worker {self._name} could not enqueue promise: {exc!r}"
                ) from exc

            # Wake the runner so it can pick up the new work promptly.
            self._condition.notify_all()

    # ------------------------------------------------------------------ #
    # Cluster / registration
    # ------------------------------------------------------------------ #

    def _register_with_cluster(self) -> None:
        """
        Register in the Cluster and set up desired-state watch.
        """
        # register_worker will create default WorkerInfo and ephemeral
        # registered_workers entry.
        self._cluster.register_worker(self.address, state=self._state)

        # Install desired-state watch.
        self._cluster.on_desired_state_change(self.address, self._on_desired_state_change)

    def _safe_unregister(self) -> None:
        try:
            self._cluster.unregister_worker(self.address)
        except Exception as exc:
            logger.warning("Worker %s unregister failed: %r", self._name, exc)

    # ------------------------------------------------------------------ #
    # Desired state handling (callback thread)
    # ------------------------------------------------------------------ #

    def _on_desired_state_change(self, desired: DesiredWorkerState) -> str | None:
        """
        Callback invoked by Cluster when desired-state changes for this worker.

        Runs in the Metastore/watch thread, so it MUST NOT perform heavy work
        or state transitions directly. It only:

        - Stores desired state as "pending"
        - Wakes the runner via Condition

        Returns:
        - None on success (ack.success = True)
        - str error message if this worker cannot accept the request
        """
        with self._lock:
            if self._state == WorkerState.BROKEN:
                msg = (
                    f"Worker {self._name} is BROKEN; cannot process desired-state "
                    f"{desired.request_id}"
                )
                logger.error(msg)
                return msg

            # Store latest desired state and wake runner.
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
    # Runner loop (single-threaded execution)
    # ------------------------------------------------------------------ #

    def _runner_loop(self) -> None:
        """
        Main runner loop.

        - Applies pending desired-state changes.
        - Drains ingress queues (promises first, then events) in states
          READY, ACTIVE, and PAUSED.
        - When ACTIVE, steps all NodeControllers in sequence.
        - Otherwise waits on Condition for further desired-state updates
          or ingress or stop requests.
        """
        try:
            while True:
                with self._lock:
                    # Check stop condition first.
                    if not self._running:
                        break

                    # Apply any pending desired state.
                    self._apply_pending_desired_locked()

                    # Re-check after applying desired state (state may have changed).
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
                        controllers: Iterable["NodeController"] = list(self._nodes.values())
                    else:
                        # Not ACTIVE → if no pending work, block until new desired
                        # state, ingress, or stop.
                        if (
                            self._pending_desired is None
                            and self._ingress_event_queue.empty()
                            and self._ingress_promise_queue.empty()
                            and self._running
                        ):
                            self._condition.wait()
                        continue

                # Outside lock: actually step NodeControllers.
                for controller in controllers:
                    try:
                        # NodeController.step() is expected to:
                        # - Drain a bit of its EventQueue
                        # - Execute a small piece of simulation logic
                        controller.step()
                    except Exception as exc:
                        # Any unexpected error in controller logic is fatal.
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

    # ------------------------------------------------------------------ #
    # Ingress draining (runner thread only)
    # ------------------------------------------------------------------ #

    def _drain_ingress_locked(self) -> None:
        """
        Drain all pending ingress messages, prioritizing promises.

        Must be called with self._lock held.
        """

        # First drain all promises.
        while True:
            try:
                msg = self._ingress_promise_queue.get_nowait()
            except Empty:
                break
            self._deliver_ingress_promise_locked(msg)

        # Then drain all events.
        while True:
            try:
                msg = self._ingress_event_queue.get_nowait()
            except Empty:
                break
            self._deliver_ingress_event_locked(msg)

    def _deliver_ingress_event_locked(self, msg: IPCEventMsg) -> None:
        """
        Convert an IPCEventMsg into an EventEnvelope and deliver it to
        the appropriate NodeController.

        Must be called with self._lock held.
        """
        node = self._nodes.get(msg.target_node)
        if node is None:
            # Unknown target node → misconfiguration for this Worker.
            self._transition_to_broken_locked(
                reason=f"Received event for unknown node {msg.target_node!r}"
            )
            return

        data: bytes
        if msg.shm_name is not None:
            # In this context (local ingress queues), we do not expect
            # shared memory; if it does happen, treat as fatal.
            self._transition_to_broken_locked(
                reason=f"Unexpected shared-memory event payload for node {msg.target_node!r}"
            )
            return
        else:
            if msg.data is None:
                self._transition_to_broken_locked(
                    reason=f"IPCEventMsg missing payload for node {msg.target_node!r}"
                )
                return
            data = msg.data

        envelope = EventEnvelope(
            target_node=msg.target_node,
            target_simproc=msg.target_simproc,
            epoch=msg.epoch,
            data=data,
            headers=msg.headers,
        )

        try:
            node.receive_event(envelope)
        except Exception as exc:
            self._transition_to_broken_locked(
                reason=f"NodeController.receive_event failed for node {msg.target_node!r}: {exc!r}"
            )

    def _deliver_ingress_promise_locked(self, msg: IPCPromiseMsg) -> None:
        """
        Convert an IPCPromiseMsg into a PromiseEnvelope and deliver it to
        the appropriate NodeController.

        Must be called with self._lock held.
        """
        node = self._nodes.get(msg.target_node)
        if node is None:
            self._transition_to_broken_locked(
                reason=f"Received promise for unknown node {msg.target_node!r}"
            )
            return

        envelope = PromiseEnvelope(
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
                reason=f"NodeController.receive_promise failed for node {msg.target_node!r}: {exc!r}"
            )

    def _clear_ingress_queues_locked(self) -> None:
        """
        Drop any pending ingress messages.

        Used during teardown or before starting a new run to avoid
        delivering stale messages to fresh assignments.
        """
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
        """
        Apply at most one pending desired-state change.

        Must be called with self._lock held and from the runner thread.
        """
        if self._pending_desired is None:
            return

        desired = self._pending_desired
        self._pending_desired = None

        if desired is None:
            return

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
        Core desired-state handling logic.

        Implements the WorkerState transitions described in chapter 5.

        Rules (simplified):

        - AVAILABLE:
            Tear down any current run and NodeControllers, clear assignment.
        - READY:
            From AVAILABLE:
                - Set assignment (expid, repid, partition, nodes)
                - INITIALIZING → create NodeControllers, update Cluster WorkerInfo
                - READY when initialization completes
        - ACTIVE:
            From READY or PAUSED → set ACTIVE (runner will start/continue stepping).
        - PAUSED:
            From ACTIVE → set PAUSED (runner will stop stepping, but ingress allowed).
        - TERMINATED:
            From ACTIVE or PAUSED or READY:
                - Immediately abort run and tear down NodeControllers
                - TRANSITION: TERMINATED → AVAILABLE
        - BROKEN:
            Not set via desired-state; only internal failures set BROKEN.
        """
        target = desired.state

        # Always update assignment fields from desired; a full desired-state
        # is considered to overwrite previous assignment.
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

        if target == WorkerState.AVAILABLE:
            # Tear down any current run and return to AVAILABLE.
            self._teardown_run_locked()
            self._assignment = Assignment()
            self._update_worker_info_locked()
            self._set_state_locked(WorkerState.AVAILABLE)
            return

        if target == WorkerState.READY:
            # Only sensible from CREATED / AVAILABLE / TERMINATED.
            if self._state not in (
                WorkerState.CREATED,
                WorkerState.AVAILABLE,
                WorkerState.TERMINATED,
            ):
                raise WorkerError(
                    f"Cannot transition to READY from {self._state.name}"
                )

            # Initialize for new run.
            self._assignment = new_assignment
            self._set_state_locked(WorkerState.INITIALIZING)

            # Create NodeControllers and publish assignment.
            self._setup_run_locked()
            self._update_worker_info_locked()

            # Initialization complete.
            self._set_state_locked(WorkerState.READY)
            return

        if target == WorkerState.ACTIVE:
            if self._state not in (WorkerState.READY, WorkerState.PAUSED):
                raise WorkerError(
                    f"Cannot transition to ACTIVE from {self._state.name}"
                )
            # Optional: update assignment if provided (e.g. no-op typically).
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

            # Abort current run and teardown NodeControllers.
            self._set_state_locked(WorkerState.TERMINATED)
            self._teardown_run_locked()
            self._assignment = Assignment()
            self._update_worker_info_locked()
            # Immediately become AVAILABLE again.
            self._set_state_locked(WorkerState.AVAILABLE)
            return

        if target == WorkerState.BROKEN:
            # External control plane should not request BROKEN.
            raise WorkerError("BROKEN state cannot be requested via desired-state.")

        if target == WorkerState.CREATED:
            # CREATED is only used as an initial state.
            raise WorkerError("CREATED state cannot be requested via desired-state.")

        # Defensive fall-through (should not be reached).
        raise WorkerError(f"Unsupported target state: {target}")

    # ------------------------------------------------------------------ #
    # Run setup/teardown (runner thread only)
    # ------------------------------------------------------------------ #

    def _setup_run_locked(self) -> None:
        """
        Create NodeControllers for all assigned nodes.

        Placeholder: the NodeController internals and routing/transports are
        configured by the application via node_factory and other mechanisms.
        """
        # Teardown any previous run just in case (and clear ingress).
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

        for node_name in nodes:
            if node_name in self._nodes:
                continue
            controller = self._node_factory(node_name)
            self._nodes[node_name] = controller

    def _teardown_run_locked(self) -> None:
        """
        Destroy all NodeControllers for the current run and clear ingress.

        This is intentionally conservative and does not assume any particular
        NodeController teardown API. If NodeControllers require explicit
        shutdown, the node_factory or application code should wrap that logic.
        """
        if self._nodes:
            logger.info(
                "Worker %s tearing down %d NodeControllers.",
                self._name,
                len(self._nodes),
            )
            self._nodes.clear()

        # Drop any pending ingress so new runs don't see stale messages.
        self._clear_ingress_queues_locked()

    # ------------------------------------------------------------------ #
    # Cluster state helpers (runner thread only)
    # ------------------------------------------------------------------ #

    def _update_worker_info_locked(self) -> None:
        """
        Publish current assignment to Cluster.WorkerInfo.
        """
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
        """
        Set internal WorkerState and update Cluster.

        Must be called with self._lock held.
        """
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
            # If we cannot update state in Cluster, this is severe.
            logger.exception(
                "Worker %s failed to publish state %s to Cluster: %r",
                self._name,
                new_state.name,
                exc,
            )
            # Last resort: mark ourselves broken.
            self._state = WorkerState.BROKEN

        # Wake up any waiters (e.g. await_available).
        self._condition.notify_all()

    def _transition_to_broken_locked(self, reason: str) -> None:
        """
        Transition the worker into BROKEN state and stop the runner.

        Must be called with self._lock held.
        """
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

        # Stop runner loop.
        self._running = False
        self._condition.notify_all()
