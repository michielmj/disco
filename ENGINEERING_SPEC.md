# ENGINEERING_SPEC: A Distributed Simulation Core Enginer
## 1. Overview

This document describes the architecture and design of **disco**: a **Distributed Simulation Core** for running large, event-driven simulations across multiple processes and machines.

At a high level, disco provides:

- A **runtime hierarchy** of:
  - **Application** → one OS process acting as the coordinator and host for multiple Workers.
  - **Workers** → long-lived simulation processes running on one or more machines.
  - **NodeControllers** → per-node managers that own simulation logic and event queues.
  - **Simprocs** → small simulation processes / state machines inside a node.
- A clear separation between:
  - **Control plane**: configuration, metadata, desired state, orchestration.
  - **Data plane**: events and promises flowing between nodes, Workers, and machines.
- A **metadata and coordination layer** built on ZooKeeper (wrapped by `Metastore` and `Cluster`), used for:
  - Worker registration and state
  - Experiment / replication / partition assignments
  - Routing metadata (which node lives where)
- A **routing and transport layer** that delivers events and promises:
  - In-process (same Worker)
  - Via IPC queues + shared memory (same machine, different process)
  - Via gRPC (different machines)
- A **graph/scenario subsystem** that stores layered DAGs in a relational DB, with in-memory acceleration using `python-graphblas` and persisted masks for efficient data extraction.

The **Graph** provides the **basic structure of a simulation scenario**: it describes which entities exist, how they are connected, and how interactions are layered. This graph structure **governs how nodes are created and how they interact**, and simprocs are closely linked to the graph layers. The exact mapping from graph to nodes and simprocs is specified in later chapters.

All Python source files live under:

- `src/disco/...`

The engineering spec is organized around these responsibilities:

- **Chapter 1–2** – High-level overview and core terminology.
- **Chapter 3–4** – Metastore (ZooKeeper) and Cluster (metadata, address book, control plane).
- **Chapter 5** – Worker architecture and lifecycle.
- **Chapter 6** – Routing and transport (InProcess, IPC, gRPC).
- **Chapter 7+** – Layered graph and scenarios, plus higher-level modules as they are introduced.

This spec focuses on architectural contracts and invariants rather than specific simulation models. Different simulation engines (domains, models) should be able to sit on top of the same core without changing the infrastructure components.

---

## 2. Concepts & Terminology

This section defines the core concepts used throughout the spec and codebase. Later chapters reference these terms without redefining them.

### 2.1 Runtime Hierarchy

- **Application**  
  A single OS process that bootstraps the system (e.g. a CLI or service entrypoint). An application:
  - Creates and manages one or more **Workers** (often via `multiprocessing`).
  - Instantiates a single **Metastore** and **Cluster** client in its own process.
  - Optionally runs a small simulation locally (e.g. for small experiments) without gRPC.

- **Worker**  
  A long-lived simulation process that:

  - Hosts a set of **NodeControllers** for one or more nodes.
  - Runs a **single runner loop** on one thread for determinism.
  - Maintains a **WorkerState** (`CREATED`, `AVAILABLE`, `INITIALIZING`, `READY`, `ACTIVE`, `PAUSED`, `TERMINATED`, `BROKEN`).
  - Installs routing and transports for its nodes.
  - Receives desired-state changes via the Cluster and applies them in the runner thread.

- **NodeController**  
  A per-node manager that:

  - Owns the node’s **EventQueue** and all its **simprocs**.
  - Provides methods to send/receive events and promises.
  - Serializes event payloads and delegates remote routing to the **WorkerRouter**.
  - Drains its own EventQueue when the Worker gives it attention in the runner loop.

- **Simproc**  
  A *simulation process*—a unit of simulation logic (e.g. a state machine, step function, or handler) inside a node. Conceptually:

  - Each node has a structured set of simprocs that is **closely linked to the layers of the Graph** (e.g. one simproc per layer or per role within a layer).
  - Simprocs consume events and promises from the node’s EventQueue.
  - Simprocs are not directly visible to the routing/transport layer; they are addressed through the NodeController.

The precise mapping between graph layers, nodes, and simprocs is described in a later chapter, but the basic idea is: **the Graph defines the layered structure, simprocs implement the behavior per layer.**

### 2.2 Addressing and Targets

- **Node name**  
  A unique logical name for a node within an experiment/replication (e.g. `"factory_1"`, `"warehouse_A"`).

- **Simproc name**  
  A logical name identifying a simproc within a node (e.g. `"arrival"`, `"processing"`).

- **Target string**  
  Consumers are referenced as:

  ```text
  <node>/<simproc>
  ```

  or, within a NodeController:

  ```text
  self/<simproc>
  ```

  where `"self"` means “this NodeController’s node”. NodeControllers resolve `"self"` to their own `node_name` before routing.

### 2.3 Events, Promises, and EventQueues

- **Event**  
  A message carrying actual simulation data. Characteristics:

  - Has a simulation time `epoch` (float).
  - Carries an opaque, already serialized payload (`bytes`).
  - Can have optional `headers: dict[str, str]`.

- **Promise**  
  A small, control-oriented message used by the EventQueue layer to maintain ordering and completeness. Characteristics:

  - Identified by `seqnr` (sequence number).
  - Contains `epoch` and `num_events` (how many events belong to this promise).
  - Never carries a payload (`bytes`) and is always small.

- **EventQueue**  
  A per-node (or per-node/per-simproc) queue inside the NodeController that:

  - Accepts both events and promises.
  - Is drained by the NodeController when the Worker runner gives it attention.
  - May enforce ordering and completion guarantees based on promises (details in the NodeController chapter).

- **NodeController send API**  
  The NodeController exposes a high-level API for model code to send messages:

  ```python
  def send_event(
      self,
      target: str,                # "<node>/<simproc>" or "self/<simproc>"
      epoch: float,
      data: Any,
      headers: dict[str, str] | None = None,
  ) -> None: ...

  def send_promise(
      self,
      target: str,                # "<node>/<simproc>" or "self/<simproc>"
      seqnr: int,
      epoch: float,
      num_events: int,
  ) -> None: ...
  ```

  The NodeController is responsible for:

  - Resolving `"self"` into a concrete node name.
  - Serializing `data` into `bytes`.
  - Deciding whether the target is local or remote.
  - Constructing appropriate **envelopes** and delegating to the WorkerRouter.

### 2.4 Envelopes, Routing, and Transports

- **Envelope**  
  An immutable object used to carry events and promises across process or machine boundaries:

  - `EventEnvelope` – node, simproc, epoch, serialized bytes, headers.
  - `PromiseEnvelope` – node, simproc, seqnr, epoch, num_events.

  Envelopes are used by **transports**; model code and simprocs typically see only higher-level events and promises.

- **WorkerRouter**  
  A per-Worker routing component that:

  - Knows the current replication id (`repid`).
  - Consults the **Cluster’s address book** to understand where nodes live.
  - Holds an ordered list of **transports** and chooses the first one whose `handles_node(repid, node)` returns `True`.
  - Delegates `send_event` and `send_promise` to the chosen transport.

- **Transport**  
  A concrete mechanism for delivering envelopes between processes/machines. All transports implement a common interface:

  - `handles_node(repid, node) -> bool`
  - `send_event(repid, envelope)`
  - `send_promise(repid, envelope)`

  Current transport types:

  - **InProcessTransport** – direct function calls into local NodeControllers.
  - **IPCTransport** – uses `multiprocessing.Queue` and shared memory between processes on the same machine.
  - **GrpcTransport** – uses gRPC (with protobuf messages) to communicate with Workers on other machines, with prioritized, retried promise delivery.

### 2.5 Metastore and Cluster (Control Plane)

- **Metastore**  
  A high-level abstraction over ZooKeeper providing:

  - Hierarchical key–value storage (`update_key`, `get_key`, `update_keys`, `get_keys`).
  - Logical path namespacing (groups).
  - Watch callbacks for changes.
  - Optional queue primitives.

  Metastore is **process-local**: each application process owns its own Metastore instance and ZooKeeper connection manager.

- **Cluster**  
  A higher-level view built on top of the Metastore. It:

  - Tracks **registered workers** and their ephemeral state.
  - Stores per-worker metadata (“worker info”), such as:
    - Application id (`uuid4`) for grouping Workers in the same application.
    - Worker process address (for IPC/gRPC).
    - NUMA node for affinity-aware scheduling.
    - Node assignments, experiment/replication/partition ids.
  - Maintains an **address book** mapping `(repid, node)` → worker address.
  - Exposes a **desired-state** mechanism:
    - Desired state is stored as a **single JSON blob per worker**, so Workers never see partial updates.
    - The Worker subscribes via `on_desired_state_change(worker_address, handler)`.
    - Handlers run in a different thread and signal the Worker runner via conditions.

  Cluster itself is agnostic of local Worker internals; it only manipulates metadata and desired state.

### 2.6 WorkerState and Ingress

- **WorkerState**  
  An `IntEnum` describing the lifecycle of a Worker:

  ```python
  class WorkerState(IntEnum):
      CREATED = 0
      AVAILABLE = 1
      INITIALIZING = 2
      READY = 3
      ACTIVE = 4
      PAUSED = 5
      TERMINATED = 6
      BROKEN = 9
  ```

- **Ingress rules**  
  Transports and gRPC/IPC receivers obey WorkerState:

  - Ingress accepted for:
    - `READY`
    - `ACTIVE`
    - `PAUSED` (queues may fill; backpressure applies)
  - Ingress rejected or failed for:
    - `CREATED`
    - `AVAILABLE`
    - `INITIALIZING`
    - `TERMINATED`
    - `BROKEN`

  Delivery failures (especially for promises) are logged; unrecoverable failures cause the Worker to transition to `BROKEN`.

### 2.7 Graphs, Scenarios, and Masks (Data + Structure Layer)

- **Scenario**  
  A named graph instance stored in the database. Scenarios model the **structural aspects of a simulation** (e.g. supply chain networks, process graphs, resource graphs) and are stored in the dedicated `graph` schema.

- **Graph**  
  The main in-memory structure (`disco.graph.core.Graph`) representing:

  - Vertices and layered edges (one GraphBLAS matrix per layer).
  - Labels and label types (as matrices/vectors).
  - An optional vertex mask (`GraphMask`) used to select subgraphs.

  The **Graph provides the basic structure of a simulation scenario**:

  - It defines which entities exist and how they are connected.
  - It describes **layers of interaction** (e.g. physical flows, information flows, capacity layers).
  - It **governs how nodes will be created and how they interact** at runtime: nodes and their relationships are derived from the graph structure when experiments are loaded.
  - **Simprocs are closely linked to the graph layers**: a typical pattern is that each layer corresponds to a simproc (or a small group of simprocs) that implements the behavior for that layer.

  The exact mapping (vertex ↔ node, layer ↔ simproc, label ↔ configuration) is defined in later chapters, but this spec assumes that **the graph is the canonical structural model** for a scenario.

- **GraphMask**  
  A wrapper around a `Vector[BOOL]`:

  - Represents a selection of vertices (a subgraph).
  - Can be persisted temporarily in `graph.vertex_masks` using a UUID.
  - Enables efficient DB queries by joining on the mask instead of sending large `IN` lists.

The graph/scenario subsystem is orthogonal to the runtime in implementation, but conceptually it is the **source of truth for simulation structure**: runtime components (Workers, NodeControllers, simprocs) are configured from graph and scenario metadata rather than ad-hoc configuration.

### 2.8 Configuration and Settings

- **Application configuration**  
  Disco uses a config module (e.g. `config.py`) with Pydantic models to configure:

  - Logging
  - Database (SQLAlchemy engine)
  - Metastore / ZooKeeper connection
  - gRPC settings (bind address, timeouts, keepalive, compression, retry policies)

- **GrpcSettings** (excerpt)  
  Includes fields such as:

  - `bind_host`, `bind_port`, `timeout_s`, `max_workers`, `grace_s`
  - Message size limits and keepalive options
  - `compression`
  - Promise retry configuration:
    - `promise_retry_delays_s` (backoff sequence, e.g. `[0.05, 0.15, 0.5, 1.0, 2.0]`)
    - `promise_retry_max_window_s` (e.g. `3.0` seconds)

These settings are consumed by the gRPC server and `GrpcTransport` to ensure consistent behavior across applications and environments.

## 3 Metadata Store (Zookeeper‑Backed)

### Purpose
The Metadata Store provides a distributed, fault‑tolerant key–value hierarchy used by simulation servers to coordinate state, assignments, routing metadata, replication information, and other dynamic configuration elements. It acts as the system-wide source of truth for metadata that must be shared across processes, nodes, or simulation clusters.

### Design Overview
The metastore is implemented as a thin, high‑level API on top of ZooKeeper via the `ZkConnectionManager`. It provides atomic hierarchical operations, optional path namespacing via “groups”, lightweight pub/sub via watch callbacks, and structured data expansion semantics for nested trees.

The metastore uses:
- **ZkConnectionManager**: one ZooKeeper client per process, automatically reconnected, watchers restored.
- **Serialization**: pluggable `packb` / `unpackb` (defaults: `pickle.dumps` / `pickle.loads`).
- **Hierarchical API**: `update_key`, `update_keys`, `get_key`, `get_keys`.
- **Expand semantics**: declarative control over nested tree retrieval and persistence.
- **Watch support**: callback functions attached to paths, automatically restored after reconnects.

### Path Handling & Namespacing
The metastore exposes *logical paths* (e.g. `"replications/r1/assignments/a"`). These are mapped to ZooKeeper paths via:

```
_full_path(path) =
    "/" + group + "/" + path        if group is set
    "/" + path                      otherwise
```

Examples:
- group = `sim1`, path = `"foo/bar"` → `"/sim1/foo/bar"`
- group = None, path = `"foo/bar"` → `"/foo/bar"`

This allows multiple simulations or tenants to share the same ZK cluster.

### Base Structure
On initialization, the metastore ensures a predefined directory structure (`BASE_STRUCTURE`) inside the chroot. If a group is configured, these structures are created inside the group namespace.

### Serialization
All stored values are serialized using:
- `packb(value) -> bytes`
- `unpackb(bytes) -> value`

Users may inject custom serializers (e.g. msgpack, raw JSON, custom binary formats).

### Watch Callbacks
Watchers are registered through:

```
watch_with_callback(path, callback)
```

Callbacks receive `(value, full_path)` and must return:
- **True** → continue watching  
- **False** → unregister the watcher  

Deletion events pass `raw=None` to the wrapper, which stops the watch automatically.

The `ZkConnectionManager` restores all watches on reconnect.

### Key–Value Operations

#### `update_key(path, value)`
Stores a single leaf value.

#### `get_key(path)`
Reads and deserializes a leaf value. Returns `None` if not present.

### Hierarchical Get/Update

#### Expand Semantics
The `expand` parameter dictates how nested structures are written or read:

- **`expand = {"replications": {"assignments": None}}`**  
  → fully expand `replications`, expand one level for `assignments`.

- **`expand = {"replications": None}`**  
  → expand `replications` only one level; store subtrees as dictionaries.

Examples:

With:
```
members = {"replications": {"r1": {"assignments": {"a": 1}}}}
```

**Case A — nested expand**
```
expand = {"replications": {"assignments": None}}
```
Writes:
```
/replications/r1/assignments/a = 1
```

**Case B — shallow expand**
```
expand = {"replications": None}
```
Writes:
```
/replications/assignments = {"a": 1}
```

### Automatic Parent Node Behavior
ZooKeeper itself distinguishes between nodes and their children. Our FakeKazooClient mimics this semantics: a parent exists if it has either data or children. This ensures `get_keys()` behaves consistently.

### Queue Operations
The metastore exposes simple FIFO queue operations backed by ZooKeeper’s `Queue` recipe:
- `enqueue(path, value)`
- `dequeue(path, timeout=None)`

These are used for lightweight inter-node message passing or distribution of pending events.

### Failure & Recovery Semantics
- All client operations route through a **single client instance** owned by `ZkConnectionManager`.
- Session loss triggers automatic reconnection and watch reinstallation.
- `update_keys` and `get_keys` operate only on logical paths, ensuring compatibility with grouping and chrooting.

### Intended Usage in the Application
- Store routing tables, node status, simulation assignments, replication metadata.
- Provide shared configuration across long‑lived processes.
- Support live reconfiguration without restarts.
- Enable efficient, fine-grained read access to metadata subsets.
- Allow stateless workers to bootstrap by reading the full hierarchical metadata tree.

### Limitations / Non-Goals
- Not designed for large binary payloads (those must go to shared memory or the data layer).
- Not a transactional database—ZooKeeper operations are atomic per node, not per subtree.
- Not a metrics store or event log.

### Testing Requirements
- Use `FakeKazooClient` and `FakeConnectionManager` for isolated testing.
- Must test:
  - expand semantics round-trip (write → read)
  - watch registration and deletion behavior
  - recoverability after reconnection
  - queue timeouts and ordering
  - group namespacing in paths
  - handling of scalar vs dictionary values in expansion


## 4 Cluster

### 4.1 Purpose

The **Cluster** component provides a high-level, read-mostly, strictly in-process representation of the current simulation topology. It is built entirely on top of the `Metastore` (Chapter 3), which abstracts all ZooKeeper behavior, connection management, and watcher restoration.

Cluster has no knowledge of Worker internals. It never interacts with Worker objects, NodeControllers, transports, or runtime execution. Its sole responsibility is to interpret metadata exposed in the Metastore and present a coherent view of:

- Which workers exist,
- Which nodes they host,
- Their experiment (`expid`) and replication (`repid`), both UUID4,
- Their application and NUMA grouping,
- Their runtime state,
- How to route messages to them.

This metadata is consumed by the transport layer and by the Worker lifecycle controller.

### 4.2 Worker Metadata (Persistent)

Each Worker process publishes structured metadata in the Metastore under:

```
/simulation/workers/{worker_address}/
```

All of these keys are written atomically using `Metastore.update_keys()`.

#### Metadata fields

| Key | Type | Description |
|-----|-------|-------------|
| `expid` | UUID4 string | Experiment ID to which this worker belongs |
| `repid` | UUID4 string | Replication ID for this worker |
| `partition` | int | Partition index within the replication |
| `nodes` | list[str] | Names of nodes hosted on this worker |
| `application_id` | UUID4 string | Identifier of the application process that spawned the worker |
| `numa_node` | int | NUMA node on which the worker runs |

Cluster treats this metadata as definitive topology information.

#### Malformed or incomplete metadata

If Cluster encounters missing, malformed, or nonsensical metadata:

**The corresponding worker is considered BROKEN.**

Cluster does not attempt partial interpretation or correction. The orchestration layer must recreate the worker.

### 4.3 Worker WorkerState (Ephemeral)

Each worker publishes its runtime state in an ephemeral key:

```
/simulation/registered_workers/{worker_address}
```

The value is an integer representing `WorkerState`:

```
0 CREATED
1 AVAILABLE
2 INITIALIZING
3 READY
4 ACTIVE
5 PAUSED
6 TERMINATED
9 BROKEN
```

Because the node is ephemeral:

- If the worker process dies, the key disappears automatically.
- Cluster removes the worker and all associated routing information.

Cluster does **not** perform any state transitions; it only reflects what the worker publishes.

### 4.4 Desired WorkerState (Control Plane Input)

Each worker receives its desired operational state via a **single structured value** stored under:

```python
/simulation/desired_state/{worker_address}/desired
```

This value is written atomically by the orchestrator as an object of type:

```python
DesiredWorkerState:
    request_id: UUID4
    expid: UUID4 | None
    repid: UUID4 | None
    partition: int | None
    nodes: list[str] | None
    state: WorkerState
```

**Serialization and deserialization are fully handled by the Metastore.**

Cluster neither serializes nor deserializes these values—it receives and emits Python objects exactly as returned by the 
Metastore’s unpackb function.

#### Cluster Responsibilities

Cluster does **not** interpret the fields inside the desired state. Its responsibilities are limited to:

- Installing a watch on the worker’s desired-state path.
- Receiving decoded values from the Metastore upon change.
- Delivering them to subscriber code via:

```python
cluster.on_desired_state_change(worker_address, handler)
```

The handler signature is:

```python
handler(desired_state: DesiredWorkerState) -> str | None
```

Return semantics:

- `None` → request accepted successfully  
- `str` → error message; request considered failed

Cluster writes the acknowledgment to:

```python
/simulation/desired_state/{worker_address}/ack
```

Acknowledgment structure:

```python
{
    request_id: <same as input>,
    success: bool,
    error: str | None
}
```

#### Subscription Model

Cluster does **not** maintain any internal list of subscribers.

Each call to:

```python
on_desired_state_change(worker_address, handler)
```

installs exactly one watcher via Metastore, and the handler is invoked for every update until the watch is removed 
(e.g., deletion of the node or callback returning False).

### 4.5 Address Book

From valid worker metadata, Cluster derives a mapping:

```python
address_book: Mapping[tuple[str, str], str]
# (repid: UUID4, node_name: str) -> worker_address: str
```

Semantics:

- For each `(repid, node)`, the address identifies where the node is hosted.
- This address may serve multiple nodes or partitions.
- UUID4 `repid` ensures globally unique replication identifiers.

Cluster also exposes helper classification functions:

```python
is_local_address(worker_address: str, application_id: str) -> bool
is_ipc_reachable(worker_address: str, application_id: str, numa_node: int) -> bool
is_remote_address(worker_address: str) -> bool
```

These guides transport selection but do not perform routing themselves.

### 4.6 Internal Data Structures

Cluster maintains synchronized in-memory structures:

- `worker_meta: dict[worker_address, WorkerInfo]`
- `worker_state: dict[worker_address, WorkerState]`
- `desired_state: dict[worker_address, dict]`
- `address_book: Mapping[(repid, node), worker_address]`
- `application_groups: dict[application_id, set[worker_address]]`
- `numa_layout: dict[worker_address, int]`

All modifications occur under a Cluster-level lock to guarantee consistency.

Watch callbacks from Metastore update only the affected sections.

### 4.7 Error Model and Recovery

#### Metastore disconnection

Handled **entirely** by Metastore. Cluster:

- Does not manage watchers,
- Does not manage reconnection,
- Does not implement failover logic.

When the Metastore reconnects, Cluster naturally receives updated callbacks and rebuilds state.

#### Malformed metadata

If a worker publishes invalid metadata:

- Cluster treats it as a fatal condition for that worker.
- The worker must be recreated by orchestration.
- Cluster removes the worker from routing.

#### Worker removal

If the ephemeral key disappears:

- The worker is immediately removed.
- All routing entries for nodes it hosted are dropped.

Cluster never attempts to reassign or recover nodes.

### 4.8 Non-Responsibilities

Cluster explicitly does **not**:

- Infer the local worker's address,
- Interact with Workers or NodeControllers,
- Perform routing or transport selection,
- Modify or validate desired-state semantics,
- Move workers through the state machine,
- Handle Metastore recovery mechanisms.

It is a pure metadata reflector, translating the contents of the Metastore into in-memory structures for other components to consume.

## Chapter 5: Worker Architecture & Lifecycle (Final Version)

### 5.1 Overview

A Worker is a long-lived simulation process responsible for hosting a
set of nodes and executing their logic through their associated
`NodeController` instances. Workers operate as autonomous units driven
by desired-state inputs published through the Cluster. All simulation
execution, state transitions, and NodeController stepping occur on a
single Worker runner thread for determinism and safety.

### 5.2 Worker Responsibilities

Workers are responsible for:

-   Hosting `NodeController` instances for all assigned nodes.
-   Running the simulation runner loop.
-   Maintaining and transitioning `WorkerState`.
-   Installing routing and transports between nodes.
-   Handling external ingress into `NodeController` queues.
-   Responding to desired-state changes via the Cluster.

Workers never interact directly with the Metastore.

### 5.3 WorkerState Machine

``` python
class WorkerState(IntEnum):
    CREATED = 0
    AVAILABLE = 1
    INITIALIZING = 2
    READY = 3
    ACTIVE = 4
    PAUSED = 5
    TERMINATED = 6
    BROKEN = 9
```

#### WorkerState Definitions

**CREATED** -- Worker registered but not yet set up.\
**AVAILABLE** -- Ready for a new assignment.\
**INITIALIZING** -- Creating NodeControllers, configuring routing and
transports.\
**READY** -- Ready to start the simulation run.\
**ACTIVE** -- Simulation executing; NodeControllers step each cycle.\
**PAUSED** -- Simulation paused; ingress allowed; controllers do not
step.\
**TERMINATED** -- Abort signal; Worker tears down the run and returns to
AVAILABLE.\
**BROKEN** -- Irrecoverable internal error; Worker must be restarted.

Workers do **not** have FINISHED/FAILED states; after a successful
completion, Workers return to AVAILABLE.

### 5.4 Interaction With NodeControllers

Each assigned node is represented by a `NodeController`. NodeControllers
own:

-   An `EventQueue` (events + promises)
-   Node-specific simulation logic executed during each "attention" step

The Worker only:

-   Creates and destroys NodeControllers during initialization/teardown
-   Configures routing and transports
-   Steps each NodeController in a fixed sequence during ACTIVE

NodeController implementation details are described in a later chapter.

### 5.5 Receiving Desired-WorkerState Changes

Workers do not read the Metastore. Instead:

    cluster.on_desired_state_change(worker_address, handler)

The handler is invoked on a separate thread. It must:

-   Store the received desired-state blob in a thread-safe structure\
-   Notify the Worker runner via a `threading.Condition`

All state transitions occur strictly inside the Worker runner thread.

### 5.6 Runner Loop

The Worker runner loop is strictly single-threaded:

    while running:
        apply pending desired-state changes
        if ACTIVE:
            for controller in NodeControllers:
                controller.step()
            continue       # do not block
        else:
            wait on Condition until a new desired-state update arrives

This ensures:

-   Determinism\
-   No concurrent modifications of WorkerState or NodeControllers\
-   No spinning when inactive

### 5.7 Applying Desired-WorkerState Commands

Desired-state commands may include:

-   New experiment, replication, partition, and node assignments\
-   A target `WorkerState`

The Worker:

-   Validates transitions\
-   Performs intermediate transitions as required\
-   Updates actual state through the Cluster\
-   Creates/tears down NodeControllers as needed\
-   Applies `TERMINATED` by immediately aborting the run and returning
    to AVAILABLE

Pending desired-state updates may overwrite earlier ones before being
processed; the latest value wins.

### 5.8 Ingress and Backpressure

Ingress delivers into NodeController queues. Behavior depends on
WorkerState:

  ------------------------------------------------------------------------
  WorkerState                   Ingress Allowed?               Behavior
  ----------------------- ------------------------------ -----------------
  CREATED                 No                             Not configured

  AVAILABLE               No                             No experiment
                                                         loaded

  INITIALIZING            No                             Routing and
                                                         queues not ready

  READY                   Yes                            Queues accept
                                                         events;
                                                         controllers idle

  ACTIVE                  Yes                            NodeControllers
                                                         drain
                                                         continuously

  PAUSED                  Yes                            Queues
                                                         accumulate; may
                                                         apply
                                                         backpressure

  TERMINATED              No                             Worker aborting
                                                         run

  BROKEN                  No                             Ingress rejected
  ------------------------------------------------------------------------

Backpressure policies are implemented by transports and queue limits.

### 5.9 Error Handling

If a Worker encounters a fatal error:

-   Worker enters `BROKEN`\
-   Cluster is notified\
-   Recovery requires a Worker process restart

### 5.10 Summary

-   Worker execution and control are fully single-threaded in the
    runner.\
-   NodeControllers drain their own queues; the Worker only sequences
    them.\
-   Desired-state updates arrive asynchronously but are applied
    synchronously.\
-   `TERMINATED` provides a clean mechanism to abort runs.\
-   WorkerState governs ingress, routing setup, and execution phases.\
-   This architecture ensures determinism, thread safety, and
    predictable simulation behavior.


## Chapter 6: Routing & Transport

This chapter describes the unified design for routing and transporting events and promises across:

- Same process (**InProcess** transport)
- Same machine but different processes (**IPC** transport)
- Different machines (**gRPC** transport)

All transports integrate with the Worker and NodeController architecture described earlier and use routing metadata maintained by the Cluster. This chapter focuses on the data plane (envelopes, routing, transports) and their interaction with Worker state; control-plane details (Cluster, Metastore, Worker desired state) are covered in earlier chapters.

---

### 6.1 Envelopes

Events and promises are moved between processes using immutable envelopes defined in `src/disco/envelopes.py`:

```python
from dataclasses import dataclass
from typing import Dict

@dataclass(slots=True)
class EventEnvelope:
    target_node: str
    target_simproc: str
    epoch: float
    data: bytes
    headers: Dict[str, str]

@dataclass(slots=True)
class PromiseEnvelope:
    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int
```

Properties:

- `target_node` is always the **resolved node name** (never `"self"`).
- `target_simproc` identifies the target simproc within the node.
- `epoch` is the simulation time associated with the event or promise.
- `data` is a serialized payload (`bytes`), produced by the NodeController.
- `headers` are optional, opaque metadata. When not needed, `{}` SHOULD be used.

`PromiseEnvelope` never carries a data payload; it is a small, control-oriented message that indicates the number of events that follow (or have been sent) for a given sequence number.

---

### 6.2 NodeController Interaction (Transport Perspective)

A `NodeController` owns an internal **EventQueue** that receives both events and promises. From the point of view of routing and transport:

- The NodeController:
  - Serializes outbound event payloads to `bytes`.
  - Constructs `EventEnvelope` and `PromiseEnvelope` objects for non-local targets.
  - Delegates delivery of these envelopes to a `WorkerRouter`.

- For **inbound** messages, transports eventually call:
  ```python
  node.receive_event(envelope)
  node.receive_promise(envelope)
  ```
  The NodeController enqueues the envelope contents into its own EventQueue. The Worker **never** drains this queue; draining and processing are done by the NodeController when the Worker gives it attention in the runner loop.

NodeController implementation details (queue type, ordering rules, simproc execution) are described in a later chapter; here we rely only on the existence of `receive_event` and `receive_promise` and the fact that each NodeController has its own EventQueue.

---

### 6.3 WorkerRouter

The `WorkerRouter` is responsible for choosing the appropriate transport for each envelope. Its decision is based on:

- Replication ID (`repid`)
- Target node name
- Cluster-maintained address mappings (the address book)
- The set and order of available transports for the Worker

Conceptual skeleton:

```python
class WorkerRouter:
    def __init__(self, cluster, transports, repid: str):
        self._cluster = cluster
        self._transports = list(transports)
        self._repid = repid

    def _choose_transport(self, target_node: str):
        for transport in self._transports:
            if transport.handles_node(self._repid, target_node):
                return transport
        raise KeyError(f"No transport can handle node {target_node!r} for repid {self._repid}")

    def send_event(self, envelope: EventEnvelope) -> None:
        t = self._choose_transport(envelope.target_node)
        t.send_event(self._repid, envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        t = self._choose_transport(envelope.target_node)
        t.send_promise(self._repid, envelope)
```

Transport selection is **ordered**: the first transport whose `handles_node()` returns `True` is used. The Worker is responsible for registering transports in the correct priority order (see Section 6.8).

---

### 6.4 Transport Interface

All transports implement a common interface:

```python
from typing import Protocol
from .envelopes import EventEnvelope, PromiseEnvelope

class Transport(Protocol):
    def handles_node(self, repid: str, node: str) -> bool: ...
    def send_event(self, repid: str, envelope: EventEnvelope) -> None: ...
    def send_promise(self, repid: str, envelope: PromiseEnvelope) -> None: ...
```

Responsibilities:

- `handles_node(repid, node)` decides whether the transport can reach the given node for a specific replication.
- `send_event` / `send_promise` deliver envelopes via the transport’s underlying mechanism:
  - Direct function calls (in-process)
  - Multiprocessing queues + shared memory (IPC)
  - gRPC channels and RPCs (cross-machine)

Transports MUST NOT modify envelopes in-place; envelopes are considered immutable.

---

### 6.5 In-Process Transport

**Use case**: the target node is hosted in the **same Worker process**.

```python
class InProcessTransport(Transport):
    def __init__(self, nodes: dict[str, NodeController], cluster):
        self._nodes = nodes
        self._cluster = cluster

    def handles_node(self, repid: str, node: str) -> bool:
        # Local node and recognized in the address book for this repid.
        return node in self._nodes and (repid, node) in self._cluster.address_book

    def send_event(self, repid: str, envelope: EventEnvelope) -> None:
        self._nodes[envelope.target_node].receive_event(envelope)

    def send_promise(self, repid: str, envelope: PromiseEnvelope) -> None:
        self._nodes[envelope.target_node].receive_promise(envelope)
```

Characteristics:

- Zero-copy, synchronous delivery into the NodeController.
- No additional serialization/deserialization beyond the envelope itself.
- Fastest path; SHOULD be registered before IPC and gRPC in the router’s transport list.

---

### 6.6 IPC Transport (Same Machine, Different Processes)

**Use case**: the target node lives on the **same machine**, but in a **different process**.

Each process exposes inbound IPC queues keyed by its logical address (e.g. `"127.0.0.1:5001"`):

```python
event_queues: dict[str, multiprocessing.Queue]    # address -> event queue
promise_queues: dict[str, multiprocessing.Queue]  # address -> promise queue
```

The Cluster’s address book maps `(repid, node)` → `address`. A given address may serve multiple Workers (e.g. partitions or replications) in the same application process.

#### 6.6.1 IPC Message Types

```python
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass(slots=True)
class IPCEventMsg:
    target_node: str
    target_simproc: str
    epoch: float
    headers: Dict[str, str]
    data: Optional[bytes]       # small payload inline
    shm_name: Optional[str]     # shared memory name for large payloads
    size: int                   # payload length in bytes

@dataclass(slots=True)
class IPCPromiseMsg:
    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int
```

Rules:

- For events, exactly one of `data` or `shm_name` MUST be non-`None`.
- `size` MUST match the payload length.
- Promises are always small and are sent inline as `IPCPromiseMsg` (no shared memory).

#### 6.6.2 IPCTransport Implementation

```python
class IPCTransport(Transport):
    def __init__(self, cluster, event_queues, promise_queues, large_payload_threshold: int = 64 * 1024):
        self._cluster = cluster
        self._event_queues = event_queues
        self._promise_queues = promise_queues
        self._large_threshold = large_payload_threshold

    def handles_node(self, repid: str, node: str) -> bool:
        addr = self._cluster.address_book.get((repid, node))
        return addr in self._event_queues and addr in self._promise_queues
```

Sending an event:

1. Resolve `addr = cluster.address_book[(repid, envelope.target_node)]`.
2. Choose `q = self._event_queues[addr]`.
3. If `len(envelope.data) <= large_payload_threshold`:
   - Build `IPCEventMsg(data=envelope.data, shm_name=None, size=len(envelope.data))`.
4. Otherwise:
   - Allocate a `SharedMemory` segment.
   - Copy the payload into shared memory.
   - Build `IPCEventMsg(data=None, shm_name=shm.name, size=len(envelope.data))`.
5. Put `IPCEventMsg` on `q`.

Sending a promise:

1. Resolve `addr` as above.
2. Choose `q = self._promise_queues[addr]`.
3. Build `IPCPromiseMsg` and put it on `q`.

If IPC queues or shared memory allocation fail, the sending Worker SHOULD log an error and MAY transition to state `BROKEN` if the condition is unrecoverable.

#### 6.6.3 IPC Receiver

Each Worker process runs inbound IPC receiver loops (often in dedicated threads) that:

1. Block on `event_queue.get()` for `IPCEventMsg` instances.
2. For event messages:
   - If `shm_name is None`: use `data` directly.
   - Otherwise:
     - Attach to `SharedMemory(name=msg.shm_name)`.
     - Copy `msg.size` bytes into a local `bytes` object.
     - Close and unlink the shared memory segment.
   - Construct an `EventEnvelope`.
   - Look up `node = nodes[msg.target_node]` and call `node.receive_event(envelope)`.

3. For promise messages:
   - Construct a `PromiseEnvelope`.
   - Deliver to `node.receive_promise(envelope)`.

Unknown target nodes SHOULD result in a clear, deterministic error (e.g. log and drop, or `KeyError` that escalates to `BROKEN`, depending on the application’s error policy).

Ingress via IPC MUST respect WorkerState rules (see Section 6.9).

---

### 6.7 gRPC Transport (Cross-Machine)

**Use case**: the target node lives on a **different machine**.

Each Worker process participates in gRPC communication as:

- A **gRPC client** (via `GrpcTransport`) when sending envelopes to remote Workers.
- A **gRPC server** (the “gRPC ingress service”) when receiving envelopes from remote Workers.

The Cluster’s address book maps `(repid, node)` → `remote_worker_address`. `GrpcTransport` uses this address and the application’s `GrpcSettings` to construct and manage gRPC channels.

#### 6.7.1 GrpcSettings and Retry Configuration

The application’s gRPC configuration is defined in `config.py`:

```python
class GrpcSettings(BaseModel):
    bind_host: str = Field(
        "0.0.0.0", description="Host/interface to bind the gRPC server to."
    )
    bind_port: int = Field(
        50051, description="Port to bind the gRPC server to."
    )

    timeout_s: float = Field(
        600.0, description="Default timeout for gRPC calls in seconds."
    )
    max_workers: int = Field(
        10, description="Maximum number of worker threads for the gRPC server."
    )
    grace_s: float = Field(
        60.0, description="Grace period in seconds for server shutdown."
    )

    max_send_message_bytes: int | None = Field(
        default=None,
        description="Maximum send message size in bytes (None = default).",
    )
    max_receive_message_bytes: int | None = Field(
        default=None,
        description="Maximum receive message size in bytes (None = default).",
    )

    keepalive_time_s: float | None = Field(
        default=None,
        description="Time between keepalive pings in seconds (None = disabled).",
    )
    keepalive_timeout_s: float | None = Field(
        default=None,
        description="Timeout for keepalive pings in seconds (None = default).",
    )
    keepalive_permit_without_calls: bool = Field(
        False,
        description="Allow keepalive pings even when there are no active calls.",
    )

    compression: Literal["none", "gzip"] = Field(
        "none",
        description="Compression algorithm for gRPC calls.",
    )

    # Example additional fields (names and exact types may be adjusted in code):
    promise_retry_delays_s: list[float] = Field(
        default_factory=lambda: [0.05, 0.15, 0.5, 1.0, 2.0],
        description="Backoff sequence for promise retries in seconds.",
    )
    promise_retry_max_window_s: float = Field(
        3.0,
        description="Maximum time window for promise delivery retries in seconds.",
    )
```

The GrpcTransport MUST use `promise_retry_delays_s` and `promise_retry_max_window_s` to implement promise delivery retry behavior (see Section 6.7.4).

#### 6.7.2 Protobuf Messages and Service

At the wire level, gRPC uses protobuf messages that correspond closely to the internal envelopes:

```proto
message EventEnvelopeMsg {
  string target_node    = 1;
  string target_simproc = 2;
  double epoch          = 3;
  bytes data            = 4;
  map<string, string> headers = 5;
}

message PromiseEnvelopeMsg {
  string target_node    = 1;
  string target_simproc = 2;
  int64 seqnr           = 3;
  double epoch          = 4;
  int32 num_events      = 5;
}

message TransportAck {
  string message = 1;   // optional diagnostics
}

service DiscoTransport {
  // Events: potentially large and frequent. Use a client-streaming RPC.
  rpc SendEvents(stream EventEnvelopeMsg) returns (TransportAck);

  // Promises: always small. Use a unary RPC for better latency and observability.
  rpc SendPromise(PromiseEnvelopeMsg) returns (TransportAck);
}
```

Design choices:

- Events are sent over a **client-streaming RPC** (`SendEvents`) for efficiency.
- Promises use a **unary RPC** (`SendPromise`) to support independent retry and strict priority.
- For each remote Worker, GrpcTransport MAY maintain:
  - One channel for events and one channel for promises, or
  - A single channel with separate stubs, depending on implementation.
  The important requirement is that promise delivery is **strictly prioritized** over event delivery.

#### 6.7.3 GrpcTransport (Outbound Behavior)

`GrpcTransport` is responsible for:

- Maintaining gRPC channels and stubs per remote Worker address.
- Managing a long-lived client-stream for events (`SendEvents`) per remote Worker.
- Issuing unary calls for promises (`SendPromise`) with retry logic.
- Converting internal envelopes to protobuf messages.

Conceptual sketch:

```python
class GrpcTransport(Transport):
    def __init__(self, cluster, grpc_settings: GrpcSettings, channel_factory):
        self._cluster = cluster
        self._settings = grpc_settings
        self._channel_factory = channel_factory  # e.g. addr -> grpc.Channel
        self._event_stubs = {}
        self._event_streams = {}
        self._promise_stubs = {}

    def handles_node(self, repid: str, node: str) -> bool:
        addr = self._cluster.address_book.get((repid, node))
        # 'is_remote_address' is a Cluster helper that returns True for non-local, non-IPC addresses.
        return addr is not None and self._cluster.is_remote_address(addr)
```

Outbound events:

1. Resolve `addr` from `(repid, envelope.target_node)`.
2. Obtain or create:
   - a channel for events,
   - a `DiscoTransportStub`,
   - a `SendEvents` stream for that address.
3. Convert `EventEnvelope` → `EventEnvelopeMsg`.
4. Write the message to the stream.

Outbound promises:

1. Resolve `addr` as above.
2. Obtain or create:
   - a channel for promises,
   - a `DiscoTransportStub`.
3. Convert `PromiseEnvelope` → `PromiseEnvelopeMsg`.
4. Execute a unary `SendPromise` RPC with retry behavior as described below.

#### 6.7.4 Promise Delivery Retry and Worker Failure Semantics

Promises are **not strictly synchronous**: the sender does not wait for the remote Worker to process the promise, only to accept it at the gRPC boundary. However, delivery must obey the following rules:

- **Retry policy**
  - Use the backoff sequence from `GrpcSettings.promise_retry_delays_s`, e.g.:
    - 0.05 s → 0.15 s → 0.5 s → 1.0 s → 2.0 s → ...
  - Total retry window MUST NOT exceed `GrpcSettings.promise_retry_max_window_s` (default 3.0 s).
  - Retries are performed only while the sending Worker is in state `ACTIVE` (or READY/PAUSED if the application chooses to send promises from those states as well).

- **On successful delivery**
  - The unary `SendPromise` RPC returns `OK`.
  - The sending Worker proceeds without any state change.

- **On persistent failure**
  - If all retries within the configured time window fail:
    - The failure MUST be **clearly logged**, including:
      - The target node and simproc.
      - The target Worker address.
      - The sequence number (`seqnr`) and `epoch`.
      - The total number of retry attempts and total elapsed time.
    - The sending Worker MUST transition to state `BROKEN`.
    - This transition and its reason SHOULD be published via the Cluster, as described in the Worker lifecycle chapter.

This rule ensures that the system does not silently lose promises or continue running with inconsistent partial deliveries.

#### 6.7.5 gRPC Ingress Service (Inbound Behavior)

Each Worker process hosts a gRPC server implementing the `DiscoTransport` service:

- The gRPC server runs in its own thread(s), managed by the gRPC runtime.
- The server uses a **thread-safe ingress API** on the Worker (e.g. a `WorkerIngress`) to deliver envelopes into NodeController queues.

Inbound events (`SendEvents`):

- For each `EventEnvelopeMsg` received:
  1. Convert to an internal `EventEnvelope`.
  2. Resolve the target `NodeController`.
  3. Enqueue the event into the NodeController’s EventQueue.
- The server SHOULD honor WorkerState:
  - In `READY`, `ACTIVE`, and `PAUSED`, ingress is allowed.
  - In `CREATED`, `AVAILABLE`, `INITIALIZING`, `TERMINATED`, or `BROKEN`, ingress SHOULD be rejected (e.g. by failing the RPC or dropping with error logging).

Inbound promises (`SendPromise`):

- For each unary `SendPromise` call:
  1. Convert to a `PromiseEnvelope`.
  2. Resolve `NodeController`.
  3. Enqueue the promise into the NodeController’s EventQueue.
  4. Return a `TransportAck` to the caller.
- The same WorkerState rules apply: only `READY`, `ACTIVE`, and `PAUSED` accept ingress.

Backpressure:

- If a NodeController’s queues are full:
  - The ingress layer MAY block (up to a reasonable timeout) or fail the RPC.
  - For promises, failing the RPC will trigger the sender-side retry policy.
  - For events, the streaming RPC may be backpressured or failed, depending on configuration.

The Worker runner thread remains fully isolated from gRPC network threads; it only observes new work via NodeController EventQueues.

---

### 6.8 Transport Selection Priority

A Worker configures its transports in a deterministic order, typically:

1. `InProcessTransport`
2. `IPCTransport`
3. `GrpcTransport`

`WorkerRouter` respects this order when choosing a transport. This ensures:

- Local nodes are always handled locally when possible.
- Same-machine, other-process nodes go through IPC.
- Cross-machine nodes use gRPC as a fallback.

Address classification (local / IPC / remote) is provided by the Cluster, which maintains the address book and helper methods (e.g. `is_remote_address`).

---

### 6.9 Error Handling and WorkerState Interaction

Transports and routing interact with the WorkerState machine as follows:

- **Ingress rules**:
  - Ingress of events and promises is allowed in Worker states:
    - `READY`
    - `ACTIVE`
    - `PAUSED`
  - Ingress is rejected (or immediately failed) in states:
    - `CREATED`
    - `AVAILABLE`
    - `INITIALIZING`
    - `TERMINATED`
    - `BROKEN`

- **Delivery failures in IPC or gRPC**:
  - Transient failures SHOULD be retried as appropriate (IPC retries are implementation-defined; gRPC retries for promises follow Section 6.7.4).
  - If a transport concludes that a message cannot be delivered (permanent error, deadline exceeded, unreachable remote):
    - The failure MUST be clearly logged.
    - The sending Worker MUST transition to state `BROKEN`.
    - The reason for entering `BROKEN` SHOULD be published via the Cluster.

- **In-process delivery errors**:
  - Errors during in-process delivery (e.g. unknown `target_node`, `receive_event` raising unexpectedly) SHOULD generally cause the Worker to transition to `BROKEN`, since they indicate serious internal misconfiguration or logic errors.

The overarching rule is that message delivery failures are never silently ignored: if the system cannot guarantee delivery as designed, the affected Worker must be considered broken and require restart or operator intervention.

---

### 6.10 Testing Guidelines

Tests for routing and transport SHOULD validate at least the following:

- **NodeController interactions**
  - Correct resolution of `"self"` targets inside NodeController (local vs remote).
  - Proper construction of `EventEnvelope` and `PromiseEnvelope`.

- **Routing and transport selection**
  - `InProcessTransport.handles_node` returns `True` only for nodes hosted in the same process.
  - `IPCTransport.handles_node` returns `True` only for nodes reachable via IPC queues.
  - `GrpcTransport.handles_node` returns `True` only for nodes whose addresses are classified as remote.
  - `WorkerRouter` chooses the correct transport based on:
    - Locality of the target node.
    - Transport priority order.

- **IPC behavior**
  - Inline vs shared-memory payload selection based on `large_payload_threshold`.
  - Correct reconstruction of `EventEnvelope` and `PromiseEnvelope` in the IPC receiver.
  - Proper handling of missing nodes or addresses (clear logging and/or transition to `BROKEN`).

- **gRPC behavior**
  - Correct mapping between internal envelopes and protobuf messages.
  - Stable behavior of `SendEvents` streams under load.
  - Unary `SendPromise` behavior:
    - Retry logic following `promise_retry_delays_s` and `promise_retry_max_window_s`.
    - Transition to `BROKEN` on persistent delivery failure.
  - Inbound integration: gRPC ingress delivering envelopes into NodeController queues, honoring WorkerState ingress rules.

- **WorkerState integration**
  - Ingress acceptance in `READY`, `ACTIVE`, `PAUSED`.
  - Rejection of ingress in `CREATED`, `AVAILABLE`, `INITIALIZING`, `TERMINATED`, `BROKEN`.
  - Correct Worker transition to `BROKEN` on unrecoverable delivery failures.

Together, these tests ensure that routing and transport behave predictably across in-process, IPC, and gRPC paths, and that Workers react safely and transparently to communication failures.

    
## 7. Layered Graph and Scenario Subsystem

### 7.1 Overview and Goals

The graph subsystem provides a high‑performance, layered directed acyclic graph (DAG) abstraction on top of a relational database, with in‑memory acceleration using `python-graphblas`. It is designed as a subpackage of `disco` and is intended for large graphs with:

- Many vertices and edges per scenario
- Relatively small structural footprint in memory
- Potentially huge amounts of vertex and edge *data* stored in the database

Key goals:

- **Performance and scalability**
  - Store only structural information (adjacency, labels, masks) in memory using GraphBLAS.
  - Avoid Python loops over vectors/matrices; use GraphBLAS and NumPy operations instead.
  - Minimize database round trips and enable efficient joins via persisted masks.
- **Database abstraction**
  - Use SQLAlchemy Core for all DB interactions (queries, inserts, deletes).
  - Optimize for PostgreSQL but remain engine‑agnostic where possible.
- **Clear separation of concerns**
  - Graph structure and metadata in a dedicated DB schema (`graph`).
  - Vertex/edge *data* tables are user‑defined and live in the default schema (or an explicitly chosen schema).
- **Support for scenarios and labels**
  - Multiple scenarios in the same database, each with its own graph.
  - Label system for vertices, integrated into the graph structure and usable in graph algorithms.
- **Masking and selection**
  - Efficient vertex masks for subgraph selection and DB queries.
  - Masks persisted temporarily in the DB for use in `JOIN`s rather than large `IN` lists.

The public API of the subpackage intentionally exposes only the `Graph` class. All other modules (`db`, `extract`, `graph_mask`, `schema`, etc.) are internal implementation details.


### 7.2 Data Model and Database Schema

The graph structure is stored in a dedicated schema `graph`. Core tables:

- **`graph.scenarios`**
  - Represents a complete scenario (graph instance) in the database.
  - Columns:
    - `id: BIGINT, PK, autoincrement`
    - `name: TEXT, unique, not null`
    - `created_at: TIMESTAMP, not null`
    - `base_scenario_id: BIGINT, FK -> graph.scenarios.id, nullable`
    - `description: TEXT, nullable`

- **`graph.vertices`**
  - Maps scenario‑local vertex indices to optional domain identifiers.
  - Columns:
    - `scenario_id: BIGINT, PK, FK -> graph.scenarios.id`
    - `vertex_index: BIGINT, PK` — dense index `0..num_vertices-1` per scenario
    - `entity_id: TEXT, nullable` — optional external identifier
    - `name: TEXT, not null` — human‑readable vertex name

- **`graph.edges`**
  - Directed weighted edges by scenario and layer.
  - Columns:
    - `scenario_id: BIGINT, PK, FK -> graph.scenarios.id`
    - `layer_id: INT, PK` — layer index; each layer is acyclic
    - `source_idx: BIGINT, PK` — source vertex index
    - `target_idx: BIGINT, PK` — target vertex index
    - `weight: DOUBLE PRECISION, not null`
    - `name: TEXT, not null` — unique edge name within scenario + layer

- **`graph.labels`**
  - Label definitions per scenario. Labels are *global* within a scenario and indexed `0..num_labels-1` in the Graph object.
  - Columns:
    - `id: BIGINT, PK, autoincrement`
    - `scenario_id: BIGINT, FK -> graph.scenarios.id, not null`
    - `type: TEXT, not null` — label type (e.g. "region", "category")
    - `value: TEXT, not null` — label value (e.g. "EMEA", "A")  

- **`graph.vertex_labels`**
  - Assigns labels to vertices.
  - Columns:
    - `scenario_id: BIGINT, PK, FK -> graph.scenarios.id`
    - `vertex_index: BIGINT, PK`
    - `label_id: BIGINT, PK, FK -> graph.labels.id`

- **`graph.vertex_masks`**
  - Temporary storage for vertex masks (subgraphs) to support fast DB queries via joins.
  - Columns:
    - `scenario_id: BIGINT, PK`
    - `mask_id: CHAR(36), PK` — UUID string
    - `vertex_index: BIGINT, PK`
    - `updated_at: TIMESTAMP, not null`

A helper function `create_graph_schema(engine)`:

- Ensures `graph` schema exists (for PostgreSQL: `CREATE SCHEMA IF NOT EXISTS graph`).
- Calls `metadata.create_all()` to create all tables in the `graph` schema.


### 7.3 Graph Structure in Memory

The central in‑memory abstraction is the `Graph` class (`disco.graph.core.Graph`). It is intentionally lean and focused on structure and labels, not on storing arbitrary Python objects.

#### 7.3.1 Core fields

Each `Graph` instance contains:

- `num_vertices: int`  
  Number of vertices in the scenario (`0..num_vertices-1` are valid indices).
- `scenario_id: int`  
  Scenario identifier in the database.
- `_layers: dict[int, gb.Matrix]`  
  Mapping from `layer_id` to adjacency matrix:
  - shape: `(num_vertices, num_vertices)`
  - element type: floating point (e.g. `FP64`)
  - semantics: `A[layer_id][i, j] = weight` if an edge exists from i to j on that layer.
- `_mask: GraphMask | None`  
  Optional vertex mask, wrapping a `Vector[BOOL]` and providing DB persistence when needed.
- `_label_matrix: gb.Matrix | None`  
  Boolean matrix of label assignments:
  - shape: `(num_vertices, num_labels)`
  - rows = vertices
  - columns = labels
  - `True` indicates "vertex has this label".
- `_label_meta: dict[int, tuple[str, str]]`  
  Mapping `label_index -> (label_type, label_value)`. Label indices are dense `0..num_labels-1` and are scenario‑local.
- `_label_type_vectors: dict[str, gb.Vector]`  
  For each label type, a boolean vector over label indices:
  - `label_type_vectors[label_type]` is a `Vector[BOOL]` of size `num_labels`.
  - `True` at position ℓ indicates that label ℓ belongs to this type.
- `num_labels: int`  
  Number of labels attached to the graph (columns in `_label_matrix`).

#### 7.3.2 Construction

Graphs are typically constructed via:

```python
graph = Graph.from_edges(edge_layers, num_vertices=N, scenario_id=scenario_id)
```

where `edge_layers` is a mapping:

```python
edge_layers: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]]  # layer_id -> (src, dst, weight)
```

Internally, `from_edges` uses `gb.Matrix.from_coo()` to build one adjacency matrix per layer. No Python loops over edges are used beyond building NumPy arrays.

The `Graph` class exposes read‑only accessors:

- `get_matrix(layer_id) -> gb.Matrix`
- `get_out_edges(layer_id, vertex_index) -> gb.Vector` (row slice)
- `get_in_edges(layer_id, vertex_index) -> gb.Vector` (column slice)

All of these return GraphBLAS collections, not Python lists or dicts.


### 7.4 Masks and Subgraph Selection

A **vertex mask** is a boolean GraphBLAS vector of length `num_vertices` describing a subgraph (set of vertices). Masks are used for:

- In‑memory algorithms (filtering vertices for GraphBLAS operations).
- Efficient DB queries, by persisting the mask into `graph.vertex_masks` and joining on it.

#### 7.4.1 GraphMask

`GraphMask` is a small wrapper around `Vector[BOOL]` that adds:

- `scenario_id: int`
- `mask_id: str` (UUID)
- `_stored: bool` flag

Key methods:

- `ensure_persisted(session: Session) -> None`  
  - If not stored yet:
    - Deletes any existing rows with the same `(scenario_id, mask_id)`.
    - Extracts `(indices, values)` via `vector.to_coo()` and inserts rows into `graph.vertex_masks` for all `True` indices.
    - Sets `_stored = True`.
  - If already stored:
    - Calls `_touch()` to update `updated_at` via a single `UPDATE`.

- `delete(session: Session) -> None`  
  - Removes all rows for `(scenario_id, mask_id)` from `graph.vertex_masks`.

- `GraphMask.cleanup_old(session: Session, max_age_minutes: int = 60) -> None`  
  - Deletes all masks whose `updated_at` is older than the cutoff.

Masks are not persisted longer than necessary and can be cleaned up periodically. They are intentionally independent of the Graph’s lifetime in the DB, but typical usage is to tie them to a `Graph` instance in memory.


#### 7.4.2 Using masks on Graph

`Graph` exposes masks as GraphBLAS vectors on its public API:

- `graph.set_mask(mask_vector: gb.Vector | None) -> None`  
  - Accepts a `Vector[BOOL]` of size `num_vertices`.
  - Internally wraps it in a `GraphMask` bound to `graph.scenario_id`.
- `graph.mask_vector -> gb.Vector | None`  
  - Returns the underlying GraphBLAS mask vector (if present).

The internal `_graph_mask()` method returns the `GraphMask` object for use by DB helpers.


### 7.5 Labels in the Graph

Labels are a first‑class part of the graph structure and are stored in GraphBLAS collections so they can be used directly in algorithms and selections.

#### 7.5.1 Label representation

Within a scenario:

- Label ids are dense ints `0, 1, 2, …`, independent of label type.
- Label types form **non‑overlapping subsets** of labels.

The representation in `Graph` is:

- `label_matrix: Matrix[BOOL]` with shape `(num_vertices, num_labels)`  
  - `label_matrix[v, ℓ] = True` if vertex `v` has label `ℓ`.
- `label_meta: dict[int, (label_type, label_value)]`  
  - Maps label index ℓ to a `(type, value)` pair.
- `label_type_vectors: dict[str, Vector[BOOL]]`  
  - For each label type, a vector of size `num_labels` with `True` at label indices belonging to that type.

Labels are usually loaded from and stored to the DB via the helpers described in the next section.


#### 7.5.2 Attaching labels to a Graph

Labels are attached using:

```python
graph.set_labels(label_matrix, label_meta, label_type_vectors)
```

Checks performed:

- `label_matrix.dtype` must be boolean (e.g. `gb.dtypes.BOOL`).
- `label_matrix.nrows` must equal `graph.num_vertices`.
- Each type vector must be boolean and have `size == num_labels`.

After this call:

- `graph.num_labels` is set based on `label_matrix.ncols`.
- `graph.label_matrix`, `graph.label_meta`, and `graph.label_type_vectors` are available.


#### 7.5.3 Label‑based masks

The graph supports deriving vertex masks from labels:

- **Mask for a single label id**

  ```python
  mask = graph.get_vertex_mask_for_label_id(label_id: int) -> gb.Vector
  ```

  - Returns a `Vector[BOOL]` of size `num_vertices`.
  - `True` at positions where the vertex has label `label_id`.

- **Mask for all labels of a type**

  ```python
  mask = graph.get_vertex_mask_for_label_type(label_type: str) -> gb.Vector
  ```

  - Uses the boolean matrix‑vector product:

    ```python
    mask = graph.label_matrix.mxv(label_type_vectors[label_type], semiring=gb.semiring.lor_land)
    ```

  - Result is a `Vector[BOOL]` of size `num_vertices`, `True` for vertices with *any* label of the given type.

These masks can be used directly in algorithms or passed to `graph.set_mask()` to drive subsequent data extractions.


### 7.6 DB Integration: Scenarios, Store and Load

Graph structure and labels are integrated with the database via internal helpers in `disco.graph.db`.

#### 7.6.1 Creating scenarios

Scenarios are created using:

```python
scenario_id = create_scenario(
    session,
    name="baseline",
    base_scenario_id=None,
    description="Initial scenario",
)
```

The caller is responsible for populating `graph.vertices` with vertex indices and names for that scenario.


#### 7.6.2 Storing a Graph to the database

The canonical method is:

```python
store_graph(session, graph, store_edges=True, store_labels=True)
```

Behaviour:

- If `store_edges=True`:
  - Deletes all existing rows in `graph.edges` for `graph.scenario_id`.
  - For each layer, extracts `(rows, cols, vals)` via `Matrix.to_coo()`.
  - Inserts edges `(scenario_id, layer_id, source_idx, target_idx, weight, name)`.
- If `store_labels=True` and `graph.label_matrix` is not `None`:
  - Deletes all existing label assignments and labels for `graph.scenario_id` from `graph.vertex_labels` and `graph.labels`.
  - Inserts new label rows from `graph.label_meta`, one per label index.
    - Assigns DB ids and tracks mapping `label_index -> labels.id`.
  - Extracts `(rows, cols, vals)` from `label_matrix.to_coo()` and inserts into `graph.vertex_labels`:
    - `vertex_index = row`
    - `label_id = mapped DB id for label_index = col`

A backwards‑compatible helper `store_graph_edges(session, graph)` calls `store_graph` with both `store_edges=True` and `store_labels=True`.


#### 7.6.3 Loading a Graph from the database

Graphs are loaded via:

```python
graph = load_graph_for_scenario(session, scenario_id)
```

Steps:

1. Determine `num_vertices`:
   - Query `max(vertex_index)` from `graph.vertices` for the scenario.
   - Set `num_vertices = max_index + 1` (or `0` if no vertices).

2. Load edges for the scenario and assemble per‑layer arrays:
   - Query `graph.edges` for `(layer_id, source_idx, target_idx, weight)`.
   - Group rows by `layer_id` and build NumPy arrays.
   - Build `edge_layers: dict[int, (src_array, dst_array, weight_array)]`.

3. Load labels and assignments:
   - Query all labels for the scenario from `graph.labels`.
   - Sort by `labels.id` to build a deterministic mapping `db_label_id -> label_index` (`0..num_labels-1`).
   - Build `label_meta` mapping label_index → `(type, value)`.
   - For each label type, collect all label indices and build a `Vector[BOOL]` over label indices (`label_type_vectors`).  
   - Query `graph.vertex_labels` (assignments) and convert to `(vertex_index, label_index)` pairs (via the `db_label_id -> label_index` mapping).
   - Build `label_matrix: Matrix[BOOL]` from `Matrix.from_coo()` with `nrows=num_vertices` and `ncols=num_labels`.

4. Construct the `Graph` instance:

   ```python
   graph = Graph(
       layers=edge_layers,
       num_vertices=num_vertices,
       scenario_id=scenario_id,
       label_matrix=label_matrix,
       label_meta=label_meta,
       label_type_vectors=label_type_vectors,
   )
   ```


### 7.7 Data Extraction API

While user‑defined vertex and edge *data* tables are not part of the graph schema, the `Graph` provides thin methods that delegate to internal helpers in `disco.graph.extract` to retrieve data as pandas DataFrames or GraphBLAS matrices.

The user must provide SQLAlchemy `Table` objects that satisfy some minimal schema conventions.


#### 7.7.1 Vertex data

Requirement on the user data table:

- Columns `scenario_id` and `vertex_index` with the same semantics as `graph.vertices`.

API:

```python
df = graph.get_vertex_data(
    session,
    vertex_table=my_vertex_data_table,
    columns=[
        my_vertex_data_table.c.vertex_index,
        my_vertex_data_table.c.demand,
        my_vertex_data_table.c.capacity,
    ],
    mask=optional_mask_vector,  # optional: overrides graph.mask_vector
)
```

Semantics:

- Filters on `vertex_table.c.scenario_id == graph.scenario_id`.
- If a mask is given or set on the graph:
  - The `Vector[BOOL]` is wrapped in a `GraphMask`, persisted to `graph.vertex_masks` via `ensure_persisted()`.
  - The query joins `vertex_table` with `graph.vertex_masks` on `(scenario_id, vertex_index)` and restricts to the mask’s `mask_id`.
- Returns a pandas DataFrame built from the result mappings.


#### 7.7.2 Edge data

Requirement on edge data tables:

- Columns: `scenario_id`, `layer_id`, `source_idx`, `target_idx`.
- Additional columns are arbitrary (e.g. cost, capacity, etc.).

Outbound edge data:

```python
df_out = graph.get_outbound_edge_data(
    session,
    edge_table=my_edge_data_table,
    columns=[
        my_edge_data_table.c.source_idx,
        my_edge_data_table.c.target_idx,
        my_edge_data_table.c.cost,
    ],
    layer_id=0,
    mask=optional_mask_vector,
)
```

Inbound edge data:

```python
df_in = graph.get_inbound_edge_data(
    session,
    edge_table=my_edge_data_table,
    columns=[
        my_edge_data_table.c.source_idx,
        my_edge_data_table.c.target_idx,
        my_edge_data_table.c.cost,
    ],
    layer_id=0,
    mask=optional_mask_vector,
)
```

Semantics:

- Filter by `scenario_id` and `layer_id`.
- When a mask is present:
  - Outbound: join `vertex_masks` on `source_idx` (only edges whose source vertex is in the mask).
  - Inbound: join `vertex_masks` on `target_idx` (only edges whose target vertex is in the mask).
- Return a pandas DataFrame constructed from the result mappings.


#### 7.7.3 Map extraction (GraphBLAS matrices)

For algorithmic use, the subsystem can project the edge structure back into GraphBLAS matrices filtered by masks and layers.

Outbound map:

```python
mat_out = graph.get_outbound_map(
    session,
    layer_id=0,
    mask=optional_mask_vector,
)
```

Inbound map:

```python
mat_in = graph.get_inbound_map(
    session,
    layer_id=0,
    mask=optional_mask_vector,
)
```

Semantics:

- Both calls query `graph.edges` for the scenario and layer, optionally joined with `graph.vertex_masks`:
  - Outbound: join on `source_idx`.
  - Inbound: join on `target_idx`.
- Results are converted to COO arrays and fed into `gb.Matrix.from_coo()`:
  - shape: `(num_vertices, num_vertices)`
  - values: edge weights (currently the only supported value source).
- When no rows are found, an empty sparse matrix of the correct size is returned.


### 7.8 Testing Strategy

The current test coverage focuses on:

1. **Basic graph structure**
   - Constructing graphs from NumPy edge arrays.
   - Verifying adjacency matrix values and row/column slices (`get_matrix`, `get_out_edges`, `get_in_edges`).

2. **Masking semantics**
   - Applying a boolean mask vector via `set_mask`.
   - Ensuring the mask is stored as a `Vector[BOOL]` of correct size.
   - Checking that the adjacency matrices remain full‑sized (mask is selection, not reshape).

3. **Label integration**
   - Attaching a `label_matrix`, `label_meta`, and `label_type_vectors` to a `Graph`.
   - Verifying `num_labels` and matrix dimensions.
   - Testing label‑based masks for both a single label id and a label type.

Future test extensions can include:

- Round‑tripping a graph (structure + labels) through an in‑memory database with `store_graph` and `load_graph_for_scenario`.
- Verifying that masks persisted in `graph.vertex_masks` behave correctly in data extraction queries.
- Performance benchmarks for large graphs and label sets, to validate that no accidental Python loops or object allocations occur in hot paths.
