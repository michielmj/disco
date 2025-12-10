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


## 6 Routing and Transports

### 6.1 Purpose and Scope

This chapter describes how messages (events and promises) are routed between
nodes and workers in a simulation cluster. It covers:

- The **WorkerRouter**, which chooses a transport for each outgoing message.
- The abstract **Transport** interface.
- Concrete transports:
  - **InProcessTransport** (same-process delivery via `NodeController`).
  - **IPCTransport** (inter-process communication via queues + shared memory).
  - **GrpcTransport** (remote communication between workers over gRPC).
- The **gRPC ingress** service that receives envelopes from remote workers and
  injects them into the local IPC queues.

The goal is to provide a layered, extensible routing architecture where:

- Local messages are delivered as cheaply as possible.
- Intra-host messages use efficient IPC channels.
- Cross-host messages use gRPC with clear retry semantics.
- The routing policy is determined by a single place (the `WorkerRouter`),
  not scattered throughout the codebase.


### 6.2 Addressing and Locality Model

Routing decisions are based on the **address book** maintained by `Cluster`
(chapter 4):

- `Cluster.address_book: Mapping[tuple[UUID, str], str]`

Where each entry maps:

- `(repid, node_name) -> address`

with:

- `repid: UUID4` — replication id of the experiment.
- `node_name: str` — name of the node within the scenario graph.
- `address: str` — logical worker address, e.g. `"host:port"` or any other
  process-unique identifier.

Each worker:

- Registers itself under a unique **worker address** (chapter 4).
- Ensures that for all nodes it currently hosts in replication `repid`, the
  address book entry points to its own address.

Transports interpret an address as follows:

- **InProcessTransport**: the address must match the local worker address and
  the node must have a local `NodeController` instance.
- **IPCTransport**: the address must be present in the local IPC queue maps
  (`event_queues` and `promise_queues`).
- **GrpcTransport**: any address present in the address book is considered
  routable via gRPC; its exact meaning is a gRPC target string such as
  `"host:port"`.


### 6.3 WorkerRouter

The **WorkerRouter** is a worker-local component responsible for selecting a
transport for each outgoing envelope.

#### 6.3.1 Responsibilities

- Own an ordered list of `Transport` instances.
- For each outgoing `EventEnvelope` or `PromiseEnvelope`:
  - Ask each transport in order whether it **handles** the target node for
    the current `repid`.
  - Use the first transport that responds positively.
  - Raise an error if no transport claims responsibility.

The priority order is determined by the worker during construction, typically:

1. `InProcessTransport`
2. `IPCTransport`
3. `GrpcTransport`

This ensures that the cheapest delivery mechanism is chosen when multiple
transports could technically reach the same node.

#### 6.3.2 Construction

The router is constructed as:

```python
router = WorkerRouter(
    cluster=cluster,
    transports=[inproc, ipc, grpc],
    repid=repid_str,
)
```

Parameters:

- `cluster: Cluster` — used only as a source of `address_book` and locality
  information. The router does not talk directly to the metastore.
- `transports: Sequence[Transport]` — ordered list of transport instances.
- `repid: str` — the replication id used for routing. The worker may change
  this via `router.set_repid()` when its assignment changes.

Internally, the router wraps each transport in a `TransportInfo` structure that
adds a human-readable `name` for logging and debugging.

#### 6.3.3 Routing Logic

For events:

```python
def send_event(self, envelope: EventEnvelope) -> None:
    transport = self._choose_transport(envelope.target_node)
    transport.transport.send_event(self._repid, envelope)
```

For promises:

```python
def send_promise(self, envelope: PromiseEnvelope) -> None:
    transport = self._choose_transport(envelope.target_node)
    transport.transport.send_promise(self._repid, envelope)
```

The private `_choose_transport(target_node: str)` method:

- Iterates transports in priority order.
- Calls `handles_node(self._repid, target_node)` on each transport.
- Returns the first `TransportInfo` whose `handles_node` returns `True`.
- Logs and skips transports that raise exceptions in `handles_node`.
- Raises `RouterError` if none of the transports can handle the node.

This makes misconfiguration or missing address book entries fail fast and in a
single, well-defined place.

#### 6.3.4 Introspection

The router exposes helpers:

- `transports() -> Iterable[Transport]` — underlying transports in priority order.
- `transport_names() -> list[str]` — names of transports in priority order.

These are mainly for diagnostics, tests, and debugging tools.


### 6.4 Transport Interface

All transports implement a common protocol (defined in `transports.base`):

```python
class Transport(Protocol):
    def handles_node(self, repid: str, node: str) -> bool: ...
    def send_event(self, repid: str, envelope: EventEnvelope) -> None: ...
    def send_promise(self, repid: str, envelope: PromiseEnvelope) -> None: ...
```

Semantics:

- `handles_node` must be **pure and fast**: it should not perform blocking I/O.
  It is allowed to consult in-memory data (e.g. the address book or local maps).
- `send_event` and `send_promise` may perform I/O and may raise exceptions on
  failure. Exceptions propagate up to the caller (typically `NodeController`),
  which can decide whether to log, retry, or treat the failure as fatal.

Transport implementations must avoid **double serialization** of payloads.
Payloads are serialized exactly once in `NodeController` before envelopes are
handed to the router.


### 6.5 InProcessTransport

The **InProcessTransport** delivers messages directly to `NodeController`
instances in the same process.

#### 6.5.1 Construction

```python
@dataclass(slots=True)
class InProcessTransport(Transport):
    nodes: Mapping[str, NodeController]
    cluster: Cluster
```

- `nodes` maps node names to their local `NodeController` instances.
- `cluster` is used only for address-book checks in `handles_node`.

#### 6.5.2 Routing Decision

```python
def handles_node(self, repid: str, node: str) -> bool:
    if node not in self.nodes:
        return False
    return (repid, node) in self.cluster.address_book
```

A node is considered in-process if:

- The worker hosts a `NodeController` for that node, and
- The address book has an entry for `(repid, node)` mapping to this worker's
  address (ensured by `Cluster` and `Worker` assignment logic).

#### 6.5.3 Delivery

Events:

```python
def send_event(self, repid: str, envelope: EventEnvelope) -> None:
    node = self.nodes[envelope.target_node]
    node.receive_event(envelope)
```

Promises:

```python
def send_promise(self, repid: str, envelope: PromiseEnvelope) -> None:
    node = self.nodes[envelope.target_node]
    node.receive_promise(envelope)
```

`NodeController.receive_event` and `receive_promise` are responsible for
pushing envelopes into their internal queues and integrating them into the
deterministic runner loop controlled by the `Worker`.


### 6.6 IPC Transport (Queues + Shared Memory)

The **IPCTransport** supports communication between processes on the same host
(or within a tightly coupled environment) via `multiprocessing.Queue` and
optional shared memory for large payloads.

#### 6.6.1 Egress (IPCTransport)

```python
class IPCTransport(Transport):
    def __init__(
        self,
        cluster: Cluster,
        event_queues: Mapping[str, Queue[IPCEventMsg]],
        promise_queues: Mapping[str, Queue[IPCPromiseMsg]],
        large_payload_threshold: int = 64 * 1024,
    ) -> None: ...
```

- `cluster` provides the address book mapping `(repid, node)` to a worker address.
- `event_queues` maps worker addresses to event queues.
- `promise_queues` maps worker addresses to promise queues.
- `large_payload_threshold` determines when to spill payloads into shared
  memory instead of putting them inline on the queue.

Routing decision:

```python
def handles_node(self, repid: str, node: str) -> bool:
    addr = self._cluster.address_book.get((repid, node))
    if addr is None:
        return False
    return addr in self._event_queues and addr in self._promise_queues
```

A node is routable via IPC if both an event queue and a promise queue are
available for its address.

Event send:

- Resolve `addr = address_book[(repid, envelope.target_node)]`.
- If `len(envelope.data) <= large_payload_threshold`:
  - Construct `IPCEventMsg` with inline `data` and `shm_name=None`.
  - Put the message on `event_queues[addr]`.
- Else:
  - Allocate a `SharedMemory` block of size `len(envelope.data)`.
  - Copy `envelope.data` into the buffer.
  - Construct `IPCEventMsg` with `data=None`, `shm_name` set to the shared
    memory name, and `size` set to the payload length.
  - Put the message on the event queue.

Promise send:

- Resolve `addr = address_book[(repid, envelope.target_node)]`.
- Construct `IPCPromiseMsg` with the promise metadata.
- Put the message on `promise_queues[addr]`.


#### 6.6.2 IPC Message Types

```python
@dataclass(slots=True)
class IPCEventMsg:
    target_node: str
    target_simproc: str
    epoch: float
    headers: Dict[str, str]
    data: Optional[bytes]
    shm_name: Optional[str]
    size: int

@dataclass(slots=True)
class IPCPromiseMsg:
    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int
```

- For small payloads, `data` is the serialized bytes and `shm_name` is `None`.
- For large payloads, `data` is `None`, `shm_name` is the name of the shared
  memory handle, and `size` is the actual payload length.

#### 6.6.3 IPCReceiver

`IPCReceiver` provides a small helper for reading from IPC queues and
forwarding envelopes to local `NodeController`s.

```python
class IPCReceiver:
    def __init__(
        self,
        nodes: Mapping[str, NodeController],
        event_queue: Queue[IPCEventMsg],
        promise_queue: Queue[IPCPromiseMsg],
    ) -> None: ...
```

Responsibilities:

- `run_event_loop()` — blocking loop: consume `IPCEventMsg` from `event_queue`,
  reconstruct `EventEnvelope`, and call `NodeController.receive_event`.
- `run_promise_loop()` — blocking loop: consume `IPCPromiseMsg` from
  `promise_queue`, reconstruct `PromiseEnvelope`, and call
  `NodeController.receive_promise`.
- `_extract_event_data(msg: IPCEventMsg) -> bytes` — internal helper to restore
  the payload from inline data or shared memory. It is responsible for closing
  and unlinking the shared memory segment after reading to avoid leaks.

The worker runner thread is expected to coordinate one or more `IPCReceiver`
instances, ensuring that promise handling can be prioritized over events if
desired.


### 6.7 gRPC Transport (Egress)

The **GrpcTransport** is responsible for sending envelopes to remote workers
over gRPC, using the `DiscoTransport` service defined in
`transports/proto/transport.proto`.

#### 6.7.1 Protobuf Service

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
  string message = 1;
}

service DiscoTransport {
  rpc SendEvents(stream EventEnvelopeMsg) returns (TransportAck);
  rpc SendPromise(PromiseEnvelopeMsg) returns (TransportAck);
}
```

#### 6.7.2 Construction and Address Resolution

```python
class GrpcTransport(Transport):
    def __init__(
        self,
        cluster: Cluster,
        settings: GrpcSettings,
        channel_factory: Callable[[str, GrpcSettings], grpc.Channel] | None = None,
        stub_factory: Callable[[grpc.Channel], DiscoTransportStub] | None = None,
    ) -> None: ...
```

- `cluster` provides the address book.
- `settings: GrpcSettings` holds timeout, compression, and retry parameters.
- `channel_factory` (optional) creates a `grpc.Channel` given a target address
  and settings. In production this defaults to a standard `grpc.insecure_channel`
  with configured options (message size limits, keepalive, compression, ...).
- `stub_factory` (optional) creates a `DiscoTransportStub` from a channel.

Address resolution:

```python
def _resolve_address(self, repid: str, node: str) -> str:
    try:
        return self._cluster.address_book[(repid, node)]
    except KeyError as exc:
        raise RuntimeError(f"No address for (repid={repid!r}, node={node!r})") from exc
```

`handles_node` simply checks whether `(repid, node)` exists in the address
book. The router ensures that `GrpcTransport` is only used when higher-priority
transports do not apply.

#### 6.7.3 Event Sending

Events are sent via the `SendEvents` client-streaming RPC. For simplicity,
`GrpcTransport.send_event` currently opens a short-lived stream carrying a
single message:

```python
def send_event(self, repid: str, envelope: EventEnvelope) -> None:
    addr = self._resolve_address(repid, envelope.target_node)
    endpoint = self._get_or_create_endpoint(addr)

    msg = transport_pb2.EventEnvelopeMsg(
        target_node=envelope.target_node,
        target_simproc=envelope.target_simproc,
        epoch=envelope.epoch,
        data=envelope.data,
        headers=envelope.headers,
    )

    def _iter():
        yield msg

    # Timeout and other RPC options are taken from GrpcSettings.
    endpoint.stub.SendEvents(_iter(), timeout=self._settings.timeout_s)
```

`_get_or_create_endpoint(addr)` returns a cached structure containing the
channel and stub for the given target address. Channels and stubs are reused
across calls to minimize connection overhead.

Errors raised by `SendEvents` propagate back to the caller; there is currently
no retry logic for events (they are expected to be retried at a higher layer if
needed).


#### 6.7.4 Promise Sending with Retry

Promises are sent via the unary `SendPromise` RPC. Because promises are small
and critical for synchronization, `GrpcTransport` implements a retry policy
based on `GrpcSettings`:

- `promise_retry_delays_s: list[float]` — backoff sequence between retry
  attempts (in seconds).
- `promise_retry_max_window_s: float` — maximum time window for retries
  (in seconds). Once this window is exceeded, the last error is surfaced.

Algorithm:

1. Resolve `addr` via `_resolve_address(repid, node)`.
2. Obtain `_RemoteEndpoint` (channel + stub) for `addr`.
3. Build `PromiseEnvelopeMsg` with the envelope fields.
4. Call `stub.SendPromise(msg, timeout=settings.timeout_s)`.
5. If the call succeeds, return immediately.
6. If the call fails with a retryable error (e.g. `RESOURCE_EXHAUSTED` or
   `UNAVAILABLE`), wait for the next delay in `promise_retry_delays_s`,
   accumulate elapsed time, and retry as long as the total elapsed time is
   below `promise_retry_max_window_s`.
7. Once the retry window is exceeded or a non-retryable error is encountered,
   re-raise the last exception.

This design keeps retry policy centralized and configurable, while allowing the
worker to treat persistent promise delivery failures as higher-level errors
(e.g. marking remote workers as unhealthy).


### 6.8 gRPC Ingress

The **gRPC ingress** is the server-side implementation of `DiscoTransport`
that receives envelopes from remote workers and injects them into the local
IPC queues.

#### 6.8.1 DiscoTransportServicer

```python
class DiscoTransportServicer(transport_pb2_grpc.DiscoTransportServicer):
    def __init__(self, event_queue: Queue[IPCEventMsg], promise_queue: Queue[IPCPromiseMsg]) -> None: ...

    def SendEvents(self, request_iterator, context):
        # For each EventEnvelopeMsg:
        # - Reconstruct IPCEventMsg
        # - Put it on event_queue
        # - (Shared memory is not used here; large payloads are already
        #   handled on the sending side by IPCTransport.)

    def SendPromise(self, request, context):
        # Reconstruct IPCPromiseMsg and put it on promise_queue.
```

Key points:

- The servicer does **not** talk to `NodeController` or `Worker` directly.
- It performs minimal transformation: protobuf messages → IPC message types.
- Ingress acceptance is not gated by worker state here; state gating and
  delivery prioritization are handled by the local worker and its runner loop.

#### 6.8.2 Server Bootstrap

A helper function starts the gRPC server for a worker:

```python
def start_grpc_server(
    worker_address: str,
    settings: GrpcSettings,
    event_queue: Queue[IPCEventMsg],
    promise_queue: Queue[IPCPromiseMsg],
) -> grpc.Server:
    # Create grpc.Server with a ThreadPoolExecutor(max_workers=settings.max_workers)
    # Configure message size limits and compression from GrpcSettings.
    # Register DiscoTransportServicer(event_queue, promise_queue).
    # Bind to worker_address (e.g. "host:port") via server.add_insecure_port.
    # Start and return the server instance.
```

The worker is responsible for:

- Choosing its own `worker_address` (which must match the address stored in the
  `Cluster.address_book` for its nodes).
- Creating the local IPC queues.
- Starting `IPCReceiver` loops to drain the queues and deliver envelopes to
  `NodeController`s, typically from within or coordinated with the runner loop.

This design ensures a clean separation:

- `GrpcTransport` (egress) → remote worker ingress via gRPC.
- `DiscoTransportServicer` (ingress) → local IPC queues.
- `IPCReceiver` + `Worker` runner loop → deterministic, prioritized delivery
  into `NodeController` instances.


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
