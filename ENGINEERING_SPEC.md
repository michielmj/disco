# ENGINEERING_SPEC: Routing & Transport Layer for Distributed Simulation

## 1. Overview

We introduce a routing & transport layer for a distributed simulation engine in the `disco` package.

This layer is responsible for moving events and promises between nodes and simulation processes (“simprocs”) across:

- The same node (local delivery, handled entirely by `NodeController`)
- The same machine but different processes (IPC)
- Different machines / applications (gRPC)

This spec covers:

- **Iteration 1**: Core envelopes, `NodeController`, `ServerRouter`, in-process transport
- **Iteration 2**: IPC transport using `multiprocessing.Queue` + `SharedMemory`, with a clear **egress/ingress** split

Serialization is done in `NodeController`. No compression logic is implemented in our code; gRPC compression will be configured externally in a later iteration.

All Python source files go under:

- `src/disco/...`


## 2. Concepts & Terminology

- **Node**: logical entity in the simulation. Each node has multiple simprocs.
- **Simproc**: a simulation process (layer in a layered DAG). Same set of simprocs exists on all nodes. Each node will eventually have an EventQueue per simproc (not implemented yet).
- **NodeController**: manages a single node and all its simprocs, including sending and receiving events and promises for that node.
- **Server**: runs multiple `NodeController`s on a single OS thread (details out-of-scope here).
- **Application**: runs multiple Servers in different processes on the same machine.
- **Routing / Transport**: responsible for delivering events/promises between nodes, processes, and machines.

### Addressing

- Consumers are addressed as: `<node>/<simproc>`.
- `target_node` can also be the string `"self"`, meaning “this node”. `NodeController` must interpret `"self"` as its own `node_name`.

### Events and Promises

- **Events**: carry the actual data (up to a few MB, already serialized into `bytes`).
- **Promises**: small control messages used by the EventQueue layer to determine ordering and completeness.

Public send APIs on `NodeController`:

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

Notes:

- `NodeController` holds the full state for the node; it does not need sender info for routing.
- If sender metadata is needed for model logic, it can later be added as headers or optional fields on envelopes.


## 3. Iteration 1 – Scope

### In Scope

1. Definitions of envelopes:
   - `EventEnvelope`
   - `PromiseEnvelope`
2. `NodeController` skeleton:
   - Public methods:
     - `send_event(...)`
     - `send_promise(...)`
     - `receive_event(envelope: EventEnvelope) -> None`
     - `receive_promise(envelope: PromiseEnvelope) -> None`
   - Internal methods:
     - `_deliver_local_event(...)` — must exist but be empty.
     - `_deliver_local_promise(...)` — must exist but be empty.
   - Constructor must not initialize any EventQueues.
3. Router interface and basic implementation:
   - `ServerRouter` with:
     - `send_event(envelope: EventEnvelope) -> None`
     - `send_promise(envelope: PromiseEnvelope) -> None`
   - It will just delegate to a configured transport for now.
4. Transport interfaces:
   - A base `Transport` protocol / abstract class.
   - A simple `InProcessTransport` implementation that delegates to `NodeController.receive_*` methods.
5. Support for `"self"` as a node alias:
   - `NodeController.send_event` and `send_promise` must treat `"self"` as equivalent to `node_name` for determining local vs remote.
6. Basic tests for the above.

### Out of Scope (Iteration 1)

- Actual EventQueue / PromiseQueue implementations.
- IPC transport (`SharedMemory`, `multiprocessing.Queue`, etc.).
- gRPC service definitions and client implementations.
- Address book, replications, and real node location resolution.
- Any compression logic.


## 4. Iteration 1 – File Layout

- `src/disco/envelopes.py`
- `src/disco/node_controller.py`
- `src/disco/router.py`
- `src/disco/transports/base.py`
- `src/disco/transports/inprocess.py`

Tests:

- `tests/test_node_controller.py`
- `tests/test_inprocess_transport.py`


## 5. Iteration 1 – Detailed Design

### 5.1 Envelopes

**File:** `src/disco/envelopes.py`

```python
from __future__ import annotations

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

- `target_node` is the canonical resolved node name (i.e. `"self"` should be resolved to `node_name` before creating these envelopes).
- `headers` are opaque key-value pairs; use `{}` when none are provided.

### 5.2 NodeController

**File:** `src/disco/node_controller.py`

Constructor:

```python
from typing import Any, Callable


class NodeController:
    def __init__(
        self,
        node_name: str,
        router: "ServerRouter",
        serializer: Callable[[Any], bytes],
    ) -> None:
        self._node_name = node_name
        self._router = router
        self._serializer = serializer
        # Do NOT initialize EventQueues here in this iteration.
```

`send_event` behaviour:

1. Parse `target` into `target_node` and `target_simproc` by splitting on the first `/`.
2. If `target_node == "self"`, replace it with `self._node_name`.
3. Serialize `data` using `self._serializer` to `payload: bytes`.
4. If `target_node == self._node_name`, call `_deliver_local_event(target_simproc, epoch, payload, headers or {})`.
5. Else, construct an `EventEnvelope` and call `self._router.send_event(envelope)`.

`send_promise` behaviour:

1. Parse `target` into `target_node` and `target_simproc`.
2. If `target_node == "self"`, replace it with `self._node_name`.
3. If `target_node == self._node_name`, call `_deliver_local_promise(target_simproc, seqnr, epoch, num_events)`.
4. Else, construct a `PromiseEnvelope` and call `self._router.send_promise(envelope)`.

Local delivery methods (empty in this iteration):

```python
def _deliver_local_event(
    self,
    target_simproc: str,
    epoch: float,
    data: bytes,
    headers: dict[str, str],
) -> None:
    return None


def _deliver_local_promise(
    self,
    target_simproc: str,
    seqnr: int,
    epoch: float,
    num_events: int,
) -> None:
    return None
```

Receive methods:

```python
from .envelopes import EventEnvelope, PromiseEnvelope


def receive_event(self, envelope: EventEnvelope) -> None:
    self._deliver_local_event(
        target_simproc=envelope.target_simproc,
        epoch=envelope.epoch,
        data=envelope.data,
        headers=envelope.headers,
    )


def receive_promise(self, envelope: PromiseEnvelope) -> None:
    self._deliver_local_promise(
        target_simproc=envelope.target_simproc,
        seqnr=envelope.seqnr,
        epoch=envelope.epoch,
        num_events=envelope.num_events,
    )
```

### 5.3 ServerRouter

**File:** `src/disco/router.py`

```python
from __future__ import annotations

from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.base import Transport


class ServerRouter:
    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def send_event(self, envelope: EventEnvelope) -> None:
        self._transport.send_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        self._transport.send_promise(envelope)
```

### 5.4 Transport Base & InProcessTransport

**File:** `src/disco/transports/base.py`

```python
from __future__ import annotations

from typing import Protocol

from ..envelopes import EventEnvelope, PromiseEnvelope


class Transport(Protocol):
    def send_event(self, envelope: EventEnvelope) -> None: ...
    def send_promise(self, envelope: PromiseEnvelope) -> None: ...
```

**File:** `src/disco/transports/inprocess.py`

```python
from __future__ import annotations

from typing import Mapping

from ..envelopes import EventEnvelope, PromiseEnvelope
from ..node_controller import NodeController
from .base import Transport


class InProcessTransport(Transport):
    def __init__(self, nodes: Mapping[str, NodeController]) -> None:
        self._nodes = nodes

    def send_event(self, envelope: EventEnvelope) -> None:
        node = self._nodes.get(envelope.target_node)
        if node is None:
            raise KeyError(f"No NodeController registered for node {envelope.target_node}")
        node.receive_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        node = self._nodes.get(envelope.target_node)
        if node is None:
            raise KeyError(f"No NodeController registered for node {envelope.target_node}")
        node.receive_promise(envelope)
```


## 6. Iteration 1 – Testing Strategy

See above: tests should cover local vs remote behaviour, `"self"` alias, serializer usage, and `InProcessTransport` delivery and error cases.


## 7. Iteration 2 – IPC Transport (Egress/Ingress Architecture)

### 7.1 Goal

Implement an IPC transport for communication between Server processes on the same machine using:

- `multiprocessing.Queue` for control messages and small payloads
- `multiprocessing.shared_memory.SharedMemory` for large payloads

Design principles:

- **Egress vs Ingress split**:
  - **Egress**: sender-side, responsible only for writing IPC messages to outbound queues.
  - **Ingress**: receiver-side, responsible for reading IPC messages, reconstructing envelopes, and calling local `NodeController`s.
- **One inbound queue per receiver process (per type)**:
  - All producers share the same inbound event queue and promise queue of a receiver process.
- Each process only owns its **local** `NodeController`s.

### 7.2 File Layout (Iteration 2 additions)

- `src/disco/transports/ipc_messages.py`
- `src/disco/transports/ipc_egress.py`
- `src/disco/transports/ipc_receiver.py`

Tests:

- `tests/test_ipc_transport.py`

### 7.3 IPC Message Structures

**File:** `src/disco/transports/ipc_messages.py`

IPC messages must be picklable and represent only control metadata, plus possibly small payloads:

```python
from dataclasses import dataclass
from typing import Optional, Dict


@dataclass(slots=True)
class IPCEventMsg:
    target_node: str
    target_simproc: str
    epoch: float
    headers: Dict[str, str]
    data: Optional[bytes]        # small payload
    shm_name: Optional[str]      # shared memory name (large payload)
    size: int                    # payload length in bytes


@dataclass(slots=True)
class IPCPromiseMsg:
    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int
```

Rules:

- For events: exactly one of `data` or `shm_name` must be non-`None`.
- `size` is always the payload length in bytes (even for small payloads, for consistency).
- Receiver always reconstructs a `bytes` payload before constructing an `EventEnvelope`.

### 7.4 Process Metadata & Node Placement

A parent/orchestrator process is responsible for deciding:

- Which nodes live in which process:

```python
node_placement = {
    "N1": "P1",
    "N2": "P1",
    "N3": "P2",
}
```

- For each process `Pid`, it creates:
  - `inbound_event_queue_for_Pid: multiprocessing.Queue`
  - `inbound_promise_queue_for_Pid: multiprocessing.Queue`

Each process receives:

- Its own inbound queues (event + promise)
- A mapping of outbound queues to **other** processes:

```python
process_queues: Mapping[str, tuple[multiprocessing.Queue, multiprocessing.Queue]]
# process_id -> (event_queue_for_that_process, promise_queue_for_that_process)
```

- A list of local node names it owns.

Higher-level logic or `ServerRouter` will use `node_placement` to decide which process a given `target_node` belongs to, and hence which outbound queue pair to use.

### 7.5 IPCTransportEgress (Sender Side)

**File:** `src/disco/transports/ipc_egress.py`  
Implements `Transport` and is used only on the **sender side**.

#### Purpose

- Does **not** know about `NodeController`s.
- Does **not** hold a `nodes: Mapping[str, NodeController]`.
- Is given:
  - A mapping `process_id → (event_queue, promise_queue)`.
  - A function or higher-level router mechanism to pick the correct `process_id` for a given `EventEnvelope`/`PromiseEnvelope`.

#### Constructor

```python
import multiprocessing
from typing import Mapping

from .base import Transport
from ..envelopes import EventEnvelope, PromiseEnvelope
from .ipc_messages import IPCEventMsg, IPCPromiseMsg


class IPCTransportEgress(Transport):
    def __init__(
        self,
        process_queues: Mapping[str, tuple[multiprocessing.Queue, multiprocessing.Queue]],
        large_payload_threshold: int = 64 * 1024,
    ) -> None:
        # process_queues maps process_id -> (event_queue, promise_queue)
        self._process_queues = process_queues
        self._large_payload_threshold = large_payload_threshold
```

> The mapping from `target_node` to `process_id` is not handled by this class; it is the responsibility of the caller/router, which must choose the correct process queue pair.

#### Sending events

`send_event(envelope: EventEnvelope) -> None`:

- Determine which process `pid` owns `envelope.target_node` (via caller/router; you may inject a helper or pass `pid` via the envelope wrapper in a higher layer).
- Look up `(event_queue, _) = process_queues[pid]`.
- If `len(envelope.data) <= large_payload_threshold`:
  - Create `IPCEventMsg` with `data=envelope.data`, `shm_name=None`, `size=len(envelope.data)`.
- Else:
  - Allocate `SharedMemory(len(envelope.data))`.
  - Copy bytes into `shm.buf`.
  - Create `IPCEventMsg` with:
    - `data=None`
    - `shm_name=shm.name`
    - `size=len(envelope.data)`
  - Close the `SharedMemory` handle, but **do not unlink**.
- Put the `IPCEventMsg` on `event_queue`.

#### Sending promises

`send_promise(envelope: PromiseEnvelope) -> None`:

- Determine `pid` from `envelope.target_node` (via router).
- Look up `(_, promise_queue) = process_queues[pid]`.
- Create `IPCPromiseMsg` and `put()` it on `promise_queue`.

### 7.6 IPCReceiver (Receiver Side)

**File:** `src/disco/transports/ipc_receiver.py`

Receiver is responsible for:

- Owning the `nodes: Mapping[str, NodeController]` for the local process.
- Running blocking loops on its **own** inbound queues.
- Reconstructing `EventEnvelope` / `PromiseEnvelope` and calling local NodeControllers.

#### Constructor

```python
import multiprocessing
from typing import Mapping

from ..node_controller import NodeController
from ..envelopes import EventEnvelope, PromiseEnvelope
from .ipc_messages import IPCEventMsg, IPCPromiseMsg


class IPCReceiver:
    def __init__(
        self,
        nodes: Mapping[str, NodeController],
        event_queue: multiprocessing.Queue,
        promise_queue: multiprocessing.Queue,
    ) -> None:
        self._nodes = nodes
        self._event_queue = event_queue
        self._promise_queue = promise_queue
```

#### Event loop

`run_event_loop(self) -> None`:

- Infinite loop:
  - `msg: IPCEventMsg = self._event_queue.get()`
  - If `msg.shm_name is None`:
    - `data = msg.data`
  - Else:
    - Attach to `SharedMemory(name=msg.shm_name)`.
    - Copy `data = bytes(shm.buf[:msg.size])`.
    - `shm.close()`
    - `shm.unlink()`
  - Construct `EventEnvelope` from `msg` and `data`.
  - Lookup `node = self._nodes[msg.target_node]`:
    - If not found, raise `KeyError`.
  - Call `node.receive_event(envelope)`.

#### Promise loop

`run_promise_loop(self) -> None`:

- Infinite loop:
  - `msg: IPCPromiseMsg = self._promise_queue.get()`
  - Construct `PromiseEnvelope`.
  - Lookup `node = self._nodes[msg.target_node]`:
    - If not found, raise `KeyError`.
  - Call `node.receive_promise(envelope)`.

### 7.7 Integration with ServerRouter

In each process:

- `ServerRouter` may have **multiple transports**:
  - `InProcessTransport` for nodes owned by this process.
  - `IPCTransportEgress` for nodes owned by other processes on the same machine.
  - (future) gRPC transport for nodes on other machines.

Routing decision example:

- If `target_node` is local to this process:
  - Use `InProcessTransport`.
- Else if `target_node` belongs to another process on the same machine:
  - Use `IPCTransportEgress` with the correct `process_id`.
- Else:
  - Use gRPC transport (future iteration).

`NodeController` remains unaware of remote processes; it just builds envelopes and forwards them to `ServerRouter`.

### 7.8 IPC Transport – Testing Strategy

**File:** `tests/test_ipc_transport.py`

Tests should cover both **egress** and **ingress** sides, plus an end-to-end scenario.

#### 1. Egress: small event inline

- Use a fake `process_queues` mapping with real `multiprocessing.Queue()` instances.
- Set a relatively small `large_payload_threshold` and use an even smaller payload.
- Call `IPCTransportEgress.send_event(envelope)` via a helper that provides a fixed process id.
- `get()` from the respective `event_queue`:
  - Assert `IPCEventMsg.data` is not `None`.
  - Assert `shm_name is None`.
  - Assert `size == len(envelope.data)`.

#### 2. Egress: large event via SharedMemory

- Use a very low `large_payload_threshold` so a moderately sized payload is treated as large.
- After `send_event`, `get()` an `IPCEventMsg`:
  - Assert `data is None`.
  - Assert `shm_name` is not `None`.
  - Assert `size` is set correctly.

#### 3. Ingress: small event reconstruction

- Create a fake `NodeController` that records calls to `receive_event`.
- Put an `IPCEventMsg` with `data` set (and `shm_name=None`) into the inbound event queue.
- Run a single iteration of the event receive logic (either by controlling the loop, or by factoring out a single-step handler).
- Assert:
  - `receive_event` was called once.
  - The `EventEnvelope.data` matches the original bytes.

#### 4. Ingress: large event reconstruction with SharedMemory

- Manually create a `SharedMemory` block, write some bytes into it.
- Create an `IPCEventMsg` with `shm_name` pointing to that block and `size` set.
- Put it into the event queue.
- Run a single iteration of the receive logic.
- Assert:
  - `receive_event` is called with correct data.
  - The shared memory was closed and unlinked (e.g. attaching again fails or you track via internal hooks).

#### 5. Promise path

- Similar tests for `IPCPromiseMsg` and the promise queue:
  - Egress creates and enqueues `IPCPromiseMsg`.
  - Ingress reconstructs a `PromiseEnvelope` and calls `receive_promise`.

#### 6. Unknown node

- Provide a `nodes` mapping that does **not** include `msg.target_node`.
- Put an event or promise IPC message for that missing node in the corresponding queue.
- Run the receive logic and assert a `KeyError` is raised.


## 8 Metadata Store (Zookeeper-Backed)

### 8.1 Purpose

The Metadata Store provides a distributed, fault-tolerant key–value hierarchy used by simulation servers to coordinate state, routing metadata, replication information, and dynamic configuration. It acts as the system-wide source of truth that must be shared across processes and clusters.

### 8.2 Design Overview

The Metastore is a thin, high-level API on top of ZooKeeper accessed through `ZkConnectionManager`. It provides:

- **Hierarchical atomic operations** (`update_key`, `update_keys`, `get_key`, `get_keys`)
- **Optional namespacing via groups** (logical prefixing)
- **Structured expand semantics** for nested metadata trees
- **Watchers** with automatic restoration after reconnect
- **Simple queue primitives** for lightweight inter-node coordination
- **Pluggable serialization** (default: pickle)

Logical paths (e.g., `"servers/s1/state"`) map to ZooKeeper paths via:

```
_full_path(path) =
    "/" + group + "/" + path        if group is set
    "/" + path                      otherwise
```

This allows multi‑tenant simulations on a shared ZooKeeper cluster.

### 8.3 Base Structure

On construction, the Metastore ensures a predefined directory structure (`BASE_STRUCTURE`).  
If a group is configured, creation happens inside that namespace.

This ensures deterministic availability of the simulation’s metadata tree.

### 8.4 Serialization

All values are stored as bytes using:

- `packb(value) -> bytes`
- `unpackb(bytes) -> value`

Users may override with msgpack, JSON, or domain-specific binary formats.

### 8.5 Watch Callbacks

Watches are registered using:

```
watch_with_callback(path, callback)
watch_members_with_callback(path, callback)
```

Callbacks must return:

- **True** → keep watching  
- **False** → stop watching  

The `ZkConnectionManager` automatically restores these watchers after reconnects.

Deletion events produce `raw=None`, which removes the watch by default.

### 8.6 Key–Value Operations

- `update_key(path, value)` – set/overwrite a single key.
- `get_key(path)` – load and deserialize a key.
- `update_keys(path, members, expand=...)` – structured nested update.
- `get_keys(path, expand=...)` – structured nested load.
- `__contains__(path)` – check existence.
- `list_members(path)` – list children.

The Metastore consistently treats nodes as existing if they hold *either* data or children.

### 8.7 Expand Semantics (Hierarchical Read/Write)

`expand` controls how nested dictionaries are expanded into node hierarchies.

Examples:

#### Nested expansion
```
expand = {"replications": {"assignments": None}}
members = {"replications": {"r1": {"assignments": {"a": 1}}}}
```

Writes:
```
/replications/r1/assignments/a = 1
```

#### Shallow expansion
```
expand = {"replications": None}
```

Writes:
```
/replications/assignments = {"a": 1}
```

This allows callers to choose full tree flattening or partially serialized subtrees.

### 8.8 Automatic Parent Node Behavior

ZooKeeper’s model: a node exists if it has *data or children*.  
Our FakeKazooClient matches this behavior to ensure consistent read semantics, including for deeper `expand` operations.

### 8.9 Queue Operations

Queue primitives use ZooKeeper’s recipe:

- `enqueue(path, value)`
- `dequeue(path, timeout=None)`

This is used for small, low‑frequency coordination messages (not bulk data).

### 8.10 Failure & Recovery Semantics

- A single KazooClient per process is owned by `ZkConnectionManager`.
- On session loss:
  - Reconnect is automatic.
  - All watchers are restored.
- `update_keys` and `get_keys` remain consistent under chroot and group namespaces.

### 8.11 Intended Uses

- Registration of active servers
- Routing/replication metadata
- Simulation configuration and parameters
- Address books and ownership mappings
- Coordination of distributed workers
- Bootstrap of stateless server processes

### 8.12 Non‑Goals

- Not suited for large payloads → use SharedMemory or data layer
- Not transactional at subtree level
- Not a metrics repository or log store
- Not a general-purpose database

### 8.13 Testing Requirements

Test using `FakeKazooClient` & `FakeConnectionManager`:

- `expand` semantics (full vs shallow)
- Nested writes and reads
- Watch installation and deletion
- Recovery after simulated reconnect
- Queue operations including timeout
- Group/chroot path correctness
- Mixed scalar/dict storage interactions


## 9 Cluster

The **Cluster** component provides a high‑level abstraction for managing active simulation servers, tracking their state, and maintaining a consistent, fault‑tolerant view of server metadata stored in ZooKeeper through the Metastore.

### 9.1 Purpose

Cluster is responsible for:

- Tracking which servers are currently active in the system.
- Watching ZooKeeper paths for:
  - server registration / unregistration,
  - server state changes (e.g., AVAILABLE → ALLOCATED → ACTIVE),
  - server metadata updates (nodes, repid, expid, partition).
- Maintaining an in‑memory representation of:
  - server states,
  - server nodes,
  - replication IDs,
  - derived address book mappings.
- Providing selection and availability helpers used by the scheduler and routing layer.

### 9.2 Core Architecture

Cluster operates as an in‑memory cache driven entirely by **watch callbacks** on ZooKeeper paths:

- `/simulation/active_servers` — identifies currently registered servers.
- `/simulation/active_servers/<server>` — stores server state (`State` enum).
- `/simulation/servers/<server>/nodes` — stores the list of nodes hosted by the server.
- `/simulation/servers/<server>/repid` — stores the replication ID for the server.

Cluster registers a children‑watch on `/simulation/active_servers`:

- New servers trigger installing of three watch callbacks:
  - Watch server state,
  - Watch server nodes,
  - Watch server repid.
- Removed servers cause internal dictionaries to drop all related cached values.

Cluster maintains three internal dictionaries under a lock:

```
_server_state:   dict[str, State]
_server_nodes:   dict[str, list[str]]
_server_repids:  dict[str, str]
```

These remain consistent as ZooKeeper changes occur.

### 9.3 Registration & Unregistration

Cluster provides:

- `register_server(server: str, state: State)`  
  Creates ephemeral node `/active_servers/<server>` with value = `state`, and initializes a default `ServerInfo` subtree.
- `unregister_server(server: str)`  
  Deletes the ephemeral node, removing the server from the watch list.

### 9.4 State & Metadata Management

Cluster provides helpers:

- `set_server_state(server, state)`  
  Updates the ephemeral node value.
- `get_server_state(server)`  
  Reads it back.
- `update_server_info(server, partition, expid, repid, nodes)`  
  Updates keys under `/servers/<server>/...`
- `get_server_info(server)`  
  Returns a typed `ServerInfo` instance.

All writes are executed via Metastore; Cluster never writes directly to ZooKeeper.

### 9.5 Availability Logic

`get_available(expid)` determines which servers are eligible for assignment:

- Only servers in state `AVAILABLE` are considered.
- Servers matching the requested `expid` and offering unique partitions are **preferred**.
- All other available servers are classified as **fallback candidates**.

Returns:

```
(preferred + others, partitions_of_preferred)
```

### 9.6 Address Book Generation

Read‑only property:

```
address_book: Mapping[(repid, node), server]
```

Recomputed lazily on demand from:

- `_server_nodes`,
- `_server_repids`.

Used by the routing layer to locate which server hosts a given node‑instance pair.

### 9.7 Synchronization

Cluster uses a `threading.Lock` for internal consistency and a `Condition` variable to allow blocking waits:

- `await_available(timeout=None)`  
  Blocks until server availability changes (used by schedulers).

Watch callbacks hold the lock and wake all waiters.

### 9.8 Failure Modes & Constraints

- Cluster assumes Metastore guarantees watch recovery after reconnect.
- If inconsistency occurs (e.g., missing info during a watch callback), the change is ignored until the next event.
- Cluster does not persist state; it is fully reconstructed on each process startup from ZooKeeper.

### 9.9 Non‑Goals

- Cluster does not handle inter‑server communication.
- Cluster does not manage processes, simulation nodes, or event routing.
- Cluster does not provide strong consistency beyond ZooKeeper's guarantees.


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
