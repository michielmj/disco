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
