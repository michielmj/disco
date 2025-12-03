# ENGINEERING_SPEC: Routing & Transport Layer for Distributed Simulation
## 1. Overview

We introduce a routing & transport layer for a distributed simulation engine in the `disco` package.

This layer is responsible for moving events and promises between nodes and simulation processes (“simprocs”) across:

- The same node (local delivery, handled entirely by `NodeController`)
- The same machine but different processes (IPC)
- Different machines / applications (gRPC)

This spec covers:

- **Iteration 1**: Core envelopes, `NodeController`, `ServerRouter`, in-process transport
- **Iteration 2**: IPC transport using `multiprocessing.Queue` + `SharedMemory`

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

### Purpose

The **Cluster** component provides a high-level, in-process view of the running simulation cluster, backed by the Metadata Store (ZooKeeper).

Cluster is responsible for:

- Tracking which servers are currently active in the system.
- Tracking which nodes (and replications) are hosted on which server.
- Providing an address book that maps nodes/replications to a network address usable by transports.
- Exposing the address of the local application process.

Cluster is read-mostly from the perspective of the routing/transport layer; it is updated asynchronously in response to ZooKeeper watches.

### Address Book

Cluster exposes a read-only property, conceptually:

```python
address_book: Mapping[tuple[int, str], str]  # (repid, node) -> address (e.g. "host:port")
```

Where:

- `repid` is the replication id of the experiment.
- `node` is the node name.
- `address` is a logical endpoint string for the server that currently hosts that node/replication.
  - Multiple processes belonging to the same logical server may share the same address.
  - Each application process that can receive IPC messages should have a unique address.

The routing and transport layer uses this address book to determine:

- Whether a given `(repid, node)` is local to the current process.
- Whether a given `(repid, node)` is reachable via IPC queues.
- Otherwise, that the node must be reached via gRPC.

### Local Address

Each process/application that participates in the cluster must know its own address:

```python
local_address: str  # same format as in address_book values
```

Transports use `local_address` to decide whether a node is local to this process or must be reached via IPC/gRPC.

### Consistency & Failure Semantics

- Cluster maintains its internal state using a lock to ensure thread-safety.
- Data is reconstructed on each process startup from the Metadata Store.
- Temporary inconsistencies (e.g. during ZooKeeper reconnects) may cause lookups to fail or return stale results; routing should handle this defensively (for example, by raising errors and letting higher layers retry or fail-fast).
- Cluster itself does not perform any routing or IPC/gRPC operations; it is a pure metadata component used by transports and schedulers.


## 5 Routing & Transport

This chapter describes the unified design of routing and transport for events and promises across:

- Same node / same process
- Same machine but different processes (IPC)
- Different machines (gRPC)

The design builds on the concepts in Chapters 1–4 (nodes, simprocs, envelopes, Metadata Store, Cluster).

### 5.1 Envelopes

Events and promises are moved between processes via small, immutable envelopes defined in `src/disco/envelopes.py`:

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

Notes:

- `target_node` is the resolved node name (no `"self"`).
- `data` is already serialized to bytes by the NodeController.
- `headers` are opaque metadata; use `{}` when no headers are provided.

### 5.2 NodeController

**File:** `src/disco/node_controller.py`

A `NodeController` owns all simprocs for a single node and is responsible for:

- Serializing outbound event payloads.
- Deciding whether a target is local or remote.
- Constructing envelopes for remote delivery.
- Handling inbound envelopes via `receive_event` and `receive_promise`.

Constructor (simplified):

```python
from typing import Any, Callable
from .router import ServerRouter


class NodeController:
    def __init__(
        self,
        node_name: str,
        router: ServerRouter,
        serializer: Callable[[Any], bytes],
    ) -> None:
        self._node_name = node_name
        self._router = router
        self._serializer = serializer
```

Public send API:

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

Behaviour:

1. Parse `target` as `<target_node>/<target_simproc>`.
2. If `target_node == "self"`, replace with `self._node_name`.
3. For events, serialize `data` with `self._serializer`.
4. If `target_node == self._node_name`:
   - Call `_deliver_local_event` or `_deliver_local_promise`.
5. Otherwise:
   - Create an `EventEnvelope` or `PromiseEnvelope`.
   - Delegate to `ServerRouter`.

Local delivery hooks are intentionally left to the simulation layer:

```python
def _deliver_local_event(
    self,
    target_simproc: str,
    epoch: float,
    data: bytes,
    headers: dict[str, str],
) -> None:
    # Deliver an event to a local EventQueue for the given simproc.
    raise NotImplementedError


def _deliver_local_promise(
    self,
    target_simproc: str,
    seqnr: int,
    epoch: float,
    num_events: int,
) -> None:
    # Deliver a promise to a local EventQueue for the given simproc.
    raise NotImplementedError
```

Receive methods simply call the local delivery hooks:

```python
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

### 5.3 Router

**File:** `src/disco/router.py`

The `ServerRouter` owns one or more transports and chooses which one to use for a given envelope based on:

- The target node.
- The current replication id (`repid`).
- Cluster metadata (address book and local address).

Conceptual skeleton:

```python
from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.base import Transport
from .cluster import Cluster  # conceptual import


class ServerRouter:
    def __init__(
        self,
        cluster: Cluster,
        transports: list[Transport],
        repid: int,
    ) -> None:
        self._cluster = cluster
        self._transports = transports
        self._repid = repid

    def _choose_transport(self, target_node: str) -> Transport:
        for transport in self._transports:
            if transport.handles_node(self._repid, target_node):
                return transport
        raise KeyError(f"No transport can handle node {target_node!r} for repid {self._repid}")

    def send_event(self, envelope: EventEnvelope) -> None:
        transport = self._choose_transport(envelope.target_node)
        transport.send_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        transport = self._choose_transport(envelope.target_node)
        transport.send_promise(envelope)
```

### 5.4 Transport Interface & Selection

**File:** `src/disco/transports/base.py`

All transports implement a simple protocol plus a capability query:

```python
from typing import Protocol
from ..envelopes import EventEnvelope, PromiseEnvelope


class Transport(Protocol):
    def handles_node(self, repid: int, node: str) -> bool: ...
    def send_event(self, envelope: EventEnvelope) -> None: ...
    def send_promise(self, envelope: PromiseEnvelope) -> None: ...
```

Typical policies:

- `InProcessTransport` handles nodes that are local to this process.
- `IPCTransport` handles nodes reachable via local IPC queues.
- `GrpcTransport` (future) handles nodes on other machines.

### 5.5 In-Process Transport

**File:** `src/disco/transports/inprocess.py`

The in-process transport is the simplest: it directly calls the `NodeController` of the target node.

```python
from typing import Mapping
from ..envelopes import EventEnvelope, PromiseEnvelope
from ..node_controller import NodeController
from .base import Transport
from ..cluster import Cluster


class InProcessTransport(Transport):
    def __init__(
        self,
        nodes: Mapping[str, NodeController],
        repid: int,
        cluster: Cluster,
    ) -> None:
        self._nodes = nodes
        self._repid = repid
        self._cluster = cluster

    def handles_node(self, repid: int, node: str) -> bool:
        if node not in self._nodes:
            return False
        addr = self._cluster.address_book.get((repid, node))
        return addr == self._cluster.local_address

    def send_event(self, envelope: EventEnvelope) -> None:
        node = self._nodes[envelope.target_node]
        node.receive_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        node = self._nodes[envelope.target_node]
        node.receive_promise(envelope)
```

### 5.6 IPC Transport (Queues + SharedMemory)

IPC is used when the target node lives on the same machine but in a different process. Each process maintains inbound queues keyed by address:

```python
import multiprocessing
from typing import Mapping

event_queues: Mapping[str, multiprocessing.Queue]    # address -> inbound event queue
promise_queues: Mapping[str, multiprocessing.Queue]  # address -> inbound promise queue
```

Each application process has its own `address` (for example `"127.0.0.1:5001"`). The IPC transport is constructed with:

- `cluster` (for `address_book` and `local_address`),
- `event_queues` and `promise_queues` for all addresses on the same machine that are reachable via IPC,
- a `large_payload_threshold` for deciding when to use `SharedMemory`.

#### IPC Message Types

**File:** `src/disco/transports/ipc_messages.py`

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

For events, exactly one of `data` or `shm_name` must be non-`None`. Receivers always reconstruct a `bytes` payload before constructing an `EventEnvelope`.

#### IPC Transport Implementation

**File:** `src/disco/transports/ipc_egress.py`

Implements `Transport` for sending to remote processes on the same machine.

Constructor (conceptual):

```python
from multiprocessing import Queue
from typing import Mapping
from ..cluster import Cluster
from .base import Transport
from .ipc_messages import IPCEventMsg, IPCPromiseMsg


class IPCTransport(Transport):
    def __init__(
        self,
        cluster: Cluster,
        event_queues: Mapping[str, Queue],
        promise_queues: Mapping[str, Queue],
        large_payload_threshold: int = 64 * 1024,
    ) -> None:
        self._cluster = cluster
        self._event_queues = event_queues
        self._promise_queues = promise_queues
        self._large_payload_threshold = large_payload_threshold
```

Method `handles_node` decides if a node should be reached via IPC:

```python
def handles_node(self, repid: int, node: str) -> bool:
    addr = self._cluster.address_book.get((repid, node))
    if addr is None:
        return False
    if addr == self._cluster.local_address:
        return False
    return addr in self._event_queues and addr in self._promise_queues
```

`send_event`:

1. Look up `addr = cluster.address_book[(repid, envelope.target_node)]`.
2. Get `q = self._event_queues[addr]`.
3. If `len(envelope.data) <= large_payload_threshold`:
   - Build `IPCEventMsg` with `data=envelope.data`, `shm_name=None`.
4. Otherwise:
   - Allocate a `SharedMemory` block, copy `envelope.data`, and build `IPCEventMsg` with `data=None` and `shm_name=shm.name`.
5. Put the `IPCEventMsg` on `q`.

`send_promise` builds an `IPCPromiseMsg` and puts it on `self._promise_queues[addr]`.

#### IPC Receiver

**File:** `src/disco/transports/ipc_receiver.py`

Each process runs receiver loops for its own inbound queues:

```python
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

Event loop:

1. Block on `event_queue.get()` for an `IPCEventMsg`.
2. If `msg.shm_name is None`:
   - Use `msg.data` as the payload.
3. Otherwise:
   - Attach to `SharedMemory(name=msg.shm_name)`.
   - Copy `bytes(shm.buf[:msg.size])`.
   - Close and unlink the shared memory segment.
4. Construct an `EventEnvelope` from the message and payload.
5. Look up `node = self._nodes[msg.target_node]` and call `node.receive_event(envelope)`.

Promise loop is analogous for `IPCPromiseMsg` and `receive_promise`. Unknown nodes should raise `KeyError`.

### 5.7 gRPC Transport (Placeholder)

The gRPC transport is responsible for delivering envelopes to nodes on other machines. Its design is intentionally left out of scope here, but the selection rule is:

- A node is handled by gRPC if:
  - `handles_node` returns `True` for the gRPC transport, and
  - Its address is neither `local_address` nor present in the IPC queue mappings.

Promises should be prioritized over events on the wire (for example via separate streams or explicit prioritisation in the client).

### 5.8 Testing Guidelines

Tests should cover at least:

- Correct resolution of `"self"` targets in `NodeController`.
- Local versus remote routing decisions based on `Cluster.address_book` and `local_address`.
- `Transport.handles_node` implementations for in-process, IPC, and gRPC transports.
- Inline versus SharedMemory payload handling in the IPC transport.
- Correct reconstruction of `EventEnvelope` and `PromiseEnvelope` in the IPC receiver.
- Clear, deterministic error behaviour when nodes or addresses are missing.
