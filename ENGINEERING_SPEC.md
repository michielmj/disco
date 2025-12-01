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

See original spec: tests should cover local vs remote behaviour, "self" alias, serializer usage, and InProcessTransport delivery and error cases.

## 7. Iteration 2 – IPC Transport

### 7.1 Goal

Implement an IPC transport for communication between Server processes on the same machine using:

- `multiprocessing.Queue` for control messages and small payloads
- `multiprocessing.shared_memory.SharedMemory` for large payloads

This transport allows `NodeController` instances in different OS processes to exchange events and promises efficiently.

### 7.2 File Layout

- `src/disco/transports/ipc.py`
- `tests/test_ipc_transport.py`

### 7.3 IPCTransport Responsibilities

`IPCTransport` implements `Transport` and is intended for inter-process communication between NodeControllers on the same machine.

Constructor signature:

```python
def __init__(
    self,
    nodes: Mapping[str, NodeController],
    event_queue: multiprocessing.Queue,
    promise_queue: multiprocessing.Queue,
    large_payload_threshold: int = 64 * 1024,
) -> None:
    ...
```

Responsibilities:

- `send_event`:
  - Decide inline vs shared memory based on `len(envelope.data)` and `large_payload_threshold`.
  - Enqueue an IPC message on `event_queue` describing how to reconstruct the payload.
- `send_promise`:
  - Enqueue a small IPC message on `promise_queue`.
- Receiver loops (helper functions):
  - Block on the queues, reconstruct envelopes, and call `NodeController.receive_event` / `receive_promise`.

### 7.4 IPC Message Structures

```python
from dataclasses import dataclass
from typing import Optional, Dict


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

- Exactly one of `data` or `shm_name` must be non-`None`.
- `size` is always the payload length in bytes.
- Receiver always reconstructs to a `bytes` object before delivering to `NodeController`.

### 7.5 SharedMemory Handling

#### Sender (large payload)

When `len(envelope.data) > large_payload_threshold`:

1. Allocate a `SharedMemory` block sized to `len(envelope.data)`.
2. Copy `envelope.data` into the shared memory buffer.
3. Put an `IPCEventMsg` onto `event_queue` with `data=None`, `shm_name` set to the shared memory name, and `size` set to the payload size.

Sender closes the shared memory but does not unlink it.

#### Receiver

For messages with `shm_name` not `None`:

1. Attach to `SharedMemory` by name.
2. Create `bytes(shm.buf[:size])` as the payload.
3. Close and unlink the shared memory block.
4. Construct an `EventEnvelope` with the reconstructed bytes.
5. Look up the appropriate `NodeController` in the `nodes` mapping and call `receive_event`.

### 7.6 Threshold Behaviour

```python
LARGE_PAYLOAD_THRESHOLD_DEFAULT = 64 * 1024  # 64 KiB
```

- If `len(data) <= large_payload_threshold`: inline path: `IPCEventMsg.data` is the payload, `shm_name=None`.
- If `len(data) > large_payload_threshold`: shared memory path as described above.

### 7.7 Receiver Loops

Helper functions:

```python
def run_ipc_event_receiver(
    nodes: Mapping[str, NodeController],
    event_queue: multiprocessing.Queue,
) -> None:
    ...


def run_ipc_promise_receiver(
    nodes: Mapping[str, NodeController],
    promise_queue: multiprocessing.Queue,
) -> None:
    ...
```

Unknown node in `nodes` should result in a `KeyError` (same behaviour as `InProcessTransport`).

### 7.8 IPC Transport – Testing Strategy

**File:** `tests/test_ipc_transport.py`

Key test cases:

1. Small event inline:
   - Use small payload below threshold.
   - Call `IPCTransport.send_event(...)`.
   - Manually `get()` an `IPCEventMsg` from `event_queue` and simulate receiver logic.
   - Assert `data` is not `None`, `shm_name` is `None`, and reconstructed delivery is correct.

2. Large event via shared memory:
   - Configure a small `large_payload_threshold` to force shared memory path.
   - Call `send_event` with a larger payload.
   - Inspect `IPCEventMsg` to confirm `data is None` and `shm_name` is not `None`.
   - Simulate receiver: attach to shared memory, copy bytes, unlink, deliver to `NodeController`.
   - Assert payload integrity.

3. Promise send/receive:
   - `send_promise` should enqueue an `IPCPromiseMsg` on `promise_queue`.
   - Simulate receiver to reconstruct a `PromiseEnvelope` and call `receive_promise`.

4. Unknown node:
   - Create an `IPCEventMsg` for a node not in `nodes`.
   - Simulate receiver logic and assert that `KeyError` is raised.

