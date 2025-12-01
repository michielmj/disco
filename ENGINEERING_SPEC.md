# ENGINEERING_SPEC: Routing & Transport Layer for Distributed Simulation

## 1. Overview

We want to introduce a **routing & transport layer** for a distributed simulation engine in the `disco` package.

This layer is responsible for moving **events** and **promises** between nodes and simulation processes (“simprocs”) across:

- The **same node** (local delivery, handled entirely by `NodeController`),
- The **same machine but different processes** (IPC),
- **Different machines / applications** (gRPC).

This spec covers a **first, partial implementation**:

- **Serialization is done in `NodeController`**.
- **No compression logic in our code**; gRPC compression is configured externally via gRPC options.
- **Local delivery methods are present but empty** (`_deliver_local_event`, `_deliver_local_promise`).
- **No EventQueue initialization** yet (that will be done later).
- Minimal interfaces for transports and router, plus basic envelopes and node controller.

All Python source files go under:

- `src/disco/...`

## 2. Concepts & Terminology

- **Node**: logical entity in the simulation. Each node has multiple simprocs.
- **Simproc**: a *simulation process* (layer in a layered DAG). Same set of simprocs exists on all nodes. Each node will eventually have an EventQueue per simproc (not implemented in this step).
- **NodeController**: manages a single node and all its simprocs, including sending/receiving events and promises for that node.
- **Server**: runs multiple `NodeController`s on a single OS thread (details out-of-scope here).
- **Application**: runs multiple Servers in different processes on the same machine.
- **Routing / Transport**: responsible for delivering events/promises between nodes, processes, and machines.

### Addressing

- Consumers are addressed as: `"<node>/<simproc>"`.
- `target_node` can also be the string `"self"`, meaning “this node”. The `NodeController` must interpret `"self"` as its own `node_name`.

### Events and Promises

- **Events**: carry the actual data (up to a few MB, already serialized into `bytes`).
- **Promises**: small control messages used by the EventQueue layer to determine ordering and completeness.

Event send API (public):

```python
def send_event(
    self,
    target: str,                # "<node>/<simproc>" or "self/<simproc>"
    epoch: float,
    data: Any,
    headers: dict[str, str] | None = None,
) -> None: ...
```

Promise send API (public):

```python
def send_promise(
    self,
    target: str,                # "<node>/<simproc>" or "self/<simproc>"
    seqnr: int,
    epoch: float,
    num_events: int,
) -> None: ...
```

Notes:

- `NodeController` holds the full state for the node; it does **not** need sender info for routing.
- If sender metadata is needed for model logic, it can later be added as headers or optional fields on envelopes.

## 3. Scope of This Iteration

### In Scope

1. **Definitions of envelopes**:
   - `EventEnvelope`
   - `PromiseEnvelope`

2. **NodeController skeleton**:
   - Public methods:
     - `send_event(...)`
     - `send_promise(...)`
     - `receive_event(envelope: EventEnvelope) -> None`
     - `receive_promise(envelope: PromiseEnvelope) -> None`
   - Internal methods:
     - `_deliver_local_event(...)` — **must exist but be empty**.
     - `_deliver_local_promise(...)` — **must exist but be empty**.
   - Constructor must **not** initialize any EventQueues.

3. **Router interface and basic implementation**:
   - `ServerRouter` with:
     - `send_event(envelope: EventEnvelope) -> None`
     - `send_promise(envelope: PromiseEnvelope) -> None`
   - It will:
     - Just delegate to a configured transport for now (no real location resolution yet).

4. **Transport interfaces**:
   - A base `Transport` protocol / abstract class.
   - A simple `InProcessTransport` implementation that delegates to `NodeController.receive_*` methods.

5. **Support for `"self"` as a node alias**:
   - `NodeController.send_event` and `send_promise` must treat `"self"` as equivalent to `node_name` for determining local vs remote.

6. **Basic tests**:
   - Verify NodeController:
     - Interprets `"self"` correctly.
     - Serializes data exactly once per `send_event` call.
     - Calls `_deliver_local_event` / `_deliver_local_promise` for local targets (including `"self"`).
     - Calls `ServerRouter.send_event` / `send_promise` for non-local targets.
   - Verify `InProcessTransport`:
     - Calls `receive_event` / `receive_promise` on the right `NodeController`.

### Out of Scope (for this iteration)

- Actual EventQueue / PromiseQueue implementations.
- IPC transport (`SharedMemory`, `multiprocessing.Queue`, etc.).
- gRPC service definitions and client implementations.
- Address book, replications, and real node location resolution.
- Any compression logic (beyond acknowledging that gRPC will handle compression externally).

## 4. File Layout

All new code goes under `src/disco`:

- `src/disco/envelopes.py`
  - Data structures for `EventEnvelope` and `PromiseEnvelope`.
- `src/disco/node_controller.py`
  - `NodeController` class.
- `src/disco/router.py`
  - `ServerRouter` definition.
- `src/disco/transports/base.py`
  - `Transport` protocol or base class.
- `src/disco/transports/inprocess.py`
  - `InProcessTransport` implementation.

Tests:

- `tests/test_node_controller.py`
- `tests/test_inprocess_transport.py`

## 5. Detailed Design

### 5.1 Envelopes

**File:** `src/disco/envelopes.py`

Implement two lightweight dataclasses with type hints:

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

Notes:

- `target_node` is the **canonical resolved node name** (i.e. `"self"` should be resolved to `node_name` before creating these envelopes).
- `headers` are opaque key-value pairs, optional for now but represented as an empty dict when not provided.

### 5.2 NodeController

**File:** `src/disco/node_controller.py`

Implement a `NodeController` class with the following behaviour.

#### Constructor

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
        # IMPORTANT: Do NOT initialize EventQueues here.
```

- `node_name`: the canonical name of the node.
- `router`: a `ServerRouter` instance used for non-local targets.
- `serializer`: function mapping arbitrary Python objects to `bytes`.

#### `send_event`

```python
def send_event(
    self,
    target: str,
    epoch: float,
    data: Any,
    headers: dict[str, str] | None = None,
) -> None:
    """
    Send an event to the given target "<node>/<simproc>".
    `target_node` may be "self" to indicate this NodeController's own node.
    """
```

Behaviour:

1. Parse `target` into `target_node` and `target_simproc` by splitting on the first `/`.
2. If `target_node == "self"`, replace it with `self._node_name`.
3. Serialize `data` using `self._serializer` → `payload: bytes`.
4. If `target_node == self._node_name`:
   - Call `_deliver_local_event(target_simproc, epoch, payload, headers or {})`.
5. Else:
   - Construct an `EventEnvelope` with:
     - `target_node`
     - `target_simproc`
     - `epoch`
     - `data=payload`
     - `headers=headers or {}`
   - Call `self._router.send_event(envelope)`.

#### `send_promise`

```python
def send_promise(
    self,
    target: str,
    seqnr: int,
    epoch: float,
    num_events: int,
) -> None:
    """
    Send a promise to the given target "<node>/<simproc>".
    `target_node` may be "self" to indicate this NodeController's own node.
    """
```

Behaviour:

1. Parse `target` into `target_node` and `target_simproc` by splitting on the first `/`.
2. If `target_node == "self"`, replace it with `self._node_name`.
3. If `target_node == self._node_name`:
   - Call `_deliver_local_promise(target_simproc, seqnr, epoch, num_events)`.
4. Else:
   - Construct a `PromiseEnvelope` with:
     - `target_node`
     - `target_simproc`
     - `seqnr`
     - `epoch`
     - `num_events`
   - Call `self._router.send_promise(envelope)`.

#### Local delivery methods (partial implementation)

These **must exist but be left empty** for now:

```python
def _deliver_local_event(
    self,
    target_simproc: str,
    epoch: float,
    data: bytes,
    headers: dict[str, str],
) -> None:
    """
    Placeholder: local delivery to a simproc-specific EventQueue will be implemented later.
    """
    # Intentionally left blank in this iteration.
    return None


def _deliver_local_promise(
    self,
    target_simproc: str,
    seqnr: int,
    epoch: float,
    num_events: int,
) -> None:
    """
    Placeholder: local delivery of promises to a simproc will be implemented later.
    """
    # Intentionally left blank in this iteration.
    return None
```

#### Receive methods

To support `InProcessTransport` and future transports:

```python
from .envelopes import EventEnvelope, PromiseEnvelope

def receive_event(self, envelope: EventEnvelope) -> None:
    """
    Entry point for events delivered from transports/router.
    For now, just delegate to _deliver_local_event; detailed queuing will be added later.
    """
    self._deliver_local_event(
        target_simproc=envelope.target_simproc,
        epoch=envelope.epoch,
        data=envelope.data,
        headers=envelope.headers,
    )


def receive_promise(self, envelope: PromiseEnvelope) -> None:
    """
    Entry point for promises delivered from transports/router.
    For now, just delegate to _deliver_local_promise.
    """
    self._deliver_local_promise(
        target_simproc=envelope.target_simproc,
        seqnr=envelope.seqnr,
        epoch=envelope.epoch,
        num_events=envelope.num_events,
    )
```

Note: even though `_deliver_local_*` are empty, these methods must be correctly wired.

### 5.3 Router

**File:** `src/disco/router.py`

This iteration implements a **very simple Router** that just delegates to a single transport. Future work will add address book, location resolution, etc.

```python
from __future__ import annotations

from .envelopes import EventEnvelope, PromiseEnvelope
from .transports.base import Transport


class ServerRouter:
    """
    Minimal router that delegates to a configured Transport.

    Future versions will:
    - resolve node locations,
    - choose between multiple transports (in-process, IPC, gRPC).
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def send_event(self, envelope: EventEnvelope) -> None:
        self._transport.send_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        self._transport.send_promise(envelope)
```

No additional logic is required in this iteration.

### 5.4 Transport Base & InProcessTransport

#### Base Transport

**File:** `src/disco/transports/base.py`

Define a `Transport` protocol:

```python
from __future__ import annotations

from typing import Protocol

from ..envelopes import EventEnvelope, PromiseEnvelope


class Transport(Protocol):
    def send_event(self, envelope: EventEnvelope) -> None:
        ...

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        ...
```

#### InProcessTransport

**File:** `src/disco/transports/inprocess.py`

A simple transport that uses a registry of `NodeController` instances and performs in-process delivery:

```python
from __future__ import annotations

from typing import Mapping

from ..envelopes import EventEnvelope, PromiseEnvelope
from ..node_controller import NodeController
from .base import Transport


class InProcessTransport(Transport):
    """
    Transport for in-process communication between NodeControllers
    on the same Python interpreter.

    It expects a mapping from node_name to NodeController.
    """

    def __init__(self, nodes: Mapping[str, NodeController]) -> None:
        self._nodes = nodes

    def send_event(self, envelope: EventEnvelope) -> None:
        node = self._nodes.get(envelope.target_node)
        if node is None:
            # For now, fail loudly; later we might log or handle differently.
            raise KeyError(f"No NodeController registered for node {envelope.target_node}")
        node.receive_event(envelope)

    def send_promise(self, envelope: PromiseEnvelope) -> None:
        node = self._nodes.get(envelope.target_node)
        if node is None:
            raise KeyError(f"No NodeController registered for node {envelope.target_node}")
        node.receive_promise(envelope)
```

## 6. Testing Strategy

### 6.1 `tests/test_node_controller.py`

Test cases:

1. **Local target with explicit node name**

   - Create a `NodeController` with `node_name="N1"`.
   - Use a stub router that records `send_event` / `send_promise` calls.
   - Subclass `NodeController` in the test to override `_deliver_local_event` and `_deliver_local_promise` so they record calls.
   - Call `send_event(target="N1/demand", ...)`.
   - Assert:
     - The data was serialized via the provided serializer.
     - `_deliver_local_event` was invoked with the correct arguments.
     - Router was **not** called.

2. **Local target with `"self"` alias**

   - Same as above, but `target="self/demand"`.
   - Must behave identically to `N1/demand`.

3. **Remote target (event)**

   - `node_name="N1"`, `target="N2/demand"`.
   - Ensure:
     - Data is serialized.
     - `_deliver_local_event` is **not** called.
     - Router’s `send_event` receives an `EventEnvelope` with:
       - `target_node == "N2"`,
       - `target_simproc == "demand"`,
       - `epoch` passed through,
       - `headers` passed through (or `{}` if None),
       - `data` equals serialized payload.

4. **Remote target (promise)**

   - Similar to (3) but for `send_promise`.
   - Verify:
     - `_deliver_local_promise` is not called for remote target.
     - Router’s `send_promise` receives a `PromiseEnvelope` with the expected fields.

5. **Headers defaulting**

   - Call `send_event` with `headers=None`.
   - Validate that:
     - Local path receives `headers={}`.
     - Remote path envelope has `headers={}`.

### 6.2 `tests/test_inprocess_transport.py`

1. Register a fake `NodeController` for node `"N1"` that records received envelopes.
2. Create an `InProcessTransport` with this mapping.
3. Call `send_event` and confirm:
   - `receive_event` is called with the correct envelope.
4. Call `send_promise` and confirm:
   - `receive_promise` is called with the correct envelope.
5. Test that sending to an unknown node raises `KeyError` for both events and promises.

## 7. Future Extensions (Informational, not required now)

Future iterations are expected to:

- Implement IPC transport using `multiprocessing.Queue` + `SharedMemory` for large payloads.
- Implement gRPC transport with:
  - protobuf messages for `Event` and `Promise`.
  - bi-directional streaming RPCs for events and promises.
  - gRPC compression configured via channel options (no custom compression in our code).
- Extend `ServerRouter` to:
  - maintain an address book mapping `(repid, node) -> location/transport`,
  - choose appropriate transport (in-process, IPC, gRPC),
  - prioritise promises over events at the routing level.
- Implement real EventQueue / PromiseQueue logic inside `NodeController` for each simproc.

These are **not** part of this iteration, but the current design should make them straightforward to add later.
