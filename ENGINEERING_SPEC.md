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

- `src/disco/…`

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

Note: `NodeController` holds the full state for the node; it does **not** need sender info for routing.

## 3. Scope of This Iteration

### In Scope

1. Definitions of envelopes.
2. NodeController skeleton.
3. Router interface and basic implementation.
4. Transport interfaces.
5. Support for `"self"` alias.
6. Basic tests.

### Out of Scope

- EventQueue implementations.
- IPC transport.
- gRPC service definitions.
- Address book & replication.
- Compression logic.

## 4. File Layout

```
src/disco/envelopes.py
src/disco/node_controller.py
src/disco/router.py
src/disco/transports/base.py
src/disco/transports/inprocess.py
```

## 5. Detailed Design

(Truncated for brevity in this downloadable file. The full detailed spec is in the ChatGPT message.)
