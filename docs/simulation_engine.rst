A Distributed Simulation Engine
=================

Purpose
-------
Disco (Distributed Simulation Core) is a scalable Python based environment for running distributed discrete event
simulations. Simulation models are unrestricted in size as they can be stored and evaluated across multiple
computing resources.

Structure of a Simulation Model
--------
A simulation model consists of a network of connected nodes. Nodes maintain state data and implement state
transition logic. The interactions between nodes are limited to the directed edges between them. Edges are organized
in layers where each layer must be acyclic for Disco to function. We call these layers simulation processes (simprocs).


    Disco is built with Supply Chain Simulation in mind. A node in a supply chain network
    may be a Stocking Point. A Stocking Point may for example be a shop, a distribution center, or a factory
    warehouse. A Stocking Point has customer demand that it needs to satisfy for which it needs to order stock.
    Stock is ordered from another stocking point through an *order* process. To fulfill demand from another
    Stocking Point, the source may use a *supply* process. The simulation model becomes a layered directed
    acyclic graph with a set of *order* edges on the first layer and a set of *supply* edges on the second
    layer. The shop stocking point (node) orders (process) a product at the regional distribution center
    stocking point. If the stock is available, it is supplied (process) to the shop stocking point directly.
    Each combination of product and node are a planunit (vertex). A node can be a fixed entity like a stocking
    point, but it can also represent something moving like a truck.

Logic is implemented in the simulation nodes. To facilitate concurrency, each node has its own clock that
is only loosely coupled with the clock of the other nodes. This allows nodes to make efficient use of
different computing resources. To support this loose coupling, processes and nodes need to have the following
special structure:

1. A process connects nodes via directed edges and forms a directed graph. For a specific process, this graph
   must be **acyclic**. That is, if node A orders at node B and node B orders at node C, then node C may not order
   at node A because that would create a cycle in the order subgraph.
2. Nodes communicate over processes using events. Each event has a an epoch which is the (simulation) time in
   at which the destination node receives the event.
3. Processes follow a hierarchy or sequence. For an individual node, same epoch events for higher level processes
   are always processed before events for lower level processes are processed.

The simulation sequence of events processing thus is governed by a stacking of directed acyclic graphs. By stacking
the graphs, bidirectional interaction between nodes is possible. For example, a shop can order at a distribution
center that supplies the shop at a later time. Here we have two nodes (shop, distribution center) and two processes (order, supply).
The order process runs from shop to distribution center and is thus acyclic. The supply process runs from
distribution center to the shop and is acyclic as well. The order process is hierarchically positioned above the
supply process. So orders are first processed at the before same epoch supplies are processed.

Time bookkeeping
----------------
To ensure that events across different nodes remain correctly sequenced and at the same time let nodes proceed
in time if possible, the simulation engine uses a time-bookkeeping concept called a *promise*. A *promise* tells
adjacent nodes when they may expect the next event. Promises are sequenced between each sending and receiving
node. Once a node has received all incoming promises for a process, it can determine the earliest epoch for
the next event in a process. It can then update its outbound promises and continue processing until this epoch.

Promises from a node to its successors are automatically sent for each process after an epoch has been
processed. The promise made depends on the outbound events and inbound promises. A node may also schedule a
hard wakeup for a process (meaning that no events will be processed before that epoch), in which case a later
promise can be made by the simulation engine. Hard wake-ups can thus improve concurrency in the simulation network.

Concurrency in the network can be further improved by making advance promises. An advance promise from a node to a
successor tells the successor that the node will not send events to the successor before a specific epoch,
allowing the successor to proceed further in time. The advance promise does not promise any number of events,
just that there will not be any earlier events.

Consolidating processing
------------------------
Nodes, together with the handling of events and promises, present a degree of overhead in the simulation. This
overhead can be reduced by combining many vertices in a single node and building the vector based simulation
logic. Because Disco is build with Supply Chain in mind, we sometimes call a vertex a
*planunit*. A planunit may be a product at a certain location. A node typically represents a large number of
vertices. It is an important design question how to combine nodes where the trade-off is between concurrency,
resource usage, bandwidth usage (for communication) and simplicity of the simulation logic in a node.

There are at most three levels of communication in the simulation. A simulation worker runs in a single
operating system process and typically serves multiple nodes that communicate directly (in process communication).
There may be multiple workers running in their own OS process on an individual server
that communicate via Python native multiprocess queues and shared memory. This has more overhead than the
direct communication but is still quite efficient and does not use bandwidth. Finally, there is communication
between workers on different servers. Currently, Disco uses gRPC for this but this may be changed in
future to improve transportation efficiency.

The partitioner is responsible for assigning vertices to nodes and partitions in a way that minimizes
bandwidth and communication overhead while at the same time balancing the required computation resources.
Labels guide the partitioner on what vertices must be scheduled on different nodes. For example,
we may take the vertex's location as a label. For a supply chain simulation, it is however more
effective to determine the echelon of each planunit (vertex) and use this as a criteria.
