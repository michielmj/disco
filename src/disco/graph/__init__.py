"""
disco.graph
===========

Layered graph + scenario subsystem.

Public API:

- Graph                  : core in-memory graph structure (layers, labels, mask).
- create_graph_schema    : create the graph schema/tables in a database.
- create_scenario        : insert a new scenario row.
- store_graph            : persist a Graph's structure (edges + labels) to the DB.
- store_graph_edges      : backwards-compatible alias; also persists labels.
- load_graph_for_scenario: load a Graph (edges + labels) from the DB for a scenario.

All other modules in this package are considered internal implementation details.
"""

from __future__ import annotations

from .core import Graph
from .schema import create_graph_schema
from .db import (
    create_scenario,
    store_graph,
    store_graph_edges,
    load_graph_for_scenario,
)

__all__ = [
    "Graph",
    "create_graph_schema",
    "create_scenario",
    "store_graph",
    "store_graph_edges",
    "load_graph_for_scenario",
]
