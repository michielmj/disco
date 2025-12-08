from __future__ import annotations

from typing import Sequence, Mapping, Any, List, Optional, Literal

import numpy as np
import pandas as pd
import graphblas as gb
from sqlalchemy import select, and_, literal
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import Table
from sqlalchemy.sql.elements import ColumnElement

from .core import Graph
from .graph_mask import GraphMask
from .schema import vertex_masks, edges as edges_table

Backend = Literal["pandas"]  # reserved for future extension


def _get_effective_mask(graph: Graph, mask: Optional[GraphMask]) -> Optional[GraphMask]:
    """
    Decide which mask to use: explicit argument wins, then Graph's internal mask if available.
    """
    if mask is not None:
        return mask

    # Prefer an internal accessor if Graph exposes it (e.g. _graph_mask()).
    gm = getattr(graph, "_graph_mask", None)
    if callable(gm):
        return gm()

    # Fallback: if Graph has a .mask attribute that is a GraphMask, use it.
    maybe_mask = getattr(graph, "mask", None)
    if isinstance(maybe_mask, GraphMask):
        return maybe_mask

    return None


def _rows_to_df(rows: List[Mapping[str, Any]]) -> pd.DataFrame:
    """
    Convert SQLAlchemy RowMapping list to a pandas DataFrame.
    """
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Vertex data
# ---------------------------------------------------------------------------

def get_vertex_data(
    session: Session,
    graph: Graph,
    vertex_table: Table,
    columns: Sequence[ColumnElement[Any]],
    *,
    mask: Optional[GraphMask] = None,
) -> pd.DataFrame:
    """
    Return a DataFrame with one row per vertex in the scenario (optionally filtered by mask),
    containing the requested columns from the user-provided vertex_table.

    Requirements on vertex_table:
    - must have columns: scenario_id, vertex_index
    """
    if not hasattr(vertex_table.c, "scenario_id") or not hasattr(vertex_table.c, "vertex_index"):
        raise ValueError(
            "vertex_table must have 'scenario_id' and 'vertex_index' columns "
            "with the same semantics as graph.vertices."
        )

    eff_mask = _get_effective_mask(graph, mask)

    base = select(*columns).where(vertex_table.c.scenario_id == literal(graph.scenario_id))

    if eff_mask is None:
        stmt = base
    else:
        eff_mask.ensure_persisted(session)
        vm = vertex_masks
        stmt = (
            base.join(
                vm,
                and_(vm.c.scenario_id == vertex_table.c.scenario_id,
                     vm.c.vertex_index == vertex_table.c.vertex_index),
            )
            .where(vm.c.mask_id == literal(eff_mask.mask_id))
        )

    result = session.execute(stmt)
    rows = list(result.mappings())
    return _rows_to_df(rows)


# ---------------------------------------------------------------------------
# 2. Edge data (outbound / inbound)
# ---------------------------------------------------------------------------

def _validate_edge_table(edge_table: Table) -> None:
    required = ("scenario_id", "layer_id", "source_idx", "target_idx")
    missing = [name for name in required if not hasattr(edge_table.c, name)]
    if missing:
        raise ValueError(
            f"edge_table must have columns {required}, missing: {missing}"
        )


def get_outbound_edge_data(
    session: Session,
    graph: Graph,
    edge_table: Table,
    columns: Sequence[ColumnElement[Any]],
    *,
    layer_id: int,
    mask: Optional[GraphMask] = None,
) -> pd.DataFrame:
    """
    Return a DataFrame with one row per outbound edge from vertices in the mask
    (or all vertices if no mask), for a specific layer.

    Requirements on edge_table:
    - columns: scenario_id, layer_id, source_idx, target_idx
    """
    _validate_edge_table(edge_table)
    eff_mask = _get_effective_mask(graph, mask)

    base = select(*columns).where(
        (edge_table.c.scenario_id == graph.scenario_id)
        & (edge_table.c.layer_id == int(layer_id))
    )

    if eff_mask is None:
        stmt = base
    else:
        eff_mask.ensure_persisted(session)
        vm = vertex_masks
        stmt = (
            base.join(
                vm,
                and_(vm.c.scenario_id == edge_table.c.scenario_id,
                     vm.c.vertex_index == edge_table.c.source_idx),
            )
            .where(vm.c.mask_id == literal(eff_mask.mask_id))
        )

    result = session.execute(stmt)
    rows = list(result.mappings())
    return _rows_to_df(rows)


def get_inbound_edge_data(
    session: Session,
    graph: Graph,
    edge_table: Table,
    columns: Sequence[ColumnElement[Any]],
    *,
    layer_id: int,
    mask: Optional[GraphMask] = None,
) -> pd.DataFrame:
    """
    Return a DataFrame with one row per inbound edge to vertices in the mask
    (or all vertices if no mask), for a specific layer.

    Requirements on edge_table:
    - columns: scenario_id, layer_id, source_idx, target_idx
    """
    _validate_edge_table(edge_table)
    eff_mask = _get_effective_mask(graph, mask)

    base = select(*columns).where(
        (edge_table.c.scenario_id == graph.scenario_id)
        & (edge_table.c.layer_id == int(layer_id))
    )

    if eff_mask is None:
        stmt = base
    else:
        eff_mask.ensure_persisted(session)
        vm = vertex_masks
        stmt = (
            base.join(
                vm,
                and_(vm.c.scenario_id == edge_table.c.scenario_id,
                     vm.c.vertex_index == edge_table.c.target_idx),
            )
            .where(vm.c.mask_id == literal(eff_mask.mask_id))
        )

    result = session.execute(stmt)
    rows = list(result.mappings())
    return _rows_to_df(rows)


# ---------------------------------------------------------------------------
# 3. Map extraction (GraphBLAS matrices)
# ---------------------------------------------------------------------------

ValueSource = Literal["weight"]  # keep simple for now


def get_outbound_map(
    session: Session,
    graph: Graph,
    *,
    layer_id: int,
    mask: Optional[GraphMask] = None,
    value_source: ValueSource = "weight",
) -> gb.Matrix:
    """
    Return a GraphBLAS Matrix for outbound edges in a given layer.

    - Rows: source_idx
    - Columns: target_idx
    - Values: edge weight (from graph.edges table) for now.

    Mask semantics (if provided or set on graph):
    - Only edges whose *source* vertex is in the mask are included.
    """
    if value_source != "weight":
        raise NotImplementedError("Only value_source='weight' is supported for now.")

    eff_mask = _get_effective_mask(graph, mask)

    e = edges_table
    base = select(
        e.c.source_idx.label("src"),
        e.c.target_idx.label("tgt"),
        e.c.weight.label("val"),
    ).where(
        (e.c.scenario_id == graph.scenario_id)
        & (e.c.layer_id == int(layer_id))
    )

    if eff_mask is None:
        stmt = base
    else:
        eff_mask.ensure_persisted(session)
        vm = vertex_masks
        stmt = (
            base.join(
                vm,
                and_(vm.c.scenario_id == e.c.scenario_id,
                     vm.c.vertex_index == e.c.source_idx),
            )
            .where(vm.c.mask_id == literal(eff_mask.mask_id))
        )

    result = session.execute(stmt)
    rows = list(result.mappings())

    if not rows:
        # return empty matrix with correct size
        return gb.Matrix.sparse(
            gb.dtypes.FP64, graph.num_vertices, graph.num_vertices
        )

    src = np.fromiter((r["src"] for r in rows), dtype=np.int64)
    tgt = np.fromiter((r["tgt"] for r in rows), dtype=np.int64)
    val = np.fromiter((r["val"] for r in rows), dtype=np.float64)

    return gb.Matrix.from_coo(
        src,
        tgt,
        val,
        nrows=graph.num_vertices,
        ncols=graph.num_vertices,
    )


def get_inbound_map(
    session: Session,
    graph: Graph,
    *,
    layer_id: int,
    mask: Optional[GraphMask] = None,
    value_source: ValueSource = "weight",
) -> gb.Matrix:
    """
    Return a GraphBLAS Matrix for inbound edges in a given layer.

    - Rows: source_idx
    - Columns: target_idx
    - Values: edge weight.

    Mask semantics:
    - Only edges whose *target* vertex is in the mask are included.
    """
    if value_source != "weight":
        raise NotImplementedError("Only value_source='weight' is supported for now.")

    eff_mask = _get_effective_mask(graph, mask)

    e = edges_table
    base = select(
        e.c.source_idx.label("src"),
        e.c.target_idx.label("tgt"),
        e.c.weight.label("val"),
    ).where(
        (e.c.scenario_id == graph.scenario_id)
        & (e.c.layer_id == int(layer_id))
    )

    vm = vertex_masks

    if eff_mask is None:
        stmt = base
    else:
        eff_mask.ensure_persisted(session)
        stmt = (
            base.join(
                vm,
                and_(vm.c.scenario_id == e.c.scenario_id,
                     vm.c.vertex_index == e.c.target_idx),
            )
            .where(vm.c.mask_id == literal(eff_mask.mask_id))
        )

    result = session.execute(stmt)
    rows = list(result.mappings())

    if not rows:
        return gb.Matrix.sparse(
            gb.dtypes.FP64, graph.num_vertices, graph.num_vertices
        )

    src = np.fromiter((r["src"] for r in rows), dtype=np.int64)
    tgt = np.fromiter((r["tgt"] for r in rows), dtype=np.int64)
    val = np.fromiter((r["val"] for r in rows), dtype=np.float64)

    return gb.Matrix.from_coo(
        src,
        tgt,
        val,
        nrows=graph.num_vertices,
        ncols=graph.num_vertices,
    )
