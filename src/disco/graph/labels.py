from __future__ import annotations

from typing import Iterable, List

import numpy as np
import graphblas as gb
from graphblas import Vector
from sqlalchemy.orm import Session
from sqlalchemy import select, insert

from .schema import labels, vertex_labels
from .core import Graph


def ensure_label(
    session: Session,
    scenario_id: int,
    label_type: str,
    label_value: str,
) -> int:
    """
    Get or create a label (scenario_id, type, value) and return its id.
    """
    result = session.execute(
        select(labels.c.id).where(
            labels.c.scenario_id == scenario_id,
            labels.c.type == label_type,
            labels.c.value == label_value,
        )
    ).scalar_one_or_none()

    if result is not None:
        return int(result)

    result = session.execute(
        insert(labels)
        .values(
            scenario_id=scenario_id,
            type=label_type,
            value=label_value,
        )
        .returning(labels.c.id)
    )
    return int(result.scalar_one())


def assign_label_to_vertices(
    session: Session,
    scenario_id: int,
    vertex_indices: Iterable[int],
    label_id: int,
) -> None:
    """
    Assign an existing label to a set of vertex indices for a scenario.
    """
    rows = [
        {
            "scenario_id": scenario_id,
            "vertex_index": int(idx),
            "label_id": int(label_id),
        }
        for idx in vertex_indices
    ]
    if rows:
        session.execute(insert(vertex_labels), rows)


def get_vertex_indices_for_label(
    session: Session,
    scenario_id: int,
    label_type: str,
    label_value: str,
) -> List[int]:
    """
    Return all vertex_index values that have the given (type, value) label
    in a scenario.
    """
    label_id = session.execute(
        select(labels.c.id).where(
            labels.c.scenario_id == scenario_id,
            labels.c.type == label_type,
            labels.c.value == label_value,
        )
    ).scalar_one_or_none()

    if label_id is None:
        return []

    result = session.execute(
        select(vertex_labels.c.vertex_index).where(
            (vertex_labels.c.scenario_id == scenario_id) &
            (vertex_labels.c.label_id == label_id)
        )
    )
    return [int(row[0]) for row in result]


def build_mask_for_label(
    session: Session,
    graph: Graph,
    label_type: str,
    label_value: str,
) -> Vector:
    """
    Build a GraphBLAS boolean mask Vector for all vertices in `graph`
    that have the given (type, value) label in the graph's scenario.
    """
    vertex_ids = get_vertex_indices_for_label(
        session,
        scenario_id=graph.scenario_id,
        label_type=label_type,
        label_value=label_value,
    )

    if not vertex_ids:
        return Vector.from_coo([], [], size=graph.num_vertices, dtype=gb.dtypes.BOOL)

    idx_arr = np.asarray(vertex_ids, dtype=np.int64)
    val_arr = np.ones_like(idx_arr, dtype=bool)

    return Vector.from_coo(idx_arr, val_arr, size=graph.num_vertices, dtype=gb.dtypes.BOOL)
