from typing import Iterator

import numpy as np
import graphblas as gb
from graphblas import Vector
import pytest
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session, sessionmaker

from disco.graph import (
    Graph,
    create_graph_schema,
    create_scenario,
    store_graph,
    load_graph_for_scenario,
)
from disco.graph.schema import vertices as vertices_table
from disco.graph.schema import edges as edges_table
from disco.graph.schema import vertex_masks as vertex_masks_table
from disco.graph import create_graph_schema


@pytest.fixture
def engine_and_session_factory() -> Iterator[tuple[object, sessionmaker]]:
    """
    Fixture that provides an in-memory SQLite engine and a sessionmaker.
    For SQLite we strip the 'graph' schema via schema_translate_map.
    """
    # Base in-memory engine
    base_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    # For SQLite, drop the 'graph' schema (graph.<table> -> <table>)
    engine = base_engine.execution_options(schema_translate_map={"graph": None})

    # Create schema (tables) in the in-memory database
    create_graph_schema(engine)

    SessionLocal = sessionmaker(bind=engine, future=True)

    yield engine, SessionLocal

    engine.dispose()


def _create_scenario_with_vertices(session: Session, num_vertices: int) -> int:
    """
    Helper to create a scenario and populate graph.vertices with
    vertex indices 0..num_vertices-1.
    """
    scenario_id = create_scenario(
        session,
        name="roundtrip_test",
        base_scenario_id=None,
        description="Roundtrip store/load test",
    )

    rows = [
        {
            "scenario_id": scenario_id,
            "vertex_index": i,
            "entity_id": f"v{i}",
            "name": f"vertex-{i}",
        }
        for i in range(num_vertices)
    ]
    session.execute(insert(vertices_table), rows)
    session.commit()
    return scenario_id


def test_store_and_load_graph_with_labels_and_mask(engine_and_session_factory: tuple[object, sessionmaker]) -> None:
    _, SessionLocal = engine_and_session_factory
    session: Session = SessionLocal()

    num_vertices = 4
    scenario_id = _create_scenario_with_vertices(session, num_vertices)

    # ------------------------------------------------------------------
    # Build in-memory Graph: edges
    # ------------------------------------------------------------------
    src = np.array([0, 1], dtype=np.int64)
    tgt = np.array([1, 2], dtype=np.int64)
    wgt = np.array([1.0, 2.0], dtype=np.float64)
    edge_layers: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]] = {
        0: (src, tgt, wgt)
    }

    graph = Graph.from_edges(edge_layers, num_vertices=num_vertices, scenario_id=scenario_id)

    # ------------------------------------------------------------------
    # Attach labels
    #   - 3 labels: indices 0,1,2
    #   - label_meta:
    #       0 -> (type1, A)
    #       1 -> (type2, B)
    #       2 -> (type1, C)
    #   - assignments:
    #       (0, 0), (1, 1), (2, 2), (3, 0)
    # ------------------------------------------------------------------
    v_idx = np.array([0, 1, 2, 3], dtype=np.int64)
    l_idx = np.array([0, 1, 2, 0], dtype=np.int64)
    vals = np.ones(len(v_idx), dtype=bool)

    label_matrix = gb.Matrix.from_coo(
        v_idx,
        l_idx,
        vals,
        nrows=num_vertices,
        ncols=3,
    )

    label_meta = {
        0: ("type1", "A"),
        1: ("type2", "B"),
        2: ("type1", "C"),
    }

    type1_vec = Vector.from_coo([0, 2], [True, True], size=3)
    type2_vec = Vector.from_coo([1], [True], size=3)
    label_type_vectors = {"type1": type1_vec, "type2": type2_vec}

    graph.set_labels(label_matrix, label_meta, label_type_vectors)

    # ------------------------------------------------------------------
    # Set a mask (vertices 1 and 2)
    # NOTE: mask is NOT persisted by store_graph/load_graph,
    #       but will be used in DB queries via vertex_masks.
    # ------------------------------------------------------------------
    mask_vec = Vector.from_coo([1, 2], [True, True], size=num_vertices)
    graph.set_mask(mask_vec)

    # ------------------------------------------------------------------
    # Store graph (edges + labels) into DB
    # ------------------------------------------------------------------
    store_graph(session, graph, store_edges=True, store_labels=True)
    session.commit()

    # Quick sanity: edges really exist in DB
    db_edges = session.execute(
        edges_table.select().where(edges_table.c.scenario_id == scenario_id)
    ).all()
    assert len(db_edges) == 2

    # ------------------------------------------------------------------
    # Load graph back from DB
    # ------------------------------------------------------------------
    loaded = load_graph_for_scenario(session, scenario_id)

    # Structure checks
    assert loaded.num_vertices == num_vertices
    loaded_mat = loaded.get_matrix(0)
    assert loaded_mat.nrows == num_vertices
    assert loaded_mat.ncols == num_vertices
    # Should match the original weights
    assert loaded_mat[0, 1].value == 1.0
    assert loaded_mat[1, 2].value == 2.0

    # Label checks
    assert loaded.num_labels == 3
    assert loaded.label_matrix is not None
    assert loaded.label_matrix.nrows == num_vertices
    assert loaded.label_matrix.ncols == 3

    # Meta should match
    assert loaded.label_meta == label_meta

    # Label-based mask by label id: label 0 -> vertices 0 and 3
    mask_label0 = loaded.get_vertex_mask_for_label_id(0)
    idx0, vals0 = mask_label0.to_coo()
    assert set(idx0.tolist()) == {0, 3}
    assert set(vals0.tolist()) == {True}

    # Label-based mask by type: "type1" -> vertices {0,2,3}
    mask_type1 = loaded.get_vertex_mask_for_label_type("type1")
    idx1, vals1 = mask_type1.to_coo()
    assert set(idx1.tolist()) == {0, 2, 3}
    assert set(vals1.tolist()) == {True}

    # ------------------------------------------------------------------
    # Mask + outbound map via DB (vertex_masks join)
    #   - Set mask on loaded graph to vertices {1,2}
    #   - Only edges whose source is in {1,2} should appear
    #   - In our edge set, only edge (1 -> 2) remains
    # ------------------------------------------------------------------
    loaded_mask_vec = Vector.from_coo([1, 2], [True, True], size=num_vertices)
    loaded.set_mask(loaded_mask_vec)

    mat_out = loaded.get_outbound_map(session, layer_id=0)

    # Expect only one edge: 1 -> 2 with weight 2.0
    rows, cols, vals_out = mat_out.to_coo()
    assert len(rows) == 1
    assert int(rows[0]) == 1
    assert int(cols[0]) == 2
    assert float(vals_out[0]) == 2.0

    # And ensure underlying vertex_masks table was populated for this scenario
    vm_rows = session.execute(
        vertex_masks_table.select().where(
            vertex_masks_table.c.scenario_id == scenario_id
        )
    ).all()
    # We only assert that some rows exist; exact count depends on mask usage
    assert len(vm_rows) > 0

    session.close()
