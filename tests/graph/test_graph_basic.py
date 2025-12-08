import numpy as np
import graphblas as gb
from graphblas import Vector
from disco.graph import Graph


def test_basic_graph_structure() -> None:
    src = np.array([0, 1, 2], dtype=np.int64)
    tgt = np.array([1, 2, 3], dtype=np.int64)
    wgt = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    layers: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]] = {0: (src, tgt, wgt)}

    graph = Graph.from_edges(layers, num_vertices=5)

    assert graph.num_vertices == 5
    mat = graph.get_matrix(0)

    # Matrix indexing returns a Scalar; use .value for the Python float
    assert mat[1, 2].value == 2.0

    out_edges = graph.get_out_edges(0, 2)
    # out_edges is a Vector with index 3 containing weight 3.0
    assert out_edges[3].value == 3.0


def test_graph_masking() -> None:
    src = np.array([0, 1], dtype=np.int64)
    tgt = np.array([1, 2], dtype=np.int64)
    wgt = np.array([1.0, 2.0], dtype=np.float64)
    layers: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]] = {0: (src, tgt, wgt)}

    graph = Graph.from_edges(layers, num_vertices=4)

    # Mask vertices 1 and 2
    mask = Vector.from_coo([1, 2], [True, True], size=4)
    graph.set_mask(mask)

    # Mask should be stored as a GraphBLAS Vector[BOOL] of size num_vertices
    mask_vec = graph.mask_vector
    assert mask_vec is not None
    assert mask_vec.size == 4

    idx, vals = mask_vec.to_coo()
    assert set(idx.tolist()) == {1, 2}
    # All values should be True
    assert set(vals.tolist()) == {True}

    # The adjacency matrix itself is not resized by the mask
    mat = graph.get_matrix(0)
    assert mat.nrows == graph.num_vertices
    assert mat.ncols == graph.num_vertices
    assert mat[1, 2].value == 2.0


def test_graph_labels() -> None:
    """
    Verify that label_matrix, label_meta and label_type_vectors work together
    and that we can derive vertex masks from label id and label type.
    """
    num_vertices = 4

    # We define 3 labels with global ids 0, 1, 2:
    #  0: type1 / "A"
    #  1: type2 / "B"
    #  2: type1 / "C"
    #
    # Vertex-label assignments (vertex_index, label_id):
    #  (0, 0), (1, 1), (2, 2), (3, 0)
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

    # Type vectors over labels (size == num_labels)
    type1_vec = Vector.from_coo([0, 2], [True, True], size=3)
    type2_vec = Vector.from_coo([1], [True], size=3)
    label_type_vectors = {"type1": type1_vec, "type2": type2_vec}

    # Empty structural graph; we're testing labels here
    graph = Graph(layers={}, num_vertices=num_vertices, scenario_id=0)
    graph.set_labels(label_matrix, label_meta, label_type_vectors)

    assert graph.num_labels == 3
    assert graph.label_matrix is not None
    assert graph.label_matrix.nrows == num_vertices
    assert graph.label_matrix.ncols == 3

    # Vertices that have label id 0: vertices 0 and 3
    mask_label0 = graph.get_vertex_mask_for_label_id(0)
    idx, vals = mask_label0.to_coo()
    assert set(idx.tolist()) == {0, 3}
    assert set(vals.tolist()) == {True}

    # Vertices that have any label of type "type1" (labels 0 or 2):
    # assignments: (0, 0), (2, 2), (3, 0) -> vertices {0, 2, 3}
    mask_type1 = graph.get_vertex_mask_for_label_type("type1")
    idx, vals = mask_type1.to_coo()
    assert set(idx.tolist()) == {0, 2, 3}
    assert set(vals.tolist()) == {True}

    # Vertices that have any label of type "type2" (label 1) -> vertex {1}
    mask_type2 = graph.get_vertex_mask_for_label_type("type2")
    idx, vals = mask_type2.to_coo()
    assert set(idx.tolist()) == {1}
    assert set(vals.tolist()) == {True}
