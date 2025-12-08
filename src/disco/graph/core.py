from __future__ import annotations

from typing import Mapping, Dict, Optional, Sequence, Tuple, TYPE_CHECKING, Any

import numpy as np
import graphblas as gb
from graphblas import Matrix, Vector

from .graph_mask import GraphMask  # if you already have this; otherwise remove

if TYPE_CHECKING:
    # Only for the thin delegating methods; remove if you don't want DB helpers on Graph.
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.schema import Table
    from sqlalchemy.sql.elements import ColumnElement
    import pandas as pd


class Graph:
    """
    Layered directed graph backed by python-graphblas.

    Structure:
      - Vertices are 0..num_vertices-1 per scenario.
      - Layers: adjacency Matrix per layer_id (directed, weighted).
      - Optional vertex mask: Vector[BOOL] (wrapped as GraphMask for DB use).
      - Labels:
          * Global label ids 0..num_labels-1 (per scenario).
          * label_matrix: Matrix[BOOL] with shape (num_vertices, num_labels)
                rows  = vertices
                cols  = labels
                True  = vertex has that label
          * label_meta: mapping label_index -> (label_type, label_value)
          * label_type_vectors: mapping label_type -> Vector[BOOL] over labels
                (size num_labels; True where label belongs to that type)
    """

    __slots__ = (
        "_layers",
        "num_vertices",
        "scenario_id",
        "_mask",                # GraphMask | None
        "_label_matrix",        # Matrix[BOOL] | None
        "_label_meta",          # dict[int, tuple[str, str]]
        "_label_type_vectors",  # dict[str, Vector[BOOL]]
        "num_labels",
    )

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #
    def __init__(
        self,
        layers: Mapping[int, Matrix],
        num_vertices: int,
        scenario_id: int = 0,
        *,
        mask: Optional[Vector] = None,
        label_matrix: Optional[Matrix] = None,
        label_meta: Optional[Dict[int, Tuple[str, str]]] = None,
        label_type_vectors: Optional[Dict[str, Vector]] = None,
    ) -> None:
        self._layers: Dict[int, Matrix] = dict(layers)
        self.num_vertices = int(num_vertices)
        self.scenario_id = int(scenario_id)

        # mask (GraphMask, internal)
        self._mask: Optional[GraphMask] = None
        if mask is not None:
            self.set_mask(mask)

        # labels
        self._label_matrix: Optional[Matrix] = None
        self._label_meta: Dict[int, Tuple[str, str]] = {}
        self._label_type_vectors: Dict[str, Vector] = {}
        self.num_labels: int = 0

        if label_matrix is not None:
            self.set_labels(label_matrix, label_meta, label_type_vectors)

    @classmethod
    def from_edges(
        cls,
        edge_layers: Mapping[int, tuple[np.ndarray, np.ndarray, np.ndarray]],
        *,
        num_vertices: Optional[int] = None,
        num_nodes: Optional[int] = None,   # backwards compat with older tests
        scenario_id: int = 0,
    ) -> Graph:
        """
        Build a Graph from per-layer edge arrays.

        edge_layers: mapping layer_id -> (source_vertex_indices, target_vertex_indices, weights)
        num_vertices / num_nodes: total number of vertices (must be specified via
                                  at least one of these; both must match if both are given).
        """
        if num_vertices is None and num_nodes is None:
            raise ValueError("Either num_vertices or num_nodes must be provided")

        if num_vertices is None:
            num_vertices = int(num_nodes)
        elif num_nodes is not None and int(num_nodes) != int(num_vertices):
            raise ValueError(
                f"num_vertices ({num_vertices}) != num_nodes ({num_nodes}); please pass only one"
            )

        num_vertices = int(num_vertices)

        layers: Dict[int, Matrix] = {}
        for layer_id, (src, dst, w) in edge_layers.items():
            src_arr = np.asarray(src, dtype=np.int64)
            dst_arr = np.asarray(dst, dtype=np.int64)
            val_arr = np.asarray(w)

            mat = gb.Matrix.from_coo(
                src_arr,
                dst_arr,
                val_arr,
                nrows=num_vertices,
                ncols=num_vertices,
            )
            layers[int(layer_id)] = mat

        return cls(layers=layers, num_vertices=num_vertices, scenario_id=scenario_id)

    # Backwards compat for existing `num_nodes` usage in tests
    @property
    def num_nodes(self) -> int:
        return self.num_vertices

    # ------------------------------------------------------------------ #
    # Mask handling (public API: Vector; internal: GraphMask)
    # ------------------------------------------------------------------ #
    def set_mask(self, mask_vector: Optional[Vector]) -> None:
        """
        Set or clear the vertex mask.

        - mask_vector is a GraphBLAS Vector[BOOL] of size num_vertices.
        - Internally stored as a GraphMask to support DB persistence when needed.
        """
        if mask_vector is None:
            self._mask = None
            return

        if mask_vector.dtype is not gb.dtypes.BOOL:
            raise TypeError(f"Mask vector must have BOOL dtype, got {mask_vector.dtype!r}")
        if mask_vector.size != self.num_vertices:
            raise ValueError(
                f"Mask size ({mask_vector.size}) must match num_vertices ({self.num_vertices})"
            )

        self._mask = GraphMask(mask_vector, scenario_id=self.scenario_id)

    @property
    def mask_vector(self) -> Optional[Vector]:
        """Return the underlying GraphBLAS mask vector, if any."""
        if self._mask is None:
            return None
        return self._mask.vector

    # internal accessor for DB / extract helpers
    def _graph_mask(self) -> Optional[GraphMask]:
        return self._mask

    # ------------------------------------------------------------------ #
    # Label handling
    # ------------------------------------------------------------------ #
    def set_labels(
        self,
        label_matrix: Matrix,
        label_meta: Optional[Dict[int, Tuple[str, str]]] = None,
        label_type_vectors: Optional[Dict[str, Vector]] = None,
    ) -> None:
        """
        Attach label structures to the graph.

        label_matrix:
            - Matrix[BOOL] with shape (num_vertices, num_labels)
            - rows  = vertices
            - cols  = labels
            - True  = vertex has that label

        label_meta (optional):
            - mapping label_index -> (label_type, label_value)

        label_type_vectors (optional):
            - mapping label_type -> Vector[BOOL] of size num_labels
              where True means "label at this index has this type".

        All GraphBLAS collections are stored directly; no Python loops over
        vertices/labels are used in the core representation.
        """
        if label_matrix.dtype is not gb.dtypes.BOOL:
            raise TypeError(f"label_matrix must have BOOL dtype, got {label_matrix.dtype!r}")
        if label_matrix.nrows != self.num_vertices:
            raise ValueError(
                f"label_matrix.nrows ({label_matrix.nrows}) must match num_vertices ({self.num_vertices})"
            )

        self._label_matrix = label_matrix
        self.num_labels = int(label_matrix.ncols)

        # metadata: purely Python, for introspection / mapping ids to names
        self._label_meta = dict(label_meta or {})

        # type vectors: GraphBLAS vectors over label ids
        self._label_type_vectors = {}
        if label_type_vectors is not None:
            for t, vec in label_type_vectors.items():
                if vec.dtype is not gb.dtypes.BOOL:
                    raise TypeError(f"Label type vector for {t!r} must be BOOL, got {vec.dtype!r}")
                if vec.size != self.num_labels:
                    raise ValueError(
                        f"Label type vector size ({vec.size}) must match num_labels ({self.num_labels})"
                    )
                self._label_type_vectors[t] = vec

    @property
    def label_matrix(self) -> Optional[Matrix]:
        """
        Sparse boolean matrix of label assignments (vertices x labels),
        or None if no labels are attached.
        """
        return self._label_matrix

    @property
    def label_meta(self) -> Dict[int, Tuple[str, str]]:
        """
        Mapping label_index -> (label_type, label_value).
        """
        return dict(self._label_meta)

    @property
    def label_type_vectors(self) -> Dict[str, Vector]:
        """
        Mapping label_type -> Vector[BOOL] over label indices.

        Each vector has size num_labels and True where the label at that index
        belongs to this type. Label types are non-overlapping subsets of labels
        by construction.
        """
        return dict(self._label_type_vectors)

    def get_vertex_mask_for_label_id(self, label_id: int) -> Vector:
        """
        Return a Vector[BOOL] of size num_vertices indicating which vertices
        have the given label (by label id 0..num_labels-1).
        """
        if self._label_matrix is None:
            raise RuntimeError("Graph has no labels attached (label_matrix is None)")
        if not (0 <= label_id < self.num_labels):
            raise IndexError(f"label_id {label_id} out of range [0, {self.num_labels})")
        # Column slice -> Vector[BOOL] over vertices
        return self._label_matrix[:, int(label_id)]

    def get_vertex_mask_for_label_type(self, label_type: str) -> Vector:
        """
        Return a Vector[BOOL] of size num_vertices indicating which vertices
        have ANY label of the given type.

        Implemented as a boolean matrix-vector product:

            vertex_mask = label_matrix.mxv(type_vec, lor_land)
        """
        if self._label_matrix is None:
            raise RuntimeError("Graph has no labels attached (label_matrix is None)")
        if label_type not in self._label_type_vectors:
            raise KeyError(f"Unknown label_type {label_type!r}")

        type_vec = self._label_type_vectors[label_type]
        # bool semiring: OR over labels, AND for combination
        return self._label_matrix.mxv(type_vec, gb.semiring.lor_land)

    # ------------------------------------------------------------------ #
    # Structural accessors
    # ------------------------------------------------------------------ #
    def layers_dict(self) -> Dict[int, Matrix]:
        """Return a shallow copy of the internal layer mapping."""
        return dict(self._layers)

    def get_matrix(self, layer: int) -> Matrix:
        """Return the full adjacency matrix for a layer (no masking applied)."""
        return self._layers[layer]

    def get_out_edges(self, layer: int, vertex_index: int) -> Vector:
        """
        Return outgoing edges of vertex_index as a Vector:

        - indices: target vertices
        - values: edge weights
        """
        mat = self._layers[layer]
        return mat[vertex_index, :]

    def get_in_edges(self, layer: int, vertex_index: int) -> Vector:
        """
        Return incoming edges of vertex_index as a Vector:

        - indices: source vertices
        - values: edge weights
        """
        mat = self._layers[layer]
        return mat[:, vertex_index]

    # ------------------------------------------------------------------ #
    # (Optional) thin delegating methods for DB-backed data extraction
    # ------------------------------------------------------------------ #
    def get_vertex_data(
        self,
        session: Session,
        vertex_table: Table,
        columns: Sequence[ColumnElement[Any]],
        *,
        mask: Optional[Vector] = None,
    ) -> pd.DataFrame:
        """
        Delegate to disco.graph.extract.get_vertex_data.

        mask (if provided) overrides the Graph's own mask for this call.
        """
        from .extract import get_vertex_data as _get_vertex_data  # local import avoids cycles

        effective_mask: Optional[GraphMask] = self._mask
        if mask is not None:
            if mask.dtype is not gb.dtypes.BOOL or mask.size != self.num_vertices:
                raise ValueError("Override mask must be BOOL and size == num_vertices")
            effective_mask = GraphMask(mask, scenario_id=self.scenario_id)

        return _get_vertex_data(session, self, vertex_table, columns, mask=effective_mask)

    def get_outbound_edge_data(
        self,
        session: Session,
        edge_table: Table,
        columns: Sequence[ColumnElement[Any]],
        *,
        layer_id: int,
        mask: Optional[Vector] = None,
    ) -> pd.DataFrame:
        from .extract import get_outbound_edge_data as _goe

        effective_mask: Optional[GraphMask] = self._mask
        if mask is not None:
            if mask.dtype is not gb.dtypes.BOOL or mask.size != self.num_vertices:
                raise ValueError("Override mask must be BOOL and size == num_vertices")
            effective_mask = GraphMask(mask, scenario_id=self.scenario_id)

        return _goe(session, self, edge_table, columns, layer_id=layer_id, mask=effective_mask)

    def get_inbound_edge_data(
        self,
        session: Session,
        edge_table: Table,
        columns: Sequence[ColumnElement[Any]],
        *,
        layer_id: int,
        mask: Optional[Vector] = None,
    ) -> pd.DataFrame:
        from .extract import get_inbound_edge_data as _gie

        effective_mask: Optional[GraphMask] = self._mask
        if mask is not None:
            if mask.dtype is not gb.dtypes.BOOL or mask.size != self.num_vertices:
                raise ValueError("Override mask must be BOOL and size == num_vertices")
            effective_mask = GraphMask(mask, scenario_id=self.scenario_id)

        return _gie(session, self, edge_table, columns, layer_id=layer_id, mask=effective_mask)

    def get_outbound_map(
        self,
        session: Session,
        *,
        layer_id: int,
        mask: Optional[Vector] = None,
    ) -> Matrix:
        from .extract import get_outbound_map as _gom

        effective_mask: Optional[GraphMask] = self._mask
        if mask is not None:
            if mask.dtype is not gb.dtypes.BOOL or mask.size != self.num_vertices:
                raise ValueError("Override mask must be BOOL and size == num_vertices")
            effective_mask = GraphMask(mask, scenario_id=self.scenario_id)

        return _gom(session, self, layer_id=layer_id, mask=effective_mask)

    def get_inbound_map(
        self,
        session: Session,
        *,
        layer_id: int,
        mask: Optional[Vector] = None,
    ) -> Matrix:
        from .extract import get_inbound_map as _gim

        effective_mask: Optional[GraphMask] = self._mask
        if mask is not None:
            if mask.dtype is not gb.dtypes.BOOL or mask.size != self.num_vertices:
                raise ValueError("Override mask must be BOOL and size == num_vertices")
            effective_mask = GraphMask(mask, scenario_id=self.scenario_id)

        return _gim(session, self, layer_id=layer_id, mask=effective_mask)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def __repr__(self) -> str:
        layers_str = ", ".join(str(k) for k in sorted(self._layers.keys()))
        return (
            f"Graph(num_vertices={self.num_vertices}, "
            f"scenario_id={self.scenario_id}, "
            f"layers=[{layers_str}], "
            f"num_labels={self.num_labels})"
        )
