import uuid
from datetime import datetime, timedelta
from typing import Optional, List

import graphblas as gb
from graphblas import Vector
from sqlalchemy import insert, delete, update, select
from sqlalchemy.orm import Session

from .schema import vertex_masks


class GraphMask:
    """
    A persisted boolean vertex mask tied to a scenario.

    - Wraps a python-graphblas Vector[BOOL].
    - Lazily persisted to the database on first use (ensure_persisted).
    - Subsequent uses only bump updated_at.
    - Intended to be short-lived (no longer than the associated Graph).
    """

    __slots__ = ("vector", "scenario_id", "mask_id", "_stored")

    def __init__(
        self,
        vector: Vector,
        scenario_id: int,
        mask_id: Optional[str] = None,
    ) -> None:
        if vector.dtype is not gb.dtypes.BOOL:
            raise TypeError(f"GraphMask vector must have BOOL dtype, got {vector.dtype!r}")
        self.vector = vector
        self.scenario_id = int(scenario_id)
        self.mask_id = mask_id or str(uuid.uuid4())
        self._stored: bool = False

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def ensure_persisted(self, session: Session) -> None:
        """
        Ensure the mask is present in graph.vertex_masks.

        - If it's not stored yet:
            - Write (scenario_id, mask_id, vertex_index, updated_at) rows.
        - If it is stored:
            - Only bump updated_at via an UPDATE.
        """
        if not self._stored:
            self._write_full(session)
            self._stored = True
        else:
            self._touch(session)

    def delete(self, session: Session) -> None:
        """
        Remove this mask from the database (if it was stored).
        """
        if not self._stored:
            return
        session.execute(
            delete(vertex_masks).where(
                vertex_masks.c.scenario_id == self.scenario_id,
                vertex_masks.c.mask_id == self.mask_id,
            )
        )
        self._stored = False

    @classmethod
    def cleanup_old(
        cls,
        session: Session,
        max_age_minutes: int = 60,
    ) -> None:
        """
        Remove all masks whose updated_at is older than the configured age.
        """
        cutoff = datetime.utcnow() - timedelta(minutes=max_age_minutes)
        session.execute(
            delete(vertex_masks).where(vertex_masks.c.updated_at < cutoff)
        )

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _write_full(self, session: Session) -> None:
        """
        Write the full contents of this mask into graph.vertex_masks.

        Strategy:
        - DELETE any existing rows for (scenario_id, mask_id).
        - INSERT one row per *True* vertex index.
        """
        indices, values = self.vector.to_coo()
        now = datetime.utcnow()

        session.execute(
            delete(vertex_masks).where(
                vertex_masks.c.scenario_id == self.scenario_id,
                vertex_masks.c.mask_id == self.mask_id,
            )
        )

        rows = [
            {
                "scenario_id": self.scenario_id,
                "mask_id": self.mask_id,
                "vertex_index": int(idx),
                "updated_at": now,
            }
            for idx, val in zip(indices, values)
            if bool(val)
        ]

        if rows:
            session.execute(insert(vertex_masks), rows)

    def _touch(self, session: Session) -> None:
        """
        Bump updated_at for all rows of this mask to mark recent use.
        """
        now = datetime.utcnow()
        session.execute(
            update(vertex_masks)
            .where(
                vertex_masks.c.scenario_id == self.scenario_id,
                vertex_masks.c.mask_id == self.mask_id,
            )
            .values(updated_at=now)
        )

    def exists_in_db(self, session: Session) -> bool:
        """
        Optional helper: check if this mask currently has any rows in DB.
        """
        result = session.execute(
            select(vertex_masks.c.mask_id)
            .where(
                vertex_masks.c.scenario_id == self.scenario_id,
                vertex_masks.c.mask_id == self.mask_id,
            )
            .limit(1)
        ).scalar_one_or_none()
        return result is not None

    @classmethod
    def load_indices(
        cls,
        session: Session,
        scenario_id: int,
        mask_id: str,
    ) -> List[int]:
        """
        Utility to fetch vertex indices for a persisted mask.
        """
        result = session.execute(
            select(vertex_masks.c.vertex_index).where(
                vertex_masks.c.scenario_id == scenario_id,
                vertex_masks.c.mask_id == mask_id,
            )
        )
        return [int(row[0]) for row in result]
