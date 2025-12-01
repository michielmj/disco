from __future__ import annotations

"""Envelope definitions for disco routing."""

from dataclasses import dataclass, field
from typing import Final


@dataclass(slots=True)
class EventEnvelope:
    """Container for event payloads destined for a simulation process."""

    target_node: str
    target_simproc: str
    epoch: float
    data: bytes
    headers: dict[str, str] = field(default_factory=dict)

    kind: Final[str] = "event"


@dataclass(slots=True)
class PromiseEnvelope:
    """Container for promise messages destined for a simulation process."""

    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int

    kind: Final[str] = "promise"
