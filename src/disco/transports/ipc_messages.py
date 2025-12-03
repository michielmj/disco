from __future__ import annotations

"""IPC message containers for queue-based transports."""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(slots=True)
class IPCEventMsg:
    target_node: str
    target_simproc: str
    epoch: float
    headers: Dict[str, str]
    data: Optional[bytes]
    shm_name: Optional[str]
    size: int


@dataclass(slots=True)
class IPCPromiseMsg:
    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int
