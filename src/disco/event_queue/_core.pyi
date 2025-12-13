from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class EventQueue:
    def __init__(self) -> None: ...

    @property
    def epoch(self) -> float: ...

    @property
    def next_epoch(self) -> Optional[float]: ...

    @property
    def waiting_for(self) -> str: ...

    def push(
        self,
        sender: str,
        epoch: float,
        data: Any,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool: ...

    def pop(self) -> List[Tuple[str, float, Any, Dict[str, str]]]: ...

    def pop_all(self) -> List[Tuple[str, float, Any, Dict[str, str]]]: ...

    def promise(self, sender: str, seqnr: int, epoch: float, num_events: int) -> bool: ...

    def try_next_epoch(self) -> bool: ...

    @property
    def has_predecessors(self) -> bool: ...

    def register_predecessor(self, predecessor: str) -> None: ...

    @property
    def empty(self) -> bool: ...
