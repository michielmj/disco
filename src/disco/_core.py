from __future__ import annotations

"""Pure-Python fallback for the compiled core extension used in tests."""


def example() -> int:
    """Return the sentinel value used by smoke tests."""

    return 42


__all__ = ["example"]
