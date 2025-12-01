# AGENTS.md — Rules for Codex

## General Principles
Codex must:
- Follow `ENGINEERING_SPEC.md` for architectural guidance.
- Prefer simple, clear, explicit code.
- Use modern Python (3.11+).
- Keep modules small and well separated.
- Use type hints everywhere and ensure the project passes `mypy` with `--strict`.

## Code Requirements
- All new modules must include full type annotations.
- All public APIs must include docstrings.
- Use `dataclasses` with `slots=True` for lightweight containers.
- Avoid unnecessary abstraction; keep things direct and composable.

## Serialization & Transport
Follow the guidance in `ENGINEERING_SPEC.md`:
- Serialization occurs only in `NodeController`.
- `_deliver_local_event` and `_deliver_local_promise` must exist but be empty.
- No EventQueue initialization in this iteration.
- Transports treat data as opaque bytes.
- `"self"` is a valid `target_node` alias.

## Testing
- All new code must be accompanied by `pytest` test cases.
- Tests must cover:
  - Local vs remote behavior for `NodeController`.
  - `"self"` alias behavior.
  - Serialization is called exactly once per send.
  - InProcessTransport correctly calls the receiver.
- Tests should live in the `tests/` directory and follow the `test_*.py` naming pattern.

## Static Checking
- All code MUST pass `mypy --strict`.
- If type inference is inadequate, add explicit type annotations rather than suppressions.
- Never use `# type: ignore` unless absolutely unavoidable, and justify it with a comment.

## Repository Expectations
- Code lives under `src/disco/`.
- Tests live under `tests/`.
- Prefer small files over large ones.
- Keep imports local to modules where needed (avoid heavy top-level imports).
- Install development dependencies with `pip install -e '.[dev]'` before running tests.

## Pull Requests
Codex should:
- Open a pull request for each task unless instructed otherwise.
- Include a summary of changes.
- Ensure the PR passes tests and type checks.
