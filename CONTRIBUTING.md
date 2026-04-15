# Contributing to neo-infer

Thank you for your interest in contributing!

## Development Setup

1. Fork and clone the repo
2. Install with dev dependencies: `pip install -e ".[dev]"`
3. Run tests: `pytest -q` (no Neo4j needed -- all tests use in-memory fakes)
4. Start dev server (requires Neo4j): `uvicorn main:app --reload`

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full design document.

Key conventions:
- All Cypher queries go in `neo_infer/query.py` (repository pattern -- no inline Cypher elsewhere)
- Schema bootstraps idempotently on startup -- no migration scripts needed
- Tests use monkeypatched fakes (`FakeDB`, `FakeQueryRepository`) -- see `tests/` for patterns

## Making Changes

1. Create a branch from `main`
2. Write tests for new features
3. Run `pytest -q` and ensure all tests pass
4. Open a pull request against `main`

## Reporting Bugs

Use the [bug report template](https://github.com/zxr19980213/neo-infer/issues/new?template=bug_report.md).

## Suggesting Features

Use the [feature request template](https://github.com/zxr19980213/neo-infer/issues/new?template=feature_request.md).
