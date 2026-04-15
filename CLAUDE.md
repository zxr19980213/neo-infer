# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

neo-infer is an AMIE+ style rule mining and inference engine for Neo4j knowledge graphs. It discovers Horn clause rules (e.g., `bornIn(X,Z) ∧ locatedIn(Z,Y) → nationality(X,Y)`) and applies them via forward-chaining inference. Built with FastAPI + Neo4j driver.

## Commands

```bash
# Install (editable with dev deps)
pip install -e ".[dev]"

# Run all tests (no live Neo4j needed — all tests use in-memory fakes)
pytest -q

# Run a single test
pytest tests/test_api_smoke.py::test_health -q

# Start dev server (requires running Neo4j instance)
uvicorn main:app --reload

# CLI
neo-infer --help

# Basic syntax check (no linter configured)
python3 -m py_compile neo_infer/<file>.py
```

## Environment Variables

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4jpass   # Neo4j 5.x requires >= 8 chars
NEO4J_DATABASE=neo4j
```

## Architecture

```
FastAPI app (api.py)
├── rule_mining.py        — AMIE+ search: dangling → closing → threshold pruning
├── inference.py          — Single-round & fixpoint forward-chaining
├── rule_management.py    — Rule state machine (discovered→adopted→applied/rejected)
├── conflict_management.py — Conflict pair CRUD, case recording
├── incremental_mining.py — Delta-driven rule updates from ChangeLog
├── incremental_store.py  — ChangeLog & cursor persistence
├── trigger_management.py — APOC trigger lifecycle (optional)
├── query.py              — Cypher query repository (all DB queries centralized here)
├── db.py                 — Neo4j connection client
├── models.py             — Pydantic models & request/response schemas
├── config.py             — Settings via pydantic-settings
└── cli.py                — CLI (pure stdlib urllib, no external HTTP client)
```

Entry point: `main.py` imports the FastAPI `app` from `neo_infer.api`.

### Rule State Machine

```
discovered ──[adopt]──→ adopted ──[inference]──→ applied (terminal)
     │                      │
     └──────[reject]────────┴────────────────→ rejected (terminal)
```

State transitions are enforced server-side. Invalid transitions return 409; missing rules return 404.

### Rule Mining (AMIE+)

- Body length 2–5 (configurable via `MiningConfig.body_length`, default 2)
- Metrics: support, PCA confidence, head coverage
- Pruning: `beam_width`, `head_budget_per_relation`, `confidence_ub_weight`
- `factual_only=true` (default) excludes `is_inferred=true` edges from statistics to prevent pollution

### Inference

- Single-round: apply adopted rules once
- Fixpoint: iterate until no new edges (default max 5 iterations)
- Inferred edges get `is_inferred=true`, `source_rule_id`, `confidence`, `inferred_at`

### ChangeLog / Incremental Mining

- Dual-source: app endpoint (`POST /changes/append`) + APOC triggers
- `dedup_key` unique constraint for cross-source deduplication
- Add/remove event folding within consumption windows
- Trigger filters internal labels to prevent self-trigger loops

## Testing

All 35 tests use monkeypatched fakes (`FakeDB`, `FakeQueryRepository`, etc.) — no live Neo4j required. Tests cover: API surface, rule state transitions, AMIE+ search (length 2/3/N), CLI parsing, schema bootstrap, trigger management.

## Key Design Decisions

- All Cypher queries are centralized in `query.py` (repository pattern) — never write inline Cypher elsewhere
- Schema (indexes/constraints) bootstraps idempotently on API startup — no manual migration
- APOC plugin is optional; only needed for ChangeLog trigger functionality
- CLI uses only stdlib `urllib` (no requests/httpx dependency at runtime)
- The project has no linter/formatter configured
