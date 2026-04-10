## Cursor Cloud specific instructions

### Services overview

| Service | Required | Default Port | Notes |
|---------|----------|-------------|-------|
| neo-infer (FastAPI/Uvicorn) | Yes | 8000 | `uvicorn main:app --reload` |
| Neo4j 5.x | Yes | 7687 (bolt) | Must be running before the API starts |

### Environment variables

Set before starting the API server:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4jpass
NEO4J_DATABASE=neo4j
```

### Commands quick reference

See `README.md` for full documentation. Key commands:

- **Install deps**: `pip install -e ".[dev]"`
- **Run tests**: `pytest -q` (all tests use monkeypatched fakes; no live Neo4j needed)
- **Start dev server**: `uvicorn main:app --reload` (requires running Neo4j)
- **CLI**: `neo-infer --help`

### Non-obvious caveats

1. **Neo4j password minimum**: Neo4j 5.x requires passwords of at least 8 characters. The README examples use `neo4j` as password but that will fail on fresh installs; use `neo4jpass` or similar.
2. **Schema bootstrap on startup**: The API automatically creates Neo4j indexes/constraints on startup (idempotent). No manual migration step needed.
3. **`on_event` deprecation warning**: FastAPI emits a `DeprecationWarning` about `on_event` usage. This is a known issue (tracked in PLAN P1). Tests still pass.
4. **Tests are fully mocked**: All 25 pytest tests use in-memory fakes and do not require a running Neo4j instance. Only the dev server and CLI require a live Neo4j.
5. **No linter configured**: The project does not include ruff/flake8/mypy/pylint configuration. Use `python3 -m py_compile` for basic syntax checking.
6. **APOC plugin is optional**: Only needed for ChangeLog trigger functionality. Core mining/inference works without it.
