# neo-infer

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-green.svg)](https://www.python.org/downloads/)

AMIE+ style rule mining and inference over Neo4j knowledge graphs.

From a knowledge graph, neo-infer automatically discovers Horn clause rules like:

```
bornIn(X,Z) ∧ locatedIn(Z,Y) -> nationality(X,Y)
```

Then applies adopted rules via forward-chaining inference to create new relationships.

## Quick Start

### Prerequisites

- Python >= 3.10
- Neo4j 5.x (Community Edition works)

### Install & Run

```bash
pip install -e .
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password"
uvicorn main:app --reload
```

Open http://localhost:8000/console for the web UI, or use the CLI:

```bash
neo-infer health
neo-infer mine --body-length 2 --limit 100 --min-support 1 --min-pca-confidence 0.1
neo-infer rules list --status discovered
neo-infer infer --limit-rules 100
```

### 5-Minute Walkthrough

```bash
# 1. Seed sample data
cypher-shell -a bolt://localhost:7687 -u neo4j -p your_password '
CREATE (a:Entity {id:"alice"}), (b:Entity {id:"bob"}),
       (bj:Entity {id:"beijing"}), (sh:Entity {id:"shanghai"}),
       (cn:Entity {id:"china"});
CREATE (a)-[:bornIn]->(bj), (b)-[:bornIn]->(sh),
       (bj)-[:locatedIn]->(cn), (sh)-[:locatedIn]->(cn),
       (a)-[:nationality]->(cn);'

# 2. Mine rules
curl -X POST http://localhost:8000/rules/mine \
  -H "Content-Type: application/json" \
  -d '{"body_length":2, "limit":100, "min_support":1, "min_pca_confidence":0.1}'

# 3. Adopt a discovered rule
curl -X POST http://localhost:8000/rules/rule__bornin__locatedin__to__nationality/adopt

# 4. Run inference -> creates bob-[:nationality]->china
curl -X POST http://localhost:8000/inference/run \
  -H "Content-Type: application/json" \
  -d '{"limit_rules":100, "fixpoint":false}'
```

## Features

| Feature | Description |
|---------|-------------|
| Rule Mining | AMIE+ algorithm, body length 2~5, with beam search and pruning |
| Inference | Single-round and fixpoint forward-chaining, with conflict detection |
| Rule Lifecycle | State machine: `discovered -> adopted -> applied` / `rejected` |
| Incremental Mining | ChangeLog-driven delta mining with cursor-based consumption |
| Web Console | Browser UI at `/console` for mining, rule management, inference |
| CLI | `neo-infer` command for all operations |
| Trigger Support | APOC trigger for auto-capturing graph mutations (Neo4j 4.x/5.x) |

## Rule Mining Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `body_length` | 2 | Path hops (2~5). Length 2/3 use optimized queries; 4/5 use dynamic Cypher. |
| `min_support` | 5 | Minimum support count |
| `min_pca_confidence` | 0.1 | Minimum PCA confidence threshold |
| `factual_only` | true | Exclude inferred edges from statistics |
| `beam_width` | - | Top-B body candidates per level |
| `head_budget_per_relation` | - | Max K rules per head relation |
| `candidate_limit` | - | Max total candidates to evaluate |

## Rule State Machine

```
discovered --[adopt]--> adopted --[inference]--> applied (terminal)
     |                     |
     +-----[reject]------->+---------> rejected (terminal)
```

API enforces transitions: invalid transitions return `409 Conflict`, missing rules return `404`.

## Project Structure

```
neo_infer/
  api.py                 # FastAPI routes + web console
  rule_mining.py         # AMIE+ mining engine
  inference.py           # Forward-chaining inference
  rule_management.py     # Rule store + state machine
  conflict_management.py # Conflict detection
  incremental_mining.py  # ChangeLog-driven incremental mining
  incremental_store.py   # ChangeLog / cursor persistence
  trigger_management.py  # APOC trigger lifecycle
  query.py               # Cypher query repository
  models.py              # Pydantic data models
  config.py              # Environment-based settings
  db.py                  # Neo4j driver wrapper
  cli.py                 # Command-line interface
```

## Documentation

| Document | Content |
|----------|---------|
| [API Reference](docs/api-reference.md) | All endpoints, parameters, request/response examples |
| [Configuration](docs/configuration.md) | Environment variables, Neo4j/APOC setup, schema |
| [Testing & Benchmarks](docs/testing.md) | Automated tests, manual testing guide, benchmarks |
| [Architecture & Roadmap](docs/architecture.md) | Algorithm design, implementation plan, status |

## Development

```bash
pip install -e ".[dev]"
pytest -q              # 35 tests, all mocked (no Neo4j needed)
uvicorn main:app --reload  # dev server with hot reload
```

## License

[MIT](LICENSE)
