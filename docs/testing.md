# Testing & Benchmarks

## Automated Tests

35 pytest smoke tests covering the full API surface. All tests use in-memory fakes (no live Neo4j needed).

```bash
pip install -e ".[dev]"
pytest -q
```

### Test Coverage

| Area | Tests | Description |
|------|-------|-------------|
| Rule Mining | 3 | Length-2, length-3, incremental mining |
| Inference | 2 | Single-round, fixpoint |
| Conflicts | 3 | CRUD, conflict cases, length-3 consistency |
| Incremental | 5 | ChangeLog append, consume, delta fold, cursor |
| State Transitions | 10 | All adopt/reject paths, 404, 409 |
| Schema | 1 | Bootstrap verification |
| Trigger | 1 | Install diagnostic |
| Console | 1 | HTML smoke test |

## Manual Testing (with Neo4j)

### Setup

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your_password"
export NEO4J_DATABASE="neo4j"
pip install -e ".[dev]"
uvicorn main:app --reload
```

### Seed Test Data

```bash
cypher-shell -a $NEO4J_URI -u $NEO4J_USER -p $NEO4J_PASSWORD '
MATCH (n) DETACH DELETE n;
CREATE (alice:Entity {id:"alice"}), (bob:Entity {id:"bob"}),
       (beijing:Entity {id:"beijing"}), (shanghai:Entity {id:"shanghai"}),
       (china:Entity {id:"china"}), (asia:Entity {id:"asia"});
CREATE (alice)-[:bornIn]->(beijing),
       (bob)-[:bornIn]->(shanghai),
       (beijing)-[:locatedIn]->(china),
       (shanghai)-[:locatedIn]->(china),
       (china)-[:partOf]->(asia),
       (alice)-[:nationality]->(china),
       (alice)-[:region]->(asia),
       (bob)-[:noNationality]->(china);'
```

### Full Pipeline Test

```bash
# Health check
curl http://localhost:8000/health

# Mine rules
curl -X POST http://localhost:8000/rules/mine \
  -H "Content-Type: application/json" \
  -d '{"body_length":2, "limit":100, "min_support":1, "min_pca_confidence":0.1}'

# List discovered rules
curl "http://localhost:8000/rules?status=discovered&limit=100"

# Adopt a rule
curl -X POST http://localhost:8000/rules/<RULE_ID>/adopt

# Run inference
curl -X POST http://localhost:8000/inference/run \
  -H "Content-Type: application/json" \
  -d '{"limit_rules":100, "fixpoint":false, "check_conflicts":false}'

# Set up conflicts
curl -X PUT http://localhost:8000/conflicts \
  -H "Content-Type: application/json" \
  -d '{"pairs":{"nationality":["noNationality"]}}'

# Run inference with conflict detection
curl -X POST http://localhost:8000/inference/run \
  -H "Content-Type: application/json" \
  -d '{"limit_rules":100, "fixpoint":false, "check_conflicts":true}'

# Check conflict cases
curl "http://localhost:8000/conflicts/cases?limit=50"
```

### Incremental Mining Test

```bash
# Append changes
curl -X POST http://localhost:8000/changes/append \
  -H "Content-Type: application/json" \
  -d '{"added_edges":[{"src":"u1","rel":"bornIn","dst":"u2"},{"src":"u2","rel":"locatedIn","dst":"u3"}],"removed_edges":[]}'

# Consume and mine
curl -X POST http://localhost:8000/rules/mine/incremental/from-changelog \
  -H "Content-Type: application/json" \
  -d '{"body_length":2, "limit":100, "min_support":1, "min_pca_confidence":0.1}'

# Idempotent re-consume (should return processed_changes=0)
curl -X POST http://localhost:8000/rules/mine/incremental/from-changelog \
  -H "Content-Type: application/json" \
  -d '{"body_length":2, "limit":100, "min_support":1, "min_pca_confidence":0.1}'
```

## Benchmarks

### Generate Large Dataset

```bash
python scripts/bench_seed_large.py \
  --reset \
  --num-person 200000 \
  --num-city 5000 \
  --num-country 500 \
  --num-region 200 \
  --batch-size 5000
```

### Run Benchmarks

```bash
python scripts/bench_api_perf.py \
  --api-base-url http://localhost:8000 \
  --body-length 2 \
  --mine-loops 3 \
  --infer-loops 3 \
  --top-k 1000 \
  --infer-limit-rules 500 \
  --output-json bench_api_perf.json
```

### Index Strategy Comparison

```bash
python scripts/bench_index_strategies.py \
  --api-base-url http://localhost:8000 \
  --body-length 2 \
  --mine-loops 3 \
  --infer-loops 3 \
  --output-json bench_index_compare.json
```

Built-in strategies: `baseline` (constraints only), `lean` (+ common indexes), `aggressive` (+ composite indexes).
