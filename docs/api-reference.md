# API Reference

Base URL: `http://localhost:8000`

## Health

### `GET /health`
```json
{"status": "ok", "db": "bolt://localhost:7687"}
```

## Rule Mining

### `POST /rules/mine`

Mine Horn clause rules from the knowledge graph.

**Request:**
```json
{
  "body_length": 2,
  "limit": 200,
  "min_support": 1,
  "min_pca_confidence": 0.1,
  "min_head_coverage": 0.0,
  "candidate_limit": 5000,
  "factual_only": true,
  "beam_width": null,
  "head_budget_per_relation": null,
  "confidence_ub_weight": 0.0
}
```

**Parameters:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `body_length` | int | 2 | Path hops (2~5) |
| `limit` | int | 200 | Max rules to return |
| `min_support` | int | 5 | Minimum support threshold |
| `min_pca_confidence` | float | 0.1 | Minimum PCA confidence |
| `min_head_coverage` | float | 0.0 | Minimum head coverage |
| `candidate_limit` | int | auto | Max candidates to evaluate |
| `factual_only` | bool | true | Exclude `is_inferred=true` edges |
| `beam_width` | int | null | Top-B candidates per expansion level |
| `head_budget_per_relation` | int | null | Max rules per head relation |
| `confidence_ub_weight` | float | 0.0 | Confidence upper-bound tightening (0~1) |

**Response:**
```json
{
  "rules": [
    {
      "rule_id": "rule__bornin__locatedin__to__nationality",
      "body_relations": ["bornIn", "locatedIn"],
      "head_relation": "nationality",
      "support": 2,
      "pca_confidence": 1.0,
      "head_coverage": 1.0,
      "status": "discovered",
      "version": 1
    }
  ]
}
```

## Rule Management

### `GET /rules?status=discovered&limit=100`

List rules with optional status filter.

### `POST /rules/{rule_id}/adopt`

Transition rule from `discovered` to `adopted`.

- `200`: success
- `404`: rule not found
- `409`: invalid transition (e.g., rule is already `applied`)

### `POST /rules/{rule_id}/reject`

Transition rule to `rejected` (from `discovered` or `adopted`).

Same response codes as adopt.

## Inference

### `POST /inference/run`

Apply adopted rules via forward-chaining inference.

**Request:**
```json
{
  "limit_rules": 100,
  "fixpoint": false,
  "max_iterations": 5,
  "check_conflicts": true,
  "conflict_pairs": {
    "nationality": ["noNationality"]
  }
}
```

**Response:**
```json
{
  "results": [
    {
      "rule_id": "rule__bornin__locatedin__to__nationality",
      "created_triples": 1,
      "conflict_triples": 0,
      "iteration": 1
    }
  ],
  "total_created": 1,
  "total_conflicts": 0,
  "iterations_run": 1
}
```

Conflict resolution priority: request `conflict_pairs` > database `ConflictRule` > env `CONFLICT_RELATION_PAIRS`.

## Conflict Management

### `GET /conflicts`
List configured conflict relation pairs.

### `PUT /conflicts`
Replace all conflict pairs.

```json
{"pairs": {"nationality": ["noNationality"]}}
```

### `POST /conflicts`
Add conflict pairs (merge with existing).

### `DELETE /conflicts/{inferred_relation}/{conflicting_relation}`
Delete a single conflict pair. Returns `404` if not found.

### `GET /conflicts/cases?limit=100`
List recorded conflict instances.

## Incremental Mining

### `POST /changes/append`

Append edge changes to the ChangeLog.

```json
{
  "added_edges": [{"src": "u1", "rel": "bornIn", "dst": "u2"}],
  "removed_edges": [],
  "batch_id": "optional-batch-id",
  "idempotency_key": "optional-idem-key",
  "context": {"actor": "admin"}
}
```

### `POST /rules/mine/incremental/from-changelog`

Consume pending changes and mine affected rules.

```json
{
  "body_length": 2,
  "limit": 100,
  "min_support": 1,
  "min_pca_confidence": 0.1,
  "change_limit": 1000
}
```

**Response:**
```json
{
  "processed_changes": 2,
  "affected_relations": ["bornIn", "locatedIn"],
  "rules": [...]
}
```

## Trigger Management

### `POST /triggers/changelog/install`
Install APOC changelog trigger. Returns `412` if APOC not available.

### `DELETE /triggers/changelog`
Uninstall the changelog trigger.

## Web Console

### `GET /console`
Lightweight browser UI with:
- Rule mining (body length 2~5)
- Rules Management table (Adopt / Reject / filter by status)
- Inference execution
- Change log append
- Incremental consume
