# Configuration

## Environment Variables

All settings are read from environment variables at startup.

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt endpoint |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `neo4j` | Neo4j password |
| `NEO4J_DATABASE` | `neo4j` | Target database name |
| `MIN_SUPPORT` | `5` | Default minimum support |
| `MIN_CONFIDENCE` | `0.1` | Default minimum PCA confidence |
| `MAX_RULE_LENGTH` | `2` | Default max rule body length |
| `CONFLICT_RELATION_PAIRS` | (empty) | Fallback conflict config, e.g. `nationality:noNationality,region:noRegion` |
| `CHANGELOG_TRIGGER_AUTO_INSTALL` | `0` | Set `1` to auto-install APOC trigger on startup |
| `CHANGELOG_TRIGGER_NAME` | `neo_infer_changelog` | Trigger name in APOC |

## Neo4j Setup

### Version
- Neo4j 5.x recommended (compatible with 4.x for core features)
- Community Edition works

### APOC Plugin (Optional)
Required only for ChangeLog trigger functionality. Core mining/inference works without it.

1. Install APOC matching your Neo4j major version (e.g., Neo4j 5.x -> APOC 5.x)
2. Place APOC jar in Neo4j `plugins/` directory
3. Add to `neo4j.conf`:
   ```
   dbms.security.procedures.allowlist=apoc.*
   dbms.security.procedures.unrestricted=apoc.*
   ```
4. Add to `apoc.conf`:
   ```
   apoc.trigger.enabled=true
   ```
5. Restart Neo4j

### Verify APOC
```cypher
RETURN apoc.version();
-- Neo4j 5.x:
CALL apoc.trigger.show('neo4j');
-- Neo4j 4.x:
CALL apoc.trigger.list();
```

### Trigger Installation

Via API (recommended):
```bash
curl -X POST http://localhost:8000/triggers/changelog/install
curl -X DELETE http://localhost:8000/triggers/changelog  # uninstall
```

Or auto-install on startup:
```bash
export CHANGELOG_TRIGGER_AUTO_INSTALL=1
```

### Trigger FAQ

**"No write operations are allowed ... FOLLOWER"**
Connect to the writable leader node. For standalone: use `bolt://127.0.0.1:7687`.

**"installed" but not visible in queries**
Neo4j 5.x: use `CALL apoc.trigger.show('neo4j')` on system db (not `apoc.trigger.list()`).
The service auto-detects the correct API.

## Schema

The API automatically bootstraps Neo4j indexes and constraints on startup (idempotent):

- `Rule.rule_id` unique constraint
- `ConflictRule(head_relation, conflict_relation)` composite unique
- `RelationType.name` unique constraint
- `RuleStat.rule_id` unique constraint
- `IncrementalState.name` unique constraint
- `IdSequence.name` unique constraint
- `ChangeLog.change_seq` unique constraint
- `ChangeLog.dedup_key` unique constraint
- Query indexes on `Rule.head_relation`, `Rule.status`, `ChangeLog.rel`, `ChangeLog.event_type`, `ConflictCase.rule_id`, etc.

Manual schema application:
```bash
python scripts/apply_neo4j_schema.py
# or directly:
# cypher-shell -f scripts/neo4j_schema.cypher
```

## Database Selection

All mining, inference, and rule management queries execute on the database specified by `NEO4J_DATABASE`. Trigger management operations use the `system` database (required by APOC).

The Neo4j driver creates a connection pool at startup. Each request opens a session on the target database.

## ChangeLog Mixed Mode (Trigger + App)

Two sources of changelog entries:
- **App**: via `POST /changes/append` (explicit)
- **Trigger**: APOC trigger auto-captures Neo4j relationship mutations

Deduplication: `ChangeLog.dedup_key` unique constraint. Same-window add/remove events on the same edge are folded (cancelled out).

Trigger auto-filters internal labels (`ChangeLog`, `IdSequence`, `Rule`, etc.) to prevent self-triggering loops.

### Retention
Recommended: keep consumed entries for audit/replay. Clean up by time window (e.g., 30 days) or by cursor threshold.
