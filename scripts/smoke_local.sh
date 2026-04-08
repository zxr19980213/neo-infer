#!/usr/bin/env bash
set -euo pipefail

# Local smoke test runner (assumes Neo4j already deployed locally).
# Optional envs:
#   API_BASE_URL        default: http://127.0.0.1:8000
#   NEO4J_URI           default: bolt://127.0.0.1:7687
#   NEO4J_USER          default: neo4j
#   NEO4J_PASSWORD      default: neo4j
#   NEO4J_DATABASE      default: neo4j
#   SMOKE_RESET_DB      default: 0   (set 1 to clear graph first)
#   SMOKE_INIT_DATA     default: 1   (set 0 to skip seed data)
#   SMOKE_APPLY_SCHEMA  default: 1   (set 0 to skip schema bootstrap script)
#   SMOKE_HEALTH_RETRIES default: 20
#   SMOKE_HEALTH_INTERVAL default: 1
#   SMOKE_CURL_TIMEOUT  default: 3

API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8000}"
NEO4J_URI="${NEO4J_URI:-bolt://127.0.0.1:7687}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-neo4j}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"
SMOKE_RESET_DB="${SMOKE_RESET_DB:-0}"
SMOKE_INIT_DATA="${SMOKE_INIT_DATA:-1}"
SMOKE_APPLY_SCHEMA="${SMOKE_APPLY_SCHEMA:-1}"
SMOKE_HEALTH_RETRIES="${SMOKE_HEALTH_RETRIES:-20}"
SMOKE_HEALTH_INTERVAL="${SMOKE_HEALTH_INTERVAL:-1}"
SMOKE_CURL_TIMEOUT="${SMOKE_CURL_TIMEOUT:-3}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() {
  printf '[smoke] %s\n' "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf '[smoke] missing command: %s\n' "$1" >&2
    exit 1
  fi
}

run_cypher() {
  local query="$1"
  if command -v cypher-shell >/dev/null 2>&1; then
    cypher-shell -a "$NEO4J_URI" -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" -d "$NEO4J_DATABASE" "$query" >/dev/null
  else
    CYPHER_QUERY="$query" \
    NEO4J_URI="$NEO4J_URI" \
    NEO4J_USER="$NEO4J_USER" \
    NEO4J_PASSWORD="$NEO4J_PASSWORD" \
    NEO4J_DATABASE="$NEO4J_DATABASE" \
    python3 - <<'PY'
import os
from neo4j import GraphDatabase

uri = os.environ["NEO4J_URI"]
user = os.environ["NEO4J_USER"]
password = os.environ["NEO4J_PASSWORD"]
database = os.environ["NEO4J_DATABASE"]
query = os.environ["CYPHER_QUERY"]

driver = GraphDatabase.driver(uri, auth=(user, password))
with driver.session(database=database) as session:
    session.run(query).consume()
driver.close()
PY
  fi
}

http_json() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"
  local body_file http_code
  body_file="$(mktemp)"
  if [[ -n "$payload" ]]; then
    http_code="$(curl -sS -o "$body_file" -w '%{http_code}' -X "$method" \
      -H "Content-Type: application/json" \
      -d "$payload" \
      "${API_BASE_URL}${path}")"
  else
    http_code="$(curl -sS -o "$body_file" -w '%{http_code}' -X "$method" \
      "${API_BASE_URL}${path}")"
  fi
  if [[ "$http_code" != "200" ]]; then
    printf '[smoke] %s %s failed: HTTP %s\n' "$method" "$path" "$http_code" >&2
    cat "$body_file" >&2
    rm -f "$body_file"
    exit 1
  fi
  cat "$body_file"
  rm -f "$body_file"
}

json_assert() {
  local body="$1"
  local expr="$2"
  JSON_BODY="$body" python3 - "$expr" <<'PY'
import json
import os
import sys

expr = sys.argv[1]
d = json.loads(os.environ["JSON_BODY"])
ok = eval(
    expr,
    {"__builtins__": {}},
    {
        "d": d,
        "len": len,
        "isinstance": isinstance,
        "list": list,
        "dict": dict,
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "sorted": sorted,
        "any": any,
        "all": all,
    },
)
if not ok:
    print("assertion failed:", expr)
    print("payload:", d)
    raise SystemExit(1)
PY
}

extract_json() {
  local body="$1"
  local py="$2"
  JSON_BODY="$body" python3 - "$py" <<'PY'
import json
import os
import sys

d = json.loads(os.environ["JSON_BODY"])
code = sys.argv[1]
print(eval(code, {"__builtins__": {}}, {"d": d, "len": len, "any": any}))
PY
}

wait_health() {
  local n=0
  log "wait API health: ${API_BASE_URL}/health"
  until curl -sS --connect-timeout 1 --max-time "$SMOKE_CURL_TIMEOUT" "${API_BASE_URL}/health" >/dev/null 2>&1; do
    n=$((n + 1))
    log "health retry ${n}/${SMOKE_HEALTH_RETRIES}"
    if [[ "$n" -ge "$SMOKE_HEALTH_RETRIES" ]]; then
      printf '[smoke] API not ready at %s/health (run uvicorn main:app --reload first)\n' "$API_BASE_URL" >&2
      exit 1
    fi
    sleep "$SMOKE_HEALTH_INTERVAL"
  done
  log "API health ready"
}

seed_data() {
  if [[ "$SMOKE_RESET_DB" == "1" ]]; then
    log "reset graph (MATCH DETACH DELETE)"
    run_cypher "MATCH (n) DETACH DELETE n;"
  fi

  log "seed deterministic test graph"
  run_cypher "
MERGE (alice:Entity {id:'alice'})
MERGE (bob:Entity {id:'bob'})
MERGE (beijing:Entity {id:'beijing'})
MERGE (shanghai:Entity {id:'shanghai'})
MERGE (china:Entity {id:'china'})
MERGE (asia:Entity {id:'asia'})

MERGE (alice)-[:bornIn]->(beijing)
MERGE (bob)-[:bornIn]->(shanghai)
MERGE (beijing)-[:locatedIn]->(china)
MERGE (shanghai)-[:locatedIn]->(china)
MERGE (china)-[:partOf]->(asia)
MERGE (alice)-[:nationality]->(china)
MERGE (alice)-[:region]->(asia)
MERGE (bob)-[:noNationality]->(china);
"
}

main() {
  require_cmd curl
  require_cmd python3

  log "workspace: ${ROOT_DIR}"
  wait_health

  if [[ "$SMOKE_APPLY_SCHEMA" == "1" ]]; then
    log "apply schema script"
    (cd "$ROOT_DIR" && python3 scripts/apply_neo4j_schema.py >/dev/null)
  fi

  if [[ "$SMOKE_INIT_DATA" == "1" ]]; then
    seed_data
  fi

  log "health check"
  health_body="$(http_json GET /health)"
  json_assert "$health_body" 'd.get("status") == "ok"'

  log "mine length2 rules"
  mine2_body="$(http_json POST /rules/mine '{"body_length":2,"limit":100,"min_support":1,"min_pca_confidence":0.1}')"
  json_assert "$mine2_body" 'isinstance(d.get("rules"), list) and len(d["rules"]) > 0'
  rule_id="$(extract_json "$mine2_body" 'next((r["rule_id"] for r in d.get("rules", []) if r.get("head_relation") == "nationality"), d["rules"][0]["rule_id"])')"

  log "mine length3 rules"
  mine3_body="$(http_json POST /rules/mine/length3 '{"body_length":3,"limit":100,"min_support":1,"min_pca_confidence":0.1}')"
  json_assert "$mine3_body" 'isinstance(d.get("rules"), list) and any(len(r.get("body_relations", [])) == 3 for r in d["rules"])'

  log "list discovered rules"
  list_body="$(http_json GET '/rules?status=discovered&limit=200')"
  json_assert "$list_body" 'any(r.get("rule_id") == "'"$rule_id"'" for r in d.get("rules", []))'

  log "adopt rule: ${rule_id}"
  adopt_body="$(http_json POST "/rules/${rule_id}/adopt")"
  json_assert "$adopt_body" 'd.get("status") in ("adopted", "not_found")'

  log "configure conflict pairs"
  conflicts_put="$(http_json PUT /conflicts '{"pairs":{"nationality":["noNationality"]}}')"
  json_assert "$conflicts_put" '"nationality" in d.get("pairs", {})'

  # Re-adopt once more before inference to avoid status changed in prior runs.
  _="$(http_json POST "/rules/${rule_id}/adopt")"

  log "run inference with conflict check"
  infer_body="$(http_json POST /inference/run '{"limit_rules":100,"fixpoint":false,"check_conflicts":true}')"
  json_assert "$infer_body" 'isinstance(d.get("results"), list) and "total_conflicts" in d'

  log "query conflict cases"
  cases_body="$(http_json GET '/conflicts/cases?limit=50')"
  json_assert "$cases_body" 'isinstance(d.get("cases"), list)'
  alias_cases_body="$(http_json GET '/conflict-cases?limit=50')"
  json_assert "$alias_cases_body" 'isinstance(d.get("cases"), list)'

  log "append changelog (added edges)"
  append1_body="$(http_json POST /changes/append '{"added_edges":[{"src":"u1","rel":"bornIn","dst":"u2"},{"src":"u2","rel":"locatedIn","dst":"u3"}],"removed_edges":[]}')"
  json_assert "$append1_body" 'int(d.get("pending_count", 0)) >= 1'

  log "consume changelog (non-empty delta)"
  consume1_body="$(http_json POST /rules/mine/incremental/from-changelog '{"limit":100,"min_support":1,"min_pca_confidence":0.1,"body_length":2}')"
  json_assert "$consume1_body" 'int(d.get("processed_changes", 0)) >= 1 and isinstance(d.get("affected_relations"), list)'

  log "consume changelog again (idempotent)"
  consume2_body="$(http_json POST /rules/mine/incremental/from-changelog '{"limit":100,"min_support":1,"min_pca_confidence":0.1,"body_length":2}')"
  json_assert "$consume2_body" 'int(d.get("processed_changes", 0)) == 0'

  log "append mixed add/remove delta"
  append2_body="$(http_json POST /changes/append '{"added_edges":[{"src":"u10","rel":"bornIn","dst":"u20"}],"removed_edges":[{"src":"u2","rel":"locatedIn","dst":"u3"}]}')"
  json_assert "$append2_body" 'int(d.get("pending_count", 0)) >= 1'

  log "consume mixed delta"
  consume3_body="$(http_json POST /rules/mine/incremental/from-changelog '{"limit":100,"min_support":1,"min_pca_confidence":0.1,"body_length":2}')"
  json_assert "$consume3_body" 'int(d.get("processed_changes", 0)) >= 1 and isinstance(d.get("rules"), list)'

  log "all smoke checks passed"
}

main "$@"

