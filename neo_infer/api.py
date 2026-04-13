from __future__ import annotations

import time

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from neo_infer.config import Settings, get_settings, parse_conflict_relation_pairs
from neo_infer.conflict_management import ConflictStore
from neo_infer.db import Neo4jClient
from neo_infer.incremental_mining import IncrementalMiningService
from neo_infer.incremental_store import IncrementalStore
from neo_infer.inference import InferenceEngine
from neo_infer.trigger_management import TriggerManager
from neo_infer.models import (
    ChangeEdge,
    ChangeLogAppendRequest,
    ChangeLogPendingResponse,
    EdgeDeltaApplyRequest,
    IncrementalConsumeRequest,
    IncrementalConsumeResponse,
    ConflictCaseListResponse,
    IncrementalMineRequest,
    ConflictPairsResponse,
    ConflictPairsUpdateRequest,
    InferenceRequest,
    InferenceResponse,
    MineRulesRequest,
    MineRulesResponse,
    RuleSetResponse,
)
from neo_infer.query import QueryRepository
from neo_infer.rule_management import InvalidStatusTransition, RuleStore
from neo_infer.rule_mining import MiningConfig, RuleMiningService

app = FastAPI(title="neo-infer", version="0.1.0")

CONSOLE_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>neo-infer console</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 18px; max-width: 980px; }
    h1 { margin-bottom: 0.2rem; }
    .hint { color: #444; margin-top: 0; }
    .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; margin-bottom: 12px; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin: 6px 0; }
    label { min-width: 170px; }
    input, select, textarea, button { font-size: 14px; padding: 6px; }
    textarea { width: 100%; min-height: 120px; font-family: monospace; }
    pre { background: #111; color: #f4f4f4; padding: 10px; border-radius: 6px; overflow: auto; min-height: 180px; }
    #rulesTable { width: 100%; border-collapse: collapse; font-size: 13px; }
    #rulesTable th, #rulesTable td { border: 1px solid #ddd; padding: 6px 8px; text-align: left; }
    #rulesTable th { background: #f5f5f5; }
    #rulesTable tr:hover { background: #f9f9f9; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; color: #fff; }
    .badge-discovered { background: #2196f3; }
    .badge-adopted { background: #4caf50; }
    .badge-applied { background: #009688; }
    .badge-rejected { background: #f44336; }
    .btn-sm { font-size: 12px; padding: 3px 10px; cursor: pointer; border: 1px solid #ccc; border-radius: 4px; }
    .btn-adopt { background: #e8f5e9; color: #2e7d32; }
    .btn-adopt:hover { background: #c8e6c9; }
    .btn-reject { background: #ffebee; color: #c62828; }
    .btn-reject:hover { background: #ffcdd2; }
    .btn-adopt:disabled, .btn-reject:disabled { opacity: 0.4; cursor: default; }
    #rulesPanel .status-filter { margin-right: 8px; }
  </style>
</head>
<body>
  <h1>neo-infer Console</h1>
  <p class="hint">A lightweight browser console for common APIs.</p>

  <div class="card">
    <div class="row">
      <label>API Base URL</label>
      <input id="baseUrl" type="text" value="" style="min-width: 360px" />
      <button onclick="health()">Health</button>
      <button onclick="listRules()">List Rules</button>
      <button onclick="listConflicts()">Conflict Cases</button>
    </div>
  </div>

  <div class="card">
    <h3>Mine Rules</h3>
    <div class="row"><label>Body Length</label><select id="mineBodyLength"><option value="2">2</option><option value="3">3</option></select></div>
    <div class="row"><label>Limit</label><input id="mineLimit" type="number" value="200" /></div>
    <div class="row"><label>Min Support</label><input id="mineSupport" type="number" value="1" /></div>
    <div class="row"><label>Min PCA Confidence</label><input id="minePca" type="number" step="0.01" value="0.1" /></div>
    <div class="row"><button onclick="mineRules()">Run /rules/mine</button></div>
  </div>

  <div class="card" id="rulesPanel">
    <h3>Rules Management</h3>
    <div class="row">
      <label>Status Filter</label>
      <select id="rulesStatusFilter" class="status-filter">
        <option value="">All</option>
        <option value="discovered" selected>discovered</option>
        <option value="adopted">adopted</option>
        <option value="applied">applied</option>
        <option value="rejected">rejected</option>
      </select>
      <label>Limit</label>
      <input id="rulesLimit" type="number" value="100" style="width:80px" />
      <button onclick="loadRulesTable()">Refresh</button>
      <button onclick="adoptAll()">Adopt All Visible</button>
    </div>
    <div id="rulesTableWrap" style="max-height:400px;overflow:auto;margin-top:8px;">
      <p class="hint">Click "Refresh" to load rules.</p>
    </div>
  </div>

  <div class="card">
    <h3>Inference</h3>
    <div class="row"><label>Limit Rules</label><input id="inferLimitRules" type="number" value="100" /></div>
    <div class="row"><label>Fixpoint</label><select id="inferFixpoint"><option value="false">false</option><option value="true">true</option></select></div>
    <div class="row"><label>Max Iterations</label><input id="inferMaxIter" type="number" value="5" /></div>
    <div class="row"><label>Check Conflicts</label><select id="inferConflicts"><option value="true">true</option><option value="false">false</option></select></div>
    <div class="row"><button onclick="runInference()">Run /inference/run</button></div>
  </div>

  <div class="card">
    <h3>Append Changes</h3>
    <p class="hint">One edge per line: src,rel,dst</p>
    <div class="row"><label>Added Edges</label></div>
    <textarea id="addedEdges">u1,bornIn,u2
u2,locatedIn,u3</textarea>
    <div class="row"><label>Removed Edges</label></div>
    <textarea id="removedEdges"></textarea>
    <div class="row"><button onclick="appendChanges()">Run /changes/append</button></div>
  </div>

  <div class="card">
    <h3>Incremental Consume</h3>
    <div class="row"><label>Body Length</label><select id="incBodyLength"><option value="2">2</option><option value="3">3</option></select></div>
    <div class="row"><label>Limit</label><input id="incLimit" type="number" value="100" /></div>
    <div class="row"><label>Change Limit</label><input id="incChangeLimit" type="number" value="1000" /></div>
    <div class="row"><label>Min Support</label><input id="incSupport" type="number" value="1" /></div>
    <div class="row"><label>Min PCA Confidence</label><input id="incPca" type="number" step="0.01" value="0.1" /></div>
    <div class="row"><button onclick="consumeIncremental()">Run /rules/mine/incremental/from-changelog</button></div>
  </div>

  <div class="card">
    <h3>Output</h3>
    <pre id="output"></pre>
  </div>

<script>
  function getBase() {
    const base = document.getElementById("baseUrl").value.trim();
    return base || window.location.origin;
  }
  function setOutput(data) {
    document.getElementById("output").textContent = JSON.stringify(data, null, 2);
  }
  function parseEdges(text) {
    const out = [];
    const lines = text.split("\\n").map(v => v.trim()).filter(v => v.length > 0);
    for (const line of lines) {
      const parts = line.split(",").map(v => v.trim());
      if (parts.length !== 3 || !parts[0] || !parts[1] || !parts[2]) {
        throw new Error("Invalid edge line: " + line + " (expected src,rel,dst)");
      }
      out.push({src: parts[0], rel: parts[1], dst: parts[2]});
    }
    return out;
  }
  async function api(method, path, payload) {
    const resp = await fetch(getBase() + path, {
      method,
      headers: {"Content-Type": "application/json"},
      body: payload ? JSON.stringify(payload) : undefined
    });
    const txt = await resp.text();
    let data = txt;
    try { data = txt ? JSON.parse(txt) : {}; } catch (_) {}
    setOutput({status: resp.status, ok: resp.ok, path, data});
  }
  async function health() { await api("GET", "/health"); }
  async function listRules() { await api("GET", "/rules?limit=100"); }
  async function listConflicts() { await api("GET", "/conflicts/cases?limit=50"); }
  async function mineRules() {
    await api("POST", "/rules/mine", {
      body_length: Number(document.getElementById("mineBodyLength").value),
      limit: Number(document.getElementById("mineLimit").value),
      min_support: Number(document.getElementById("mineSupport").value),
      min_pca_confidence: Number(document.getElementById("minePca").value)
    });
  }
  async function runInference() {
    await api("POST", "/inference/run", {
      limit_rules: Number(document.getElementById("inferLimitRules").value),
      fixpoint: document.getElementById("inferFixpoint").value === "true",
      max_iterations: Number(document.getElementById("inferMaxIter").value),
      check_conflicts: document.getElementById("inferConflicts").value === "true"
    });
  }
  async function appendChanges() {
    const added = parseEdges(document.getElementById("addedEdges").value);
    const removed = parseEdges(document.getElementById("removedEdges").value);
    await api("POST", "/changes/append", {added_edges: added, removed_edges: removed});
  }
  async function consumeIncremental() {
    await api("POST", "/rules/mine/incremental/from-changelog", {
      body_length: Number(document.getElementById("incBodyLength").value),
      limit: Number(document.getElementById("incLimit").value),
      change_limit: Number(document.getElementById("incChangeLimit").value),
      min_support: Number(document.getElementById("incSupport").value),
      min_pca_confidence: Number(document.getElementById("incPca").value)
    });
  }
  let _rulesCache = [];

  async function loadRulesTable() {
    const status = document.getElementById("rulesStatusFilter").value;
    const limit = Number(document.getElementById("rulesLimit").value) || 100;
    let path = "/rules?limit=" + limit;
    if (status) path += "&status=" + encodeURIComponent(status);
    const resp = await fetch(getBase() + path);
    const body = await resp.json();
    const rules = body.rules || [];
    _rulesCache = rules;
    renderRulesTable(rules);
  }

  function renderRulesTable(rules) {
    const wrap = document.getElementById("rulesTableWrap");
    if (!rules.length) {
      wrap.innerHTML = '<p class="hint">No rules found.</p>';
      return;
    }
    let html = '<table id="rulesTable"><thead><tr>' +
      '<th>Rule ID</th><th>Body</th><th>Head</th>' +
      '<th>Support</th><th>PCA Conf</th><th>Head Cov</th>' +
      '<th>Status</th><th>Ver</th><th>Actions</th>' +
      '</tr></thead><tbody>';
    for (const r of rules) {
      const bodyStr = (r.body_relations || []).join(' &and; ');
      const badge = '<span class="badge badge-' + r.status + '">' + r.status + '</span>';
      const canAdopt = (r.status === 'discovered');
      const canReject = (r.status === 'discovered' || r.status === 'adopted');
      const rid = r.rule_id.replace(/'/g, "\\\\'");
      html += '<tr id="row-' + r.rule_id + '">' +
        '<td style="font-size:11px;word-break:break-all">' + r.rule_id + '</td>' +
        '<td>' + bodyStr + '</td>' +
        '<td>' + r.head_relation + '</td>' +
        '<td>' + r.support + '</td>' +
        '<td>' + (r.pca_confidence != null ? r.pca_confidence.toFixed(3) : '-') + '</td>' +
        '<td>' + (r.head_coverage != null ? r.head_coverage.toFixed(3) : '-') + '</td>' +
        '<td>' + badge + '</td>' +
        '<td>' + (r.version || 1) + '</td>' +
        '<td style="white-space:nowrap">' +
          '<button class="btn-sm btn-adopt" onclick="adoptRule(\\'' + rid + '\\')"' +
            (canAdopt ? '' : ' disabled') + '>Adopt</button> ' +
          '<button class="btn-sm btn-reject" onclick="rejectRule(\\'' + rid + '\\')"' +
            (canReject ? '' : ' disabled') + '>Reject</button>' +
        '</td></tr>';
    }
    html += '</tbody></table>';
    wrap.innerHTML = html;
  }

  async function adoptRule(ruleId) {
    const resp = await fetch(getBase() + "/rules/" + encodeURIComponent(ruleId) + "/adopt", {method: "POST"});
    const data = await resp.json();
    setOutput({status: resp.status, ok: resp.ok, path: "/rules/" + ruleId + "/adopt", data});
    await loadRulesTable();
  }

  async function rejectRule(ruleId) {
    const resp = await fetch(getBase() + "/rules/" + encodeURIComponent(ruleId) + "/reject", {method: "POST"});
    const data = await resp.json();
    setOutput({status: resp.status, ok: resp.ok, path: "/rules/" + ruleId + "/reject", data});
    await loadRulesTable();
  }

  async function adoptAll() {
    const toAdopt = _rulesCache.filter(r => r.status === "discovered");
    if (!toAdopt.length) { setOutput({message: "No discovered rules to adopt"}); return; }
    const results = [];
    for (const r of toAdopt) {
      const resp = await fetch(getBase() + "/rules/" + encodeURIComponent(r.rule_id) + "/adopt", {method: "POST"});
      results.push({rule_id: r.rule_id, status: resp.status});
    }
    setOutput({action: "adopt_all", results});
    await loadRulesTable();
  }

  document.getElementById("baseUrl").value = window.location.origin;
</script>
</body>
</html>
"""


def _trigger_manager(db: Neo4jClient, settings: Settings) -> TriggerManager:
    return TriggerManager(db, trigger_name=settings.changelog_trigger_name)


def ensure_neo4j_schema(db: Neo4jClient) -> None:
    """Best-effort schema bootstrap for indexes/constraints."""
    statements = [
        "CREATE CONSTRAINT rule_rule_id_unique IF NOT EXISTS FOR (r:Rule) REQUIRE r.rule_id IS UNIQUE",
        "CREATE CONSTRAINT conflict_rule_unique IF NOT EXISTS FOR (c:ConflictRule) REQUIRE (c.head_relation, c.conflict_relation) IS UNIQUE",
        "CREATE CONSTRAINT relation_type_name_unique IF NOT EXISTS FOR (r:RelationType) REQUIRE r.name IS UNIQUE",
        "CREATE CONSTRAINT rule_stat_rule_id_unique IF NOT EXISTS FOR (s:RuleStat) REQUIRE s.rule_id IS UNIQUE",
        "CREATE CONSTRAINT incremental_state_name_unique IF NOT EXISTS FOR (s:IncrementalState) REQUIRE s.name IS UNIQUE",
        "CREATE CONSTRAINT id_sequence_name_unique IF NOT EXISTS FOR (s:IdSequence) REQUIRE s.name IS UNIQUE",
        "CREATE CONSTRAINT changelog_change_seq_unique IF NOT EXISTS FOR (c:ChangeLog) REQUIRE c.change_seq IS UNIQUE",
        "CREATE CONSTRAINT changelog_dedup_key_unique IF NOT EXISTS FOR (c:ChangeLog) REQUIRE c.dedup_key IS UNIQUE",
        "CREATE INDEX changelog_event_type_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.event_type)",
        "CREATE INDEX changelog_rel_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.rel)",
        "CREATE INDEX changelog_source_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.source)",
        "CREATE INDEX changelog_batch_id_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.batch_id)",
        "CREATE INDEX changelog_idempotency_key_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.idempotency_key)",
        "CREATE INDEX rule_head_relation_idx IF NOT EXISTS FOR (r:Rule) ON (r.head_relation)",
        "CREATE INDEX rule_status_idx IF NOT EXISTS FOR (r:Rule) ON (r.status)",
        "CREATE INDEX conflictcase_rule_id_idx IF NOT EXISTS FOR (c:ConflictCase) ON (c.rule_id)",
    ]
    for statement in statements:
        db.run_write(statement)


def _append_changes_compat(
    store: IncrementalStore,
    *,
    added: list[ChangeEdge],
    removed: list[ChangeEdge],
    source: str = "app",
    batch_id: str | None = None,
    idempotency_key: str | None = None,
    context: dict[str, str] | None = None,
) -> None:
    """Support both new and legacy IncrementalStore append signatures."""
    try:
        store.append_changes(
            added_edges=added,
            removed_edges=removed,
            source=source,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            context=context,
        )
        return
    except TypeError:
        pass

    legacy_changes = [
        {"op": "add", "src": edge.src, "rel": edge.rel, "dst": edge.dst}
        for edge in added
    ] + [
        {"op": "remove", "src": edge.src, "rel": edge.rel, "dst": edge.dst}
        for edge in removed
    ]
    store.append_changes(legacy_changes)


def _trigger_manager_status(manager: TriggerManager) -> dict[str, object]:
    try:
        return manager.status()
    except Exception as exc:
        return {
            "enabled": False,
            "error": str(exc),
        }


def _legacy_pending_relations(store: IncrementalStore) -> tuple[list[str], int]:
    """Read pending relations from legacy changelog queue if available."""
    if not hasattr(store, "fetch_unprocessed_changes"):
        return [], 0
    try:
        pending = store.fetch_unprocessed_changes()
    except Exception:
        return [], 0
    if not pending:
        return [], 0

    relations: set[str] = set()
    for item in pending:
        rel = ""
        if isinstance(item, dict):
            rel = str(item.get("rel", "")).strip()
        else:
            rel = str(getattr(item, "rel", "")).strip()
        if rel:
            relations.add(rel)
    return sorted(relations), len(pending)


def get_db(settings: Settings = Depends(get_settings)) -> Neo4jClient:
    return Neo4jClient(settings)


@app.get("/health")
def health(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    return {"status": "ok", "db": settings.neo4j_uri}


@app.get("/console", response_class=HTMLResponse)
def web_console() -> str:
    return CONSOLE_HTML


@app.on_event("startup")
def on_startup() -> None:
    settings = get_settings()
    db = Neo4jClient(settings)
    try:
        ensure_neo4j_schema(db)
        manager = _trigger_manager(db, settings)
        if settings.changelog_trigger_auto_install and manager.ensure_config_enabled():
            manager.upsert_trigger()
    finally:
        db.close()


@app.post("/rules/mine", response_model=MineRulesResponse)
def mine_rules(
    payload: MineRulesRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> MineRulesResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    miner = RuleMiningService(repo)
    config = MiningConfig(
        min_support=payload.min_support if payload.min_support is not None else settings.min_support,
        min_pca_confidence=(
            payload.min_pca_confidence
            if payload.min_pca_confidence is not None
            else settings.min_confidence
        ),
        min_head_coverage=payload.min_head_coverage or 0.0,
        top_k=payload.limit,
        candidate_limit=payload.candidate_limit or max(payload.limit * 20, 100),
        factual_only=payload.factual_only,
        beam_width=payload.beam_width,
        head_budget_per_relation=payload.head_budget_per_relation,
        confidence_ub_weight=payload.confidence_ub_weight,
    )
    if payload.body_length == 3:
        discovered = miner.mine_length3_rules(config)
    else:
        discovered = miner.mine_length2_rules(config)
    store = RuleStore(db)
    store.upsert_rules(discovered)
    return MineRulesResponse(rules=discovered)


@app.post("/changes/append", response_model=ChangeLogPendingResponse)
def append_changes(
    payload: ChangeLogAppendRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> ChangeLogPendingResponse:
    _ = settings
    store = IncrementalStore(db)
    def _edge_from_item(item, default_rel: str = "") -> ChangeEdge:
        src = getattr(item, "src", None) or getattr(item, "src_id", None) or ""
        rel = getattr(item, "rel", None) or getattr(item, "relation", None) or default_rel
        dst = getattr(item, "dst", None) or getattr(item, "dst_id", None) or ""
        return ChangeEdge(src=str(src), rel=str(rel), dst=str(dst))

    added = [_edge_from_item(item) for item in payload.added_edges]
    removed = [_edge_from_item(item) for item in payload.removed_edges]
    _append_changes_compat(
        store,
        added=added,
        removed=removed,
        source="app",
        batch_id=payload.batch_id,
        idempotency_key=payload.idempotency_key,
        context=payload.context,
    )
    pending = store.pending_changes(limit=1000)
    return ChangeLogPendingResponse(
        pending_count=len(pending),
        entries=pending,
    )


@app.post("/changes/log")
def append_changes_legacy_log(
    payload: EdgeDeltaApplyRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> dict[str, int]:
    _ = settings
    store = IncrementalStore(db)
    added = [ChangeEdge(src=item.src_id, rel=item.rel, dst=item.dst_id, created_at=item.created_at) for item in payload.added_edges]
    removed = [
        ChangeEdge(src=item.src_id, rel=item.rel, dst=item.dst_id, created_at=item.created_at)
        for item in payload.removed_edges
    ]
    _append_changes_compat(store, added=added, removed=removed, source="app-legacy")
    return {"added": len(added), "removed": len(removed)}


@app.post("/changelog/append")
def append_changes_legacy_batch(
    payload: dict,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> dict[str, int]:
    _ = settings
    changes = payload.get("changes", []) if isinstance(payload, dict) else []
    added: list[ChangeEdge] = []
    removed: list[ChangeEdge] = []
    for item in changes:
        if not isinstance(item, dict):
            continue
        op = str(item.get("op", "add")).lower()
        edge = ChangeEdge(
            src=str(item.get("src", "")),
            rel=str(item.get("rel", "")),
            dst=str(item.get("dst", "")),
        )
        if op == "remove":
            removed.append(edge)
        else:
            added.append(edge)
    _append_changes_compat(IncrementalStore(db), added=added, removed=removed, source="app-legacy")
    return {"appended": len(added) + len(removed)}


@app.post("/triggers/changelog/install")
def install_changelog_trigger(
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> dict[str, str]:
    manager = _trigger_manager(db, settings)
    if not manager.ensure_config_enabled():
        raise HTTPException(status_code=412, detail="APOC trigger not available")
    installed = manager.upsert_trigger()
    if not installed:
        diagnostic = manager.diagnose_install()
        raise HTTPException(status_code=500, detail={"message": "failed to install APOC trigger", "diagnostic": diagnostic})
    listed = []
    found = False
    # APOC trigger registration can be eventually visible.
    for _ in range(10):
        listed = manager.list_triggers()
        if any(str(item.get("name", "")) == settings.changelog_trigger_name for item in listed):
            found = True
            break
        time.sleep(0.2)
    if not found:
        diagnostic = manager.diagnose_install()
        raise HTTPException(
            status_code=500,
            detail={
                "message": "trigger install reported success but was not listed",
                "diagnostic": diagnostic,
                "list_snapshot": listed,
            },
        )
    return {"status": "installed", "name": settings.changelog_trigger_name}


@app.delete("/triggers/changelog")
def drop_changelog_trigger(
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> dict[str, str]:
    _trigger_manager(db, settings).drop_trigger()
    return {"status": "dropped", "name": settings.changelog_trigger_name}


@app.post("/rules/mine/length3", response_model=MineRulesResponse)
def mine_rules_length3(
    payload: MineRulesRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> MineRulesResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    miner = RuleMiningService(repo)
    config = MiningConfig(
        min_support=payload.min_support if payload.min_support is not None else settings.min_support,
        min_pca_confidence=(
            payload.min_pca_confidence
            if payload.min_pca_confidence is not None
            else settings.min_confidence
        ),
        min_head_coverage=payload.min_head_coverage or 0.0,
        top_k=payload.limit,
        candidate_limit=payload.candidate_limit or max(payload.limit * 20, 100),
        factual_only=payload.factual_only,
        beam_width=payload.beam_width,
        head_budget_per_relation=payload.head_budget_per_relation,
        confidence_ub_weight=payload.confidence_ub_weight,
    )
    discovered = miner.mine_length3_rules(config)
    RuleStore(db).upsert_rules(discovered)
    return MineRulesResponse(rules=discovered)


@app.post("/rules/mine/incremental/length2", response_model=MineRulesResponse)
def mine_rules_incremental_length2(
    payload: IncrementalMineRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> MineRulesResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    store = RuleStore(db)
    base_miner = RuleMiningService(repo)
    inc_store = IncrementalStore(db)
    inc_miner = IncrementalMiningService(base_miner, store, inc_store)
    result = inc_miner.run_incremental(
        request=MineRulesRequest(
            limit=payload.limit,
            min_support=payload.min_support,
            min_pca_confidence=payload.min_pca_confidence,
            min_head_coverage=payload.min_head_coverage,
            candidate_limit=payload.candidate_limit,
            factual_only=payload.factual_only,
            beam_width=payload.beam_width,
            head_budget_per_relation=payload.head_budget_per_relation,
            confidence_ub_weight=payload.confidence_ub_weight,
            body_length=2,
            changed_relations=payload.affected_relations,
        ),
        body_length=2,
    )
    return MineRulesResponse(rules=result.rules)


@app.post("/rules/mine/incremental/length3", response_model=MineRulesResponse)
def mine_rules_incremental_length3(
    payload: IncrementalMineRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> MineRulesResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    store = RuleStore(db)
    base_miner = RuleMiningService(repo)
    inc_store = IncrementalStore(db)
    inc_miner = IncrementalMiningService(base_miner, store, inc_store)
    result = inc_miner.run_incremental(
        request=MineRulesRequest(
            limit=payload.limit,
            min_support=payload.min_support,
            min_pca_confidence=payload.min_pca_confidence,
            min_head_coverage=payload.min_head_coverage,
            candidate_limit=payload.candidate_limit,
            factual_only=payload.factual_only,
            beam_width=payload.beam_width,
            head_budget_per_relation=payload.head_budget_per_relation,
            confidence_ub_weight=payload.confidence_ub_weight,
            body_length=3,
            changed_relations=payload.affected_relations,
        ),
        body_length=3,
    )
    return MineRulesResponse(rules=result.rules)


@app.post("/rules/mine/incremental/from-changelog", response_model=IncrementalConsumeResponse)
def mine_rules_incremental_from_changelog(
    payload: IncrementalConsumeRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> IncrementalConsumeResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    store = RuleStore(db)
    base_miner = RuleMiningService(repo)
    inc_store = IncrementalStore(db)
    inc_miner = IncrementalMiningService(base_miner, store, inc_store)

    request = MineRulesRequest(
        limit=payload.limit,
        min_support=payload.min_support if payload.min_support is not None else settings.min_support,
        min_pca_confidence=(
            payload.min_pca_confidence
            if payload.min_pca_confidence is not None
            else settings.min_confidence
        ),
        min_head_coverage=payload.min_head_coverage or 0.0,
        candidate_limit=payload.candidate_limit,
        factual_only=payload.factual_only,
        beam_width=payload.beam_width,
        head_budget_per_relation=payload.head_budget_per_relation,
        confidence_ub_weight=payload.confidence_ub_weight,
        body_length=payload.body_length,
        changed_relations=payload.changed_relations,
    )
    result = inc_miner.run_incremental(request=request, body_length=payload.body_length, change_limit=payload.change_limit)
    return IncrementalConsumeResponse(
        processed_changes=result.processed_events,
        affected_relations=result.affected_relations,
        rules=result.rules,
    )


@app.post("/rules/mine/incremental/changelog")
def mine_rules_incremental_changelog_legacy(
    payload: IncrementalConsumeRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> dict:
    result = mine_rules_incremental_from_changelog(payload=payload, settings=settings, db=db)
    return {
        "processed_changes": result.processed_changes,
        "affected_relations": result.affected_relations,
        "rules": [rule.model_dump() for rule in result.rules],
    }


@app.post("/rules/mine/incremental/consume/length2")
def mine_rules_incremental_consume_length2_legacy(
    payload: IncrementalMineRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> dict:
    # Legacy endpoint: consume queued changelog directly.
    store = IncrementalStore(db)
    legacy_relations, legacy_pending_count = _legacy_pending_relations(store)
    changed_relations = payload.changed_relations or payload.affected_relations or legacy_relations
    consume_payload = IncrementalConsumeRequest(
        limit=payload.limit,
        min_support=payload.min_support,
        min_pca_confidence=payload.min_pca_confidence,
        min_head_coverage=payload.min_head_coverage,
        candidate_limit=payload.candidate_limit,
        factual_only=payload.factual_only,
        beam_width=payload.beam_width,
        head_budget_per_relation=payload.head_budget_per_relation,
        confidence_ub_weight=payload.confidence_ub_weight,
        body_length=2,
        changed_relations=changed_relations,
        change_limit=payload.change_limit,
    )
    result = mine_rules_incremental_from_changelog(payload=consume_payload, settings=settings, db=db)
    if legacy_pending_count > 0 and hasattr(store, "mark_changes_processed"):
        try:
            store.mark_changes_processed(legacy_pending_count)
        except Exception:
            pass
    return {"rules": [rule.model_dump() for rule in result.rules]}


@app.get("/rules", response_model=RuleSetResponse)
def list_rules(
    status: str | None = None,
    limit: int = 100,
    db: Neo4jClient = Depends(get_db),
) -> RuleSetResponse:
    rules = RuleStore(db).list_rules(status=status, limit=limit)
    return RuleSetResponse(rules=rules)


@app.post("/rules/{rule_id}/adopt")
def adopt_rule(rule_id: str, db: Neo4jClient = Depends(get_db)) -> dict[str, str]:
    try:
        new_status = RuleStore(db).transition_rule_status(rule_id, "adopted")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found")
    except InvalidStatusTransition as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "rule_id": exc.rule_id,
                "current_status": exc.current,
                "target_status": exc.target,
            },
        )
    return {"status": new_status, "rule_id": rule_id}


@app.post("/rules/{rule_id}/reject")
def reject_rule(rule_id: str, db: Neo4jClient = Depends(get_db)) -> dict[str, str]:
    try:
        new_status = RuleStore(db).transition_rule_status(rule_id, "rejected")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found")
    except InvalidStatusTransition as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "rule_id": exc.rule_id,
                "current_status": exc.current,
                "target_status": exc.target,
            },
        )
    return {"status": new_status, "rule_id": rule_id}


@app.get("/conflicts", response_model=ConflictPairsResponse)
def list_conflict_pairs(db: Neo4jClient = Depends(get_db)) -> ConflictPairsResponse:
    store = ConflictStore(db)
    pairs = {k: sorted(v) for k, v in store.list_pairs().items()}
    return ConflictPairsResponse(pairs=pairs)


@app.put("/conflicts", response_model=ConflictPairsResponse)
def replace_conflict_pairs(
    payload: ConflictPairsUpdateRequest,
    db: Neo4jClient = Depends(get_db),
) -> ConflictPairsResponse:
    store = ConflictStore(db)
    store.replace_pairs(payload.pairs)
    pairs = {k: sorted(v) for k, v in store.list_pairs().items()}
    return ConflictPairsResponse(pairs=pairs)


@app.post("/conflicts", response_model=ConflictPairsResponse)
def add_conflict_pair(
    payload: ConflictPairsUpdateRequest,
    db: Neo4jClient = Depends(get_db),
) -> ConflictPairsResponse:
    store = ConflictStore(db)
    for inferred_rel, conflict_list in payload.pairs.items():
        for conflict_rel in conflict_list:
            store.upsert_pair(inferred_rel, conflict_rel)
    pairs = {k: sorted(v) for k, v in store.list_pairs().items()}
    return ConflictPairsResponse(pairs=pairs)


@app.delete("/conflicts/{inferred_relation}/{conflicting_relation}")
def delete_conflict_pair(
    inferred_relation: str,
    conflicting_relation: str,
    db: Neo4jClient = Depends(get_db),
) -> dict[str, str]:
    deleted = ConflictStore(db).delete_pair(inferred_relation, conflicting_relation)
    if not deleted:
        raise HTTPException(status_code=404, detail="conflict pair not found")
    return {
        "status": "deleted",
        "inferred_relation": inferred_relation,
        "conflicting_relation": conflicting_relation,
    }


@app.get("/conflicts/cases", response_model=ConflictCaseListResponse)
@app.get("/conflict-cases", response_model=ConflictCaseListResponse, include_in_schema=False)
def list_conflict_cases(limit: int = 100, db: Neo4jClient = Depends(get_db)) -> ConflictCaseListResponse:
    store = ConflictStore(db)
    return ConflictCaseListResponse(cases=store.list_conflict_cases(limit=limit))


@app.post("/inference/run", response_model=InferenceResponse)
def run_inference(
    payload: InferenceRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> InferenceResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    store = RuleStore(db)
    conflict_store = ConflictStore(db)
    # 优先读取数据库配置；若未配置则回退到环境变量，保证向后兼容。
    conflict_pairs = conflict_store.list_pairs()
    if not conflict_pairs:
        conflict_pairs = parse_conflict_relation_pairs(settings.conflict_relation_pairs)
    if payload.conflict_pairs:
        conflict_pairs = {
            inferred_rel: {item for item in conflict_list}
            for inferred_rel, conflict_list in payload.conflict_pairs.items()
        }
    engine = InferenceEngine(
        repo,
        store,
        conflict_store=conflict_store,
        conflict_pairs=conflict_pairs,
    )

    if payload.fixpoint:
        summary = engine.run_fixpoint(
            max_iterations=payload.max_iterations,
            limit_rules=payload.limit_rules,
            check_conflicts=payload.check_conflicts,
        )
        results = summary.results
        iterations_run = max((r.iteration for r in results), default=0)
    else:
        summary = engine.run_once(
            limit_rules=payload.limit_rules,
            check_conflicts=payload.check_conflicts,
        )
        results = summary.results
        iterations_run = 1 if results else 0

    return InferenceResponse(
        results=results,
        total_created=sum(item.created_triples for item in results),
        iterations_run=iterations_run,
        total_conflicts=summary.conflicts_detected,
    )
