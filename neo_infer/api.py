from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from neo_infer.config import Settings, get_settings, parse_conflict_relation_pairs
from neo_infer.conflict_management import ConflictStore
from neo_infer.db import Neo4jClient
from neo_infer.incremental_mining import IncrementalMiningService
from neo_infer.incremental_store import IncrementalStore
from neo_infer.inference import InferenceEngine
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
from neo_infer.rule_management import RuleStore
from neo_infer.rule_mining import MiningConfig, RuleMiningService

app = FastAPI(title="neo-infer", version="0.1.0")


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
        "CREATE INDEX changelog_event_type_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.event_type)",
        "CREATE INDEX changelog_rel_idx IF NOT EXISTS FOR (c:ChangeLog) ON (c.rel)",
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
) -> None:
    """Support both new and legacy IncrementalStore append signatures."""
    try:
        store.append_changes(added_edges=added, removed_edges=removed)
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


@app.on_event("startup")
def on_startup() -> None:
    settings = get_settings()
    db = Neo4jClient(settings)
    try:
        ensure_neo4j_schema(db)
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
    _append_changes_compat(store, added=added, removed=removed)
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
    _append_changes_compat(store, added=added, removed=removed)
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
    _append_changes_compat(IncrementalStore(db), added=added, removed=removed)
    return {"appended": len(added) + len(removed)}


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
    updated = RuleStore(db).update_rule_status(rule_id=rule_id, status="adopted")
    if not updated:
        return {"status": "not_found", "rule_id": rule_id}
    return {"status": "adopted", "rule_id": rule_id}


@app.post("/rules/{rule_id}/reject")
def reject_rule(rule_id: str, db: Neo4jClient = Depends(get_db)) -> dict[str, str]:
    updated = RuleStore(db).update_rule_status(rule_id=rule_id, status="rejected")
    if not updated:
        return {"status": "not_found", "rule_id": rule_id}
    return {"status": "rejected", "rule_id": rule_id}


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
