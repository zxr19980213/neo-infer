from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from neo_infer.config import Settings, get_settings, parse_conflict_relation_pairs
from neo_infer.conflict_management import ConflictStore
from neo_infer.db import Neo4jClient
from neo_infer.inference import InferenceEngine
from neo_infer.models import (
    ConflictCasesResponse,
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


def get_db(settings: Settings = Depends(get_settings)) -> Neo4jClient:
    return Neo4jClient(settings)


@app.get("/health")
def health(settings: Settings = Depends(get_settings)) -> dict[str, str]:
    return {"status": "ok", "db": settings.neo4j_uri}


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
    )
    if payload.max_body_length == 3:
        discovered = miner.mine_length3_rules(config)
    else:
        discovered = miner.mine_length2_rules(config)
    store = RuleStore(db)
    store.upsert_rules(discovered)
    return MineRulesResponse(rules=discovered)


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
    )
    discovered = miner.mine_length2_rules_incremental(config, payload.affected_relations)
    RuleStore(db).upsert_rules(discovered)
    return MineRulesResponse(rules=discovered)


@app.post("/rules/mine/incremental/length3", response_model=MineRulesResponse)
def mine_rules_incremental_length3(
    payload: IncrementalMineRequest,
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
    )
    discovered = miner.mine_length3_rules_incremental(config, payload.affected_relations)
    RuleStore(db).upsert_rules(discovered)
    return MineRulesResponse(rules=discovered)


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


@app.get("/conflicts/cases", response_model=ConflictCasesResponse)
def list_conflict_cases(limit: int = 100, db: Neo4jClient = Depends(get_db)) -> ConflictCasesResponse:
    store = ConflictStore(db)
    return ConflictCasesResponse(cases=store.list_conflict_cases(limit=limit))


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
    engine = InferenceEngine(repo, store, conflict_pairs=conflict_pairs)

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
