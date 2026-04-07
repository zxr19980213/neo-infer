from __future__ import annotations

from fastapi import Depends, FastAPI

from neo_infer.config import Settings, get_settings
from neo_infer.db import Neo4jClient
from neo_infer.inference import InferenceEngine
from neo_infer.models import (
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
    discovered = miner.mine_length2_rules(
        MiningConfig(
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
    )
    store = RuleStore(db)
    store.upsert_rules(discovered)
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


@app.post("/inference/run", response_model=InferenceResponse)
def run_inference(
    payload: InferenceRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> InferenceResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    store = RuleStore(db)
    engine = InferenceEngine(repo, store, conflict_relation=settings.conflict_relation)

    if payload.fixpoint:
        results = engine.run_fixpoint(max_iterations=payload.max_iterations, limit_rules=payload.limit_rules)
        iterations_run = max((r.iteration for r in results), default=0)
    else:
        results = engine.run_once(limit_rules=payload.limit_rules)
        iterations_run = 1 if results else 0

    return InferenceResponse(
        results=results,
        total_created=sum(item.created_triples for item in results),
        iterations_run=iterations_run,
    )
