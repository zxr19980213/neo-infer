from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException

from neo_infer.config import Settings, get_settings, parse_conflict_relation_pairs
from neo_infer.conflict_management import ConflictStore
from neo_infer.db import Neo4jClient
from neo_infer.incremental_mining import IncrementalMiner
from neo_infer.incremental_store import IncrementalStore
from neo_infer.inference import InferenceEngine
from neo_infer.models import (
    EdgeDelta,
    EdgeDeltaApplyRequest,
    EdgeDeltaApplyResponse,
    EdgeDeltaBatch,
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
    if payload.body_length == 3:
        discovered = miner.mine_length3_rules(config)
    else:
        discovered = miner.mine_length2_rules(config)
    store = RuleStore(db)
    store.upsert_rules(discovered)
    return MineRulesResponse(rules=discovered)


@app.post("/graph/edges/delta", response_model=EdgeDeltaApplyResponse)
def apply_edge_deltas(
    payload: EdgeDeltaApplyRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> EdgeDeltaApplyResponse:
    store = IncrementalStore(db)
    batch = EdgeDeltaBatch(
        added_edges=[EdgeDelta(**item.model_dump()) for item in payload.added_edges],
        removed_edges=[EdgeDelta(**item.model_dump()) for item in payload.removed_edges],
        cursor=payload.cursor,
    )
    batch_id, added_count, removed_count = store.append_changelog(batch)

    # 同步写图：新增用 MERGE，删除用 MATCH DELETE。该接口可作为增量入口。
    if payload.added_edges:
        db.run_write(
            """
            UNWIND $edges AS edge
            MATCH (s) WHERE elementId(s) = edge.src_id
            MATCH (t) WHERE elementId(t) = edge.dst_id
            CALL {
              WITH s, t, edge
              WITH s, t, edge, replace(edge.rel, "`", "") AS rel
              CALL apoc.create.relationship(s, rel, {}, t) YIELD rel AS created_rel
              RETURN created_rel
            }
            RETURN count(*) AS updated
            """,
            {
                "edges": [
                    {"src_id": e.src_id, "dst_id": e.dst_id, "rel": e.rel}
                    for e in payload.added_edges
                ]
            },
        )
    if payload.removed_edges:
        # 不使用动态关系删除，退化为按两端节点和关系类型匹配。
        for edge in payload.removed_edges:
            rel = edge.rel.replace("`", "")
            db.run_write(
                f"""
                MATCH (s)-[r:`{rel}`]->(t)
                WHERE elementId(s) = $src_id AND elementId(t) = $dst_id
                DELETE r
                """,
                {"src_id": edge.src_id, "dst_id": edge.dst_id},
            )

    return EdgeDeltaApplyResponse(
        batch_id=batch_id,
        added_count=added_count,
        removed_count=removed_count,
    )


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
    store = RuleStore(db)
    inc_store = IncrementalStore(db)
    miner = IncrementalMiner(repo=repo, rule_store=store, inc_store=inc_store)
    discovered = miner.run_incremental_length2(
        min_support=payload.min_support if payload.min_support is not None else settings.min_support,
        min_pca_confidence=(
            payload.min_pca_confidence if payload.min_pca_confidence is not None else settings.min_confidence
        ),
        min_head_coverage=payload.min_head_coverage or 0.0,
        top_k=payload.limit,
    )
    return MineRulesResponse(rules=discovered)


@app.post("/rules/mine/incremental/length3", response_model=MineRulesResponse)
def mine_rules_incremental_length3(
    payload: IncrementalMineRequest,
    settings: Settings = Depends(get_settings),
    db: Neo4jClient = Depends(get_db),
) -> MineRulesResponse:
    repo = QueryRepository(db.driver, database=settings.neo4j_database)
    store = RuleStore(db)
    inc_store = IncrementalStore(db)
    miner = IncrementalMiner(repo=repo, rule_store=store, inc_store=inc_store)
    discovered = miner.run_incremental_length3(
        min_support=payload.min_support if payload.min_support is not None else settings.min_support,
        min_pca_confidence=(
            payload.min_pca_confidence if payload.min_pca_confidence is not None else settings.min_confidence
        ),
        min_head_coverage=payload.min_head_coverage or 0.0,
        top_k=payload.limit,
        fanout_cap=payload.fanout_cap or 1000,
    )
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
