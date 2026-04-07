from __future__ import annotations

from dataclasses import dataclass

from neo_infer.incremental_store import IncrementalStore
from neo_infer.models import DeltaBatch, MineRulesRequest, Rule
from neo_infer.rule_management import RuleStore
from neo_infer.rule_mining import MiningConfig, RuleMiningService


@dataclass(slots=True)
class IncrementalRunResult:
    rules: list[Rule]
    processed_events: int
    last_event_id: int
    affected_relations: list[str]


class IncrementalMiningService:
    """True incremental mining driver with changelog cursor."""

    def __init__(
        self,
        miner: RuleMiningService,
        rule_store: RuleStore,
        incremental_store: IncrementalStore,
    ) -> None:
        self.miner = miner
        self.rule_store = rule_store
        self.incremental_store = incremental_store

    def run_incremental(
        self,
        request: MineRulesRequest,
        body_length: int,
        change_limit: int = 2000,
    ) -> IncrementalRunResult:
        delta = self.incremental_store.consume_delta(limit=change_limit)
        events = [*delta.added_edges, *delta.removed_edges]
        affected_relations = sorted(
            {
                item.rel
                for item in events
                if item.rel
            }
        )

        # No change since last cursor: return empty result quickly.
        if not affected_relations:
            return IncrementalRunResult(
                rules=[],
                processed_events=0,
                last_event_id=delta.cursor,
                affected_relations=[],
            )

        config = MiningConfig(
            min_support=request.min_support if request.min_support is not None else 0,
            min_pca_confidence=request.min_pca_confidence if request.min_pca_confidence is not None else 0.0,
            min_head_coverage=request.min_head_coverage or 0.0,
            top_k=request.limit,
            candidate_limit=request.candidate_limit or max(request.limit * 20, 100),
            body_length=body_length,
            changed_relations=affected_relations,
        )

        discovered = self.miner.mine_rules(config)

        # Upsert rules and maintain relation->rules index.
        self.rule_store.upsert_rules(discovered)
        self.incremental_store.update_rule_indexes(discovered)
        self.incremental_store.update_rule_stats(discovered)
        self.incremental_store.mark_consumed(delta.cursor)

        return IncrementalRunResult(
            rules=discovered,
            processed_events=len(events),
            last_event_id=delta.cursor,
            affected_relations=affected_relations,
        )


class IncrementalMiner:
    """Compatibility wrapper used by API incremental endpoints."""

    def __init__(self, repo, rule_store: RuleStore, inc_store: IncrementalStore) -> None:
        self._repo = repo
        self._rule_store = rule_store
        self._inc_store = inc_store

    def _run(
        self,
        body_length: int,
        *,
        min_support: int,
        min_pca_confidence: float,
        min_head_coverage: float,
        top_k: int,
        candidate_limit: int = 5000,
        changed_relations: list[str] | None = None,
    ) -> list[Rule]:
        miner = RuleMiningService(self._repo)
        service = IncrementalMiningService(
            miner=miner,
            rule_store=self._rule_store,
            incremental_store=self._inc_store,
        )
        req = MineRulesRequest(
            limit=top_k,
            min_support=min_support,
            min_pca_confidence=min_pca_confidence,
            min_head_coverage=min_head_coverage,
            candidate_limit=candidate_limit,
            body_length=body_length,
            changed_relations=changed_relations,
        )
        return service.run_incremental(req, body_length=body_length).rules

    def run_incremental_length2(
        self,
        *,
        min_support: int,
        min_pca_confidence: float,
        min_head_coverage: float,
        top_k: int,
        candidate_limit: int = 5000,
    ) -> list[Rule]:
        return self._run(
            2,
            min_support=min_support,
            min_pca_confidence=min_pca_confidence,
            min_head_coverage=min_head_coverage,
            top_k=top_k,
            candidate_limit=candidate_limit,
        )

    def run_incremental_length3(
        self,
        *,
        min_support: int,
        min_pca_confidence: float,
        min_head_coverage: float,
        top_k: int,
        candidate_limit: int = 5000,
        fanout_cap: int | None = None,
    ) -> list[Rule]:
        _ = fanout_cap
        return self._run(
            3,
            min_support=min_support,
            min_pca_confidence=min_pca_confidence,
            min_head_coverage=min_head_coverage,
            top_k=top_k,
            candidate_limit=candidate_limit,
        )
