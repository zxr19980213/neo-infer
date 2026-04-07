from __future__ import annotations

from dataclasses import dataclass

from neo_infer.incremental_store import IncrementalStore
from neo_infer.models import MineRulesRequest, Rule
from neo_infer.rule_management import RuleStore
from neo_infer.rule_mining import MiningConfig, RuleMiningService


@dataclass(slots=True)
class IncrementalRunResult:
    rules: list[Rule]
    processed_events: int
    last_event_id: int


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
    ) -> IncrementalRunResult:
        events, last_event_id = self.incremental_store.consume_changes()
        affected_relations = sorted(
            {
                item.relation
                for item in events
                if item.operation in {"add", "remove"} and item.relation
            }
        )

        # No change since last cursor: return empty result quickly.
        if not affected_relations:
            return IncrementalRunResult(rules=[], processed_events=0, last_event_id=last_event_id)

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
        self.incremental_store.mark_consumed(last_event_id)

        return IncrementalRunResult(
            rules=discovered,
            processed_events=len(events),
            last_event_id=last_event_id,
        )
