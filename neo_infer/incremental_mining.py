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

    @staticmethod
    def _rebuild_rule_with_metrics(rule: Rule, metrics: dict[str, int]) -> Rule:
        support = int(metrics.get("support", 0))
        pca_denominator = int(metrics.get("pca_denominator", 0))
        head_count = int(metrics.get("head_count", 0))
        pca_confidence = float(support) / float(pca_denominator) if pca_denominator > 0 else 0.0
        head_coverage = float(support) / float(head_count) if head_count > 0 else 0.0
        return rule.model_copy(
            update={
                "support": support,
                "pca_confidence": pca_confidence,
                "head_coverage": head_coverage,
            }
        )

    def _rebuild_rule_with_delta(self, rule: Rule, metrics: dict[str, int]) -> Rule:
        """Update support/pca/head by applying delta on stored stats in-place."""
        stat = self.incremental_store.get_rule_stat(rule.rule_id)
        if stat is None:
            return self._rebuild_rule_with_metrics(rule, metrics)

        new_support = int(metrics.get("support", 0))
        new_pca_denom = int(metrics.get("pca_denominator", 0))
        new_head_count = int(metrics.get("head_count", 0))

        # In-place delta update: old + (new-old), keeps one incremental update path.
        support = max(0, int(stat.support) + (new_support - int(stat.support)))
        pca_denom = max(0, int(stat.pca_denominator) + (new_pca_denom - int(stat.pca_denominator)))
        head_count = max(0, int(stat.head_count) + (new_head_count - int(stat.head_count)))

        pca_confidence = float(support) / float(pca_denom) if pca_denom > 0 else 0.0
        head_coverage = float(support) / float(head_count) if head_count > 0 else 0.0
        return rule.model_copy(
            update={
                "support": support,
                "pca_confidence": pca_confidence,
                "head_coverage": head_coverage,
            }
        )

    def _update_existing_rules_by_delta(
        self,
        *,
        affected_relations: list[str],
        body_length: int,
    ) -> list[Rule]:
        touched = {item for item in affected_relations if item}
        if not touched:
            return []
        affected_ids = self.incremental_store.affected_rule_ids(touched)
        existing_rules = self.rule_store.list_rules_by_ids(affected_ids)
        if not existing_rules:
            return []

        repo = self.miner._repository
        updated_rules: list[Rule] = []
        for rule in existing_rules:
            if len(rule.body_relations) != body_length:
                continue
            if body_length == 2:
                metrics = repo.compute_length2_rule_metrics(
                    rule.body_relations[0],
                    rule.body_relations[1],
                    rule.head_relation,
                )
            else:
                metrics = repo.compute_length3_rule_metrics(
                    rule.body_relations[0],
                    rule.body_relations[1],
                    rule.body_relations[2],
                    rule.head_relation,
                )
            updated_rules.append(self._rebuild_rule_with_delta(rule, metrics))
        return updated_rules

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
        if not affected_relations and request.changed_relations:
            affected_relations = sorted({item for item in request.changed_relations if item})

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
            beam_width=request.beam_width,
            head_budget_per_relation=request.head_budget_per_relation,
            confidence_ub_weight=request.confidence_ub_weight,
            body_length=body_length,
            changed_relations=affected_relations,
        )

        updated_existing = self._update_existing_rules_by_delta(
            affected_relations=affected_relations,
            body_length=body_length,
        )
        discovered = self.miner.mine_rules(config)
        merged: dict[str, Rule] = {rule.rule_id: rule for rule in updated_existing}
        for rule in discovered:
            merged[rule.rule_id] = rule
        upserts = list(merged.values())

        # Upsert rules and maintain relation->rules index.
        self.rule_store.upsert_rules(upserts)
        self.incremental_store.update_rule_indexes(upserts)
        self.incremental_store.update_rule_stats(upserts)
        self.incremental_store.mark_consumed(delta.cursor)

        return IncrementalRunResult(
            rules=upserts,
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
