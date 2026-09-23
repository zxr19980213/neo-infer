from __future__ import annotations

from dataclasses import dataclass

from neo_infer.incremental_counters import Edge, length2_stat_delta, length3_stat_delta
from neo_infer.incremental_store import IncrementalStore
from neo_infer.models import ChangeEdge, MineRulesRequest, Rule
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
        self._pending_exact_stats: dict[str, tuple[int, int, int]] = {}

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

    def _full_metrics(self, rule: Rule, *, factual_only: bool) -> dict[str, int]:
        repo = self.miner._repository
        if hasattr(repo, "compute_rule_metrics"):
            return self._call_metrics(
                repo.compute_rule_metrics,
                rule.body_relations,
                rule.head_relation,
                factual_only=factual_only,
            )
        if len(rule.body_relations) == 2:
            return self._call_metrics(
                repo.compute_length2_rule_metrics,
                rule.body_relations[0],
                rule.body_relations[1],
                rule.head_relation,
                factual_only=factual_only,
            )
        return self._call_metrics(
            repo.compute_length3_rule_metrics,
            rule.body_relations[0],
            rule.body_relations[1],
            rule.body_relations[2],
            rule.head_relation,
            factual_only=factual_only,
        )

    @staticmethod
    def _call_metrics(method, *args, factual_only: bool) -> dict[str, int]:
        try:
            return method(*args, factual_only=factual_only)
        except TypeError:
            return method(*args)

    def _length2_event_metrics(
        self,
        rule: Rule,
        *,
        added_edges: list[ChangeEdge],
        removed_edges: list[ChangeEdge],
        factual_only: bool,
    ) -> dict[str, int] | None:
        """Apply a neighborhood delta when a stored baseline and local edges exist."""
        if len(rule.body_relations) != 2:
            return None
        repo = self.miner._repository
        if not hasattr(repo, "length2_neighborhood_edges"):
            return None
        stat = self.incremental_store.get_rule_stat(rule.rule_id)
        if stat is None:
            return None

        r1, r2 = rule.body_relations
        head = rule.head_relation
        relevant = {r1, r2, head}

        def _counts(edge: ChangeEdge) -> bool:
            if edge.rel not in relevant:
                return False
            if factual_only and edge.is_inferred:
                return False
            return True

        added = [edge for edge in added_edges if _counts(edge)]
        removed = [edge for edge in removed_edges if _counts(edge)]
        if not added and not removed:
            return {
                "support": int(stat.support),
                "pca_denominator": int(stat.pca_denominator),
                "head_count": int(stat.head_count),
            }

        node_keys = [item for edge in (*added, *removed) for item in (edge.src, edge.dst)]
        present_rows = repo.length2_neighborhood_edges(
            r1=r1,
            r2=r2,
            head_rel=head,
            node_keys=node_keys,
            factual_only=factual_only,
        )
        present: set[Edge] = {(str(src), str(rel), str(dst)) for src, rel, dst in present_rows}
        delta_support, delta_pca, delta_head = length2_stat_delta(
            r1=r1,
            r2=r2,
            head=head,
            present=present,
            added=[(edge.src, edge.rel, edge.dst) for edge in added],
            removed=[(edge.src, edge.rel, edge.dst) for edge in removed],
        )
        return {
            "support": max(0, int(stat.support) + delta_support),
            "pca_denominator": max(0, int(stat.pca_denominator) + delta_pca),
            "head_count": max(0, int(stat.head_count) + delta_head),
        }

    def _length3_event_metrics(
        self,
        rule: Rule,
        *,
        added_edges: list[ChangeEdge],
        removed_edges: list[ChangeEdge],
        factual_only: bool,
    ) -> dict[str, int] | None:
        """Apply a neighborhood delta when a stored length-3 baseline exists."""
        if len(rule.body_relations) != 3:
            return None
        repo = self.miner._repository
        if not hasattr(repo, "length3_neighborhood_edges"):
            return None
        stat = self.incremental_store.get_rule_stat(rule.rule_id)
        if stat is None:
            return None

        r1, r2, r3 = rule.body_relations
        head = rule.head_relation
        relevant = {r1, r2, r3, head}

        def _counts(edge: ChangeEdge) -> bool:
            if edge.rel not in relevant:
                return False
            if factual_only and edge.is_inferred:
                return False
            return True

        added = [edge for edge in added_edges if _counts(edge)]
        removed = [edge for edge in removed_edges if _counts(edge)]
        if not added and not removed:
            return {
                "support": int(stat.support),
                "pca_denominator": int(stat.pca_denominator),
                "head_count": int(stat.head_count),
            }

        node_keys = [item for edge in (*added, *removed) for item in (edge.src, edge.dst)]
        present_rows = repo.length3_neighborhood_edges(
            r1=r1,
            r2=r2,
            r3=r3,
            head_rel=head,
            node_keys=node_keys,
            factual_only=factual_only,
        )
        present: set[Edge] = {(str(src), str(rel), str(dst)) for src, rel, dst in present_rows}
        delta_support, delta_pca, delta_head = length3_stat_delta(
            r1=r1,
            r2=r2,
            r3=r3,
            head=head,
            present=present,
            added=[(edge.src, edge.rel, edge.dst) for edge in added],
            removed=[(edge.src, edge.rel, edge.dst) for edge in removed],
        )
        return {
            "support": max(0, int(stat.support) + delta_support),
            "pca_denominator": max(0, int(stat.pca_denominator) + delta_pca),
            "head_count": max(0, int(stat.head_count) + delta_head),
        }

    def _update_existing_rules_by_delta(
        self,
        *,
        affected_relations: list[str],
        body_length: int,
        added_edges: list[ChangeEdge],
        removed_edges: list[ChangeEdge],
        factual_only: bool,
    ) -> list[Rule]:
        touched = {item for item in affected_relations if item}
        if not touched:
            return []
        affected_ids = self.incremental_store.affected_rule_ids(touched)
        existing_rules = self.rule_store.list_rules_by_ids(affected_ids)
        if not existing_rules:
            return []

        updated_rules: list[Rule] = []
        exact_stats: dict[str, tuple[int, int, int]] = {}
        for rule in existing_rules:
            if len(rule.body_relations) != body_length:
                continue
            metrics = None
            if added_edges or removed_edges:
                if body_length == 2:
                    metrics = self._length2_event_metrics(
                        rule,
                        added_edges=added_edges,
                        removed_edges=removed_edges,
                        factual_only=factual_only,
                    )
                elif body_length == 3:
                    metrics = self._length3_event_metrics(
                        rule,
                        added_edges=added_edges,
                        removed_edges=removed_edges,
                        factual_only=factual_only,
                    )
            if metrics is None:
                metrics = self._full_metrics(rule, factual_only=factual_only)
            updated = self._rebuild_rule_with_metrics(rule, metrics)
            updated_rules.append(updated)
            exact_stats[updated.rule_id] = (
                int(metrics.get("support", 0)),
                int(metrics.get("pca_denominator", 0)),
                int(metrics.get("head_count", 0)),
            )
        self._pending_exact_stats = exact_stats
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
            factual_only=request.factual_only,
            beam_width=request.beam_width,
            head_budget_per_relation=request.head_budget_per_relation,
            confidence_ub_weight=request.confidence_ub_weight,
            body_length=body_length,
            changed_relations=affected_relations,
        )

        self._pending_exact_stats = {}
        updated_existing = self._update_existing_rules_by_delta(
            affected_relations=affected_relations,
            body_length=body_length,
            added_edges=list(delta.added_edges),
            removed_edges=list(delta.removed_edges),
            factual_only=bool(request.factual_only),
        )
        discovered = self.miner.mine_rules(config)
        # Event-updated counters stay authoritative for rules already stored.
        # Mining still contributes rules the delta pass did not touch.
        merged: dict[str, Rule] = {rule.rule_id: rule for rule in discovered}
        for rule in updated_existing:
            merged[rule.rule_id] = rule
        upserts = list(merged.values())

        # Upsert rules and maintain relation->rules index.
        self.rule_store.upsert_rules(upserts)
        self.incremental_store.update_rule_indexes(upserts)
        self.incremental_store.update_rule_stats(upserts)
        self._flush_exact_stats(upserts)
        self.incremental_store.mark_consumed(delta.cursor)

        return IncrementalRunResult(
            rules=upserts,
            processed_events=len(events),
            last_event_id=delta.cursor,
            affected_relations=affected_relations,
        )

    def _flush_exact_stats(self, rules: list[Rule]) -> None:
        pending = getattr(self, "_pending_exact_stats", None) or {}
        if not pending:
            return
        writer = getattr(self.incremental_store, "upsert_rule_stats", None)
        if writer is None:
            return
        by_id = {rule.rule_id: rule for rule in rules}
        for rule_id, (support, pca_denominator, head_count) in pending.items():
            rule = by_id.get(rule_id)
            if rule is None:
                continue
            writer(rule, support, pca_denominator, head_count)


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
