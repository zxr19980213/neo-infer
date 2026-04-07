from __future__ import annotations

from dataclasses import dataclass

from neo_infer.models import ApplyRuleResult
from neo_infer.query import QueryRepository
from neo_infer.rule_management import RuleStore


@dataclass(frozen=True)
class InferenceRunSummary:
    results: list[ApplyRuleResult]
    conflicts_detected: int


class InferenceEngine:
    def __init__(
        self,
        query_repo: QueryRepository,
        rule_store: RuleStore,
        conflict_relation_pairs: dict[str, set[str]] | None = None,
    ) -> None:
        self.query_repo = query_repo
        self.rule_store = rule_store
        self.conflict_relation_pairs = conflict_relation_pairs or {}

    def _count_conflicts_for_rule(self, rule, check_conflicts: bool) -> int:
        if not check_conflicts:
            return 0
        conflict_relations = self.conflict_relation_pairs.get(rule.head_relation, set())
        total = 0
        for negative_relation in conflict_relations:
            total += self.query_repo.count_conflicts_for_rule(rule, negative_relation)
        return total

    def run_once(
        self,
        limit_rules: int = 100,
        check_conflicts: bool = True,
    ) -> InferenceRunSummary:
        results: list[ApplyRuleResult] = []
        total_conflicts = 0
        adopted_rules = self.rule_store.list_rules(status="adopted", limit=limit_rules)
        for rule in adopted_rules:
            conflicts = self._count_conflicts_for_rule(rule, check_conflicts=check_conflicts)
            created = self.query_repo.apply_length2_rule(rule)
            total_conflicts += conflicts
            if created > 0:
                self.rule_store.update_rule_status(rule.rule_id, "applied")
                self.rule_store.bump_rule_version(rule.rule_id)
            results.append(
                ApplyRuleResult(
                    rule_id=rule.rule_id,
                    created_triples=created,
                    conflict_triples=conflicts,
                    iteration=1,
                )
            )
        return InferenceRunSummary(results=results, conflicts_detected=total_conflicts)

    def run_fixpoint(
        self,
        limit_rules: int = 100,
        max_iterations: int = 5,
        check_conflicts: bool = True,
    ) -> InferenceRunSummary:
        all_results: list[ApplyRuleResult] = []
        total_conflicts = 0
        adopted_rules = self.rule_store.list_rules(status="adopted", limit=limit_rules)
        if not adopted_rules:
            return InferenceRunSummary(results=all_results, conflicts_detected=total_conflicts)

        rules_with_new_facts: set[str] = set()
        for iteration in range(1, max_iterations + 1):
            created_in_iteration = 0
            for rule in adopted_rules:
                conflicts = self._count_conflicts_for_rule(rule, check_conflicts=check_conflicts)
                created = self.query_repo.apply_length2_rule(rule)
                created_in_iteration += created
                total_conflicts += conflicts
                if created > 0:
                    rules_with_new_facts.add(rule.rule_id)
                all_results.append(
                    ApplyRuleResult(
                        rule_id=rule.rule_id,
                        created_triples=created,
                        conflict_triples=conflicts,
                        iteration=iteration,
                    )
                )

            if created_in_iteration == 0:
                break

        for rule_id in rules_with_new_facts:
            self.rule_store.update_rule_status(rule_id, "applied")
            self.rule_store.bump_rule_version(rule_id)
        return InferenceRunSummary(results=all_results, conflicts_detected=total_conflicts)

    # Backward-compatible aliases.
    def apply_adopted_rules_once(self, limit_rules: int = 100) -> list[ApplyRuleResult]:
        return self.run_once(limit_rules=limit_rules).results

    def apply_adopted_rules_fixpoint(
        self,
        limit_rules: int = 100,
        max_iterations: int = 5,
    ) -> list[ApplyRuleResult]:
        return self.run_fixpoint(limit_rules=limit_rules, max_iterations=max_iterations).results
