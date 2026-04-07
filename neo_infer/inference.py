from __future__ import annotations

from neo_infer.models import ApplyRuleResult
from neo_infer.query import QueryRepository
from neo_infer.rule_management import RuleStore


class InferenceEngine:
    def __init__(self, query_repo: QueryRepository, rule_store: RuleStore) -> None:
        self.query_repo = query_repo
        self.rule_store = rule_store

    def run_once(self, limit_rules: int = 100) -> list[ApplyRuleResult]:
        results: list[ApplyRuleResult] = []
        adopted_rules = self.rule_store.list_rules(status="adopted", limit=limit_rules)
        for rule in adopted_rules:
            created = self.query_repo.apply_length2_rule(rule)
            if created > 0:
                self.rule_store.update_rule_status(rule.rule_id, "applied")
            results.append(ApplyRuleResult(rule_id=rule.rule_id, created_triples=created, iteration=1))
        return results

    def run_fixpoint(
        self,
        limit_rules: int = 100,
        max_iterations: int = 5,
    ) -> list[ApplyRuleResult]:
        all_results: list[ApplyRuleResult] = []
        adopted_rules = self.rule_store.list_rules(status="adopted", limit=limit_rules)
        if not adopted_rules:
            return all_results

        rules_with_new_facts: set[str] = set()
        for iteration in range(1, max_iterations + 1):
            created_in_iteration = 0
            for rule in adopted_rules:
                created = self.query_repo.apply_length2_rule(rule)
                created_in_iteration += created
                if created > 0:
                    rules_with_new_facts.add(rule.rule_id)
                all_results.append(
                    ApplyRuleResult(
                        rule_id=rule.rule_id,
                        created_triples=created,
                        iteration=iteration,
                    )
                )

            if created_in_iteration == 0:
                break

        for rule_id in rules_with_new_facts:
            self.rule_store.update_rule_status(rule_id, "applied")
        return all_results

    # Backward-compatible aliases.
    def apply_adopted_rules_once(self, limit_rules: int = 100) -> list[ApplyRuleResult]:
        return self.run_once(limit_rules=limit_rules)

    def apply_adopted_rules_fixpoint(
        self,
        limit_rules: int = 100,
        max_iterations: int = 5,
    ) -> list[ApplyRuleResult]:
        return self.run_fixpoint(limit_rules=limit_rules, max_iterations=max_iterations)
