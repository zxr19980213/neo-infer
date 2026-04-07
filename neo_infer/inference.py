from __future__ import annotations

from neo_infer.models import ApplyRuleResult
from neo_infer.query import QueryRepository
from neo_infer.rule_management import RuleStore


class InferenceEngine:
    def __init__(self, query_repo: QueryRepository, rule_store: RuleStore) -> None:
        self.query_repo = query_repo
        self.rule_store = rule_store

    def apply_adopted_rules_once(self, limit_rules: int = 100) -> list[ApplyRuleResult]:
        results: list[ApplyRuleResult] = []
        adopted_rules = self.rule_store.list_rules(status="adopted", limit=limit_rules)
        for rule in adopted_rules:
            created = self.query_repo.apply_path_rule_once(rule)
            if created > 0:
                self.rule_store.update_rule_status(rule.rule_id, "applied")
            results.append(ApplyRuleResult(rule_id=rule.rule_id, created_triples=created, iteration=1))
        return results

    def apply_adopted_rules_fixpoint(
        self,
        limit_rules: int = 100,
        max_iterations: int = 5,
    ) -> list[ApplyRuleResult]:
        all_results: list[ApplyRuleResult] = []
        for iteration in range(1, max_iterations + 1):
            rules = self.rule_store.list_rules(status="adopted", limit=limit_rules)
            if not rules:
                break

            created_in_iteration = 0
            for rule in rules:
                created = self.query_repo.apply_path_rule_once(rule)
                created_in_iteration += created
                if created > 0:
                    self.rule_store.update_rule_status(rule.rule_id, "applied")
                all_results.append(
                    ApplyRuleResult(
                        rule_id=rule.rule_id,
                        created_triples=created,
                        iteration=iteration,
                    )
                )

            if created_in_iteration == 0:
                break
        return all_results
