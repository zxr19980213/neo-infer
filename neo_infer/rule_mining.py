from __future__ import annotations

from dataclasses import dataclass

from neo_infer.models import Rule, build_rule_id, normalize_relation_token
from neo_infer.query import QueryRepository


@dataclass(slots=True)
class MiningConfig:
    min_support: int = 5
    min_pca_confidence: float = 0.1
    min_head_coverage: float = 0.0
    top_k: int = 100
    candidate_limit: int = 2000
    body_length: int = 2
    changed_relations: list[str] | None = None


class RuleMiningService:
    """MVP 挖掘器：r1(X,Z) ∧ r2(Z,Y) -> r3(X,Y)。"""

    def __init__(self, repository: QueryRepository) -> None:
        self._repository = repository

    def mine_rules(self, config: MiningConfig) -> list[Rule]:
        if config.body_length == 3:
            if config.changed_relations:
                return self.mine_length3_rules_incremental(config, config.changed_relations)
            return self.mine_length3_rules(config)

        if config.changed_relations:
            return self.mine_length2_rules_incremental(config, config.changed_relations)
        return self.mine_length2_rules(config)

    @staticmethod
    def _to_rules_from_candidates(
        candidates,
        head_counts: dict[str, int],
        config: MiningConfig,
    ) -> list[Rule]:
        rules: list[Rule] = []
        for candidate in candidates:
            head_total = int(head_counts.get(candidate.head_relation, 0))
            head_coverage = float(candidate.support) / float(head_total) if head_total > 0 else 0.0
            if candidate.support < config.min_support:
                continue
            if candidate.pca_confidence < config.min_pca_confidence:
                continue
            if head_coverage < config.min_head_coverage:
                continue
            rules.append(
                Rule(
                    rule_id=build_rule_id(candidate.body_relations, candidate.head_relation),
                    body_relations=candidate.body_relations,
                    head_relation=candidate.head_relation,
                    support=candidate.support,
                    pca_confidence=candidate.pca_confidence,
                    head_coverage=head_coverage,
                    status="discovered",
                    version=1,
                )
            )
        rules.sort(key=lambda x: (x.pca_confidence, x.support, x.head_coverage), reverse=True)
        return rules[: config.top_k]

    def mine_length2_rules(self, config: MiningConfig) -> list[Rule]:
        raw_candidates = self._repository.length2_path_rule_candidates(limit=config.candidate_limit)
        head_counts = self._repository.head_relation_counts()
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def build_rules_from_relation_triples(
        self,
        triples: list[tuple[str, str, str]],
        config: MiningConfig,
    ) -> list[Rule]:
        if not triples:
            return []

        head_counts = self._repository.head_relation_counts()
        rules: list[Rule] = []
        seen: set[str] = set()
        for r1, r2, head_rel in triples:
            metrics = self._repository.compute_length2_rule_metrics(r1, r2, head_rel)
            support = int(metrics.get("support", 0))
            pca_denominator = int(metrics.get("pca_denominator", 0))
            pca_confidence = float(support) / float(pca_denominator) if pca_denominator > 0 else 0.0
            head_total = int(head_counts.get(head_rel, 0))
            head_coverage = float(support) / float(head_total) if head_total > 0 else 0.0

            if support < config.min_support:
                continue
            if pca_confidence < config.min_pca_confidence:
                continue
            if head_coverage < config.min_head_coverage:
                continue

            body_relations = (r1, r2)
            rule = Rule(
                rule_id=build_rule_id(body_relations, head_rel),
                body_relations=body_relations,
                head_relation=head_rel,
                support=support,
                pca_confidence=pca_confidence,
                head_coverage=head_coverage,
                status="discovered",
                version=1,
            )
            if rule.rule_id in seen:
                continue
            seen.add(rule.rule_id)
            rules.append(rule)

        rules.sort(key=lambda x: (x.pca_confidence, x.support, x.head_coverage), reverse=True)
        return rules[: config.top_k]

    def mine_length2_rules_incremental(
        self,
        config: MiningConfig,
        affected_relations: list[str],
    ) -> list[Rule]:
        normalized = [normalize_relation_token(item) for item in affected_relations]
        normalized = [item for item in normalized if item]
        if not normalized:
            return []
        raw_candidates = self._repository.length2_path_rule_candidates_incremental(
            limit=config.candidate_limit,
            affected_relations=normalized,
        )
        head_counts = self._repository.head_relation_counts()
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def mine_length3_rules_incremental(
        self,
        config: MiningConfig,
        affected_relations: list[str],
    ) -> list[Rule]:
        normalized = [normalize_relation_token(item) for item in affected_relations]
        normalized = [item for item in normalized if item]
        if not normalized:
            return []
        raw_candidates = self._repository.length3_path_rule_candidates_incremental(
            limit=config.candidate_limit,
            affected_relations=normalized,
        )
        head_counts = self._repository.head_relation_counts()
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def mine_length3_rules(self, config: MiningConfig) -> list[Rule]:
        raw_candidates = self._repository.length3_path_rule_candidates(limit=config.candidate_limit)
        head_counts = self._repository.head_relation_counts()
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)
