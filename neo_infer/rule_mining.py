from __future__ import annotations

from dataclasses import dataclass

from neo_infer.models import Rule, build_rule_id
from neo_infer.query import QueryRepository


@dataclass(slots=True)
class MiningConfig:
    min_support: int = 5
    min_pca_confidence: float = 0.1
    min_head_coverage: float = 0.0
    top_k: int = 100
    candidate_limit: int = 2000


class RuleMiningService:
    """MVP 挖掘器：r1(X,Z) ∧ r2(Z,Y) -> r3(X,Y)。"""

    def __init__(self, repository: QueryRepository) -> None:
        self._repository = repository

    def mine_length2_rules(self, config: MiningConfig) -> list[Rule]:
        raw_candidates = self._repository.length2_path_rule_candidates(limit=config.candidate_limit)
        head_counts = self._repository.head_relation_counts()
        rules: list[Rule] = []
        for candidate in raw_candidates:
            head_total = int(head_counts.get(candidate.head_r3, 0))
            head_coverage = float(candidate.support) / float(head_total) if head_total > 0 else 0.0

            if candidate.support < config.min_support:
                continue
            if candidate.pca_confidence < config.min_pca_confidence:
                continue
            if head_coverage < config.min_head_coverage:
                continue
            body_relations = (candidate.body_r1, candidate.body_r2)
            rules.append(
                Rule(
                    rule_id=build_rule_id(body_relations, candidate.head_r3),
                    body_relations=body_relations,
                    head_relation=candidate.head_r3,
                    support=candidate.support,
                    pca_confidence=candidate.pca_confidence,
                    head_coverage=head_coverage,
                    status="discovered",
                    version=1,
                )
            )
        rules.sort(key=lambda x: (x.pca_confidence, x.support, x.head_coverage), reverse=True)
        return rules[: config.top_k]
