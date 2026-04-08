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
    """AMIE+-style miner with dangling/closing search skeleton."""

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
        # AMIE dangling step (len-2): enumerate body candidates first, then close to head.
        bodies = self._repository.length2_body_candidates(
            limit=config.candidate_limit,
            affected_relations=config.changed_relations,
        )
        body_pairs = [(r1, r2) for r1, r2, body_support in bodies if body_support >= config.min_support]
        raw_candidates = self._repository.length2_path_rule_candidates_for_bodies(
            body_pairs=body_pairs,
            limit=config.candidate_limit,
        )
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
        # Keep same path but force relation filter for impacted relations only.
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
        # AMIE dangling+closing (len-3 incremental):
        # 1) mine length-2 prefix bodies under affected relations
        # 2) expand dangling third atom constrained by prefixes
        # 3) close to head relations
        prefix_bodies = self._repository.length2_body_candidates(
            limit=config.candidate_limit,
            affected_relations=normalized,
        )
        prefixes = [(r1, r2) for r1, r2, body_support in prefix_bodies if body_support >= config.min_support]
        len3_bodies = self._repository.length3_body_candidates(
            limit=config.candidate_limit,
            affected_relations=normalized,
            prefixes=prefixes,
        )
        body_triples = [
            (r1, r2, r3)
            for r1, r2, r3, body_support in len3_bodies
            if body_support >= config.min_support
        ]
        raw_candidates = self._repository.length3_path_rule_candidates_for_bodies(
            body_triples=body_triples,
            limit=config.candidate_limit,
        )
        head_counts = self._repository.head_relation_counts()
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def mine_length3_rules(self, config: MiningConfig) -> list[Rule]:
        # AMIE dangling+closing (len-3 full):
        # 1) enumerate length-2 bodies
        # 2) dangle third body atom on top of prefixes
        # 3) close to head relations
        prefix_bodies = self._repository.length2_body_candidates(
            limit=config.candidate_limit,
            affected_relations=config.changed_relations,
        )
        prefixes = [(r1, r2) for r1, r2, body_support in prefix_bodies if body_support >= config.min_support]
        len3_bodies = self._repository.length3_body_candidates(
            limit=config.candidate_limit,
            affected_relations=config.changed_relations,
            prefixes=prefixes,
        )
        body_triples = [
            (r1, r2, r3)
            for r1, r2, r3, body_support in len3_bodies
            if body_support >= config.min_support
        ]
        raw_candidates = self._repository.length3_path_rule_candidates_for_bodies(
            body_triples=body_triples,
            limit=config.candidate_limit,
        )
        head_counts = self._repository.head_relation_counts()
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)
