from __future__ import annotations

from collections import defaultdict
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
    factual_only: bool = True
    # AMIE+ search-space controls:
    # - beam_width: keep top-B body candidates per expansion level.
    # - head_budget_per_relation: keep at most K rules per head relation.
    # - confidence_ub_weight: local-stat tightening strength for confidence UB.
    beam_width: int | None = None
    head_budget_per_relation: int | None = None
    confidence_ub_weight: float = 0.0


class RuleMiningService:
    """AMIE+-style miner with dangling/closing search skeleton."""

    def __init__(self, repository: QueryRepository) -> None:
        self._repository = repository

    def _repo_call(self, method_name: str, /, **kwargs):
        method = getattr(self._repository, method_name)
        try:
            return method(**kwargs)
        except TypeError:
            # Backward-compatible fallback for stub/legacy repos that don't accept new kwargs.
            if "factual_only" in kwargs:
                fallback = dict(kwargs)
                fallback.pop("factual_only", None)
                return method(**fallback)
            raise

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

    @staticmethod
    def _canonical_signature(body_relations: tuple[str, ...], head_relation: str) -> tuple[tuple[str, ...], str]:
        # AMIE+ style canonicalization (lightweight): remove spacing/quotes and lower-case.
        body = tuple(rel.strip().replace("`", "").lower() for rel in body_relations)
        head = head_relation.strip().replace("`", "").lower()
        return body, head

    def _dedup_by_signature(self, candidates) -> list:
        seen: set[tuple[tuple[str, ...], str]] = set()
        uniq: list = []
        for candidate in candidates:
            sig = self._canonical_signature(candidate.body_relations, candidate.head_relation)
            if sig in seen:
                continue
            seen.add(sig)
            uniq.append(candidate)
        return uniq

    @staticmethod
    def _prune_non_improving_specializations(
        candidates,
        parent_support_map: dict[tuple[str, str], int] | None = None,
    ) -> list:
        # AMIE+ high-yield pruning:
        # if adding one more body atom does not reduce support vs 2-body prefix,
        # the specialization often brings little gain and inflates search space.
        if not parent_support_map:
            return list(candidates)
        pruned: list = []
        for candidate in candidates:
            if len(candidate.body_relations) < 3:
                pruned.append(candidate)
                continue
            prefix = (candidate.body_relations[0], candidate.body_relations[1])
            parent_support = int(parent_support_map.get(prefix, -1))
            if parent_support >= 0 and candidate.support >= parent_support:
                continue
            pruned.append(candidate)
        return pruned

    def _support_prune_length2_bodies(
        self,
        bodies: list[tuple[str, str, int]],
        min_support: int,
    ) -> tuple[list[tuple[str, str]], dict[tuple[str, str], int]]:
        scan_support_map = {(r1, r2): int(sup) for r1, r2, sup in bodies}
        pairs = list(scan_support_map.keys())
        support_map = dict(scan_support_map)
        if hasattr(self._repository, "length2_body_support_for_pairs"):
            try:
                support_map = self._repository.length2_body_support_for_pairs(pairs)
            except Exception:
                support_map = dict(scan_support_map)
        kept = [pair for pair in pairs if int(support_map.get(pair, scan_support_map.get(pair, 0))) >= min_support]
        normalized_support = {
            pair: int(support_map.get(pair, scan_support_map.get(pair, 0)))
            for pair in pairs
        }
        return kept, normalized_support

    def _support_prune_length3_bodies(
        self,
        bodies: list[tuple[str, str, str, int]],
        min_support: int,
    ) -> tuple[list[tuple[str, str, str]], dict[tuple[str, str, str], int]]:
        scan_support_map = {(r1, r2, r3): int(sup) for r1, r2, r3, sup in bodies}
        triples = list(scan_support_map.keys())
        support_map = dict(scan_support_map)
        if hasattr(self._repository, "length3_body_support_for_triples"):
            try:
                support_map = self._repository.length3_body_support_for_triples(triples)
            except Exception:
                support_map = dict(scan_support_map)
        kept = [tri for tri in triples if int(support_map.get(tri, scan_support_map.get(tri, 0))) >= min_support]
        normalized_support = {
            tri: int(support_map.get(tri, scan_support_map.get(tri, 0)))
            for tri in triples
        }
        return kept, normalized_support

    @staticmethod
    def _redundancy_prune_length2_bodies(
        body_pairs: list[tuple[str, str]],
        support_map: dict[tuple[str, str], int],
    ) -> list[tuple[str, str]]:
        # Keep one representative for same (first relation, support) bucket.
        buckets: dict[tuple[str, int], list[tuple[str, str]]] = defaultdict(list)
        for pair in body_pairs:
            buckets[(pair[0], int(support_map.get(pair, 0)))].append(pair)
        kept: list[tuple[str, str]] = []
        for items in buckets.values():
            items_sorted = sorted(items, key=lambda x: (x[1], x[0]))
            kept.append(items_sorted[0])
        return sorted(kept)

    @staticmethod
    def _prune_non_improving_length3_bodies(
        body_triples: list[tuple[str, str, str]],
        triple_support_map: dict[tuple[str, str, str], int],
        prefix_support_map: dict[tuple[str, str], int],
    ) -> list[tuple[str, str, str]]:
        kept: list[tuple[str, str, str]] = []
        for tri in body_triples:
            prefix = (tri[0], tri[1])
            parent_support = int(prefix_support_map.get(prefix, -1))
            tri_support = int(triple_support_map.get(tri, 0))
            # specialization with unchanged/increased support is usually redundant.
            if parent_support >= 0 and tri_support >= parent_support:
                continue
            kept.append(tri)
        return kept

    def _relation_functionality(self) -> dict[str, float]:
        if not hasattr(self._repository, "relation_functionality"):
            return {}
        try:
            data = self._repo_call(
                "relation_functionality",
                factual_only=self._active_factual_only,
            )
            return {str(k): float(v) for k, v in data.items()}
        except Exception:
            return {}

    @property
    def _active_factual_only(self) -> bool:
        return bool(getattr(self, "_factual_only", True))

    def _sort_length2_bodies_by_functionality(
        self,
        body_pairs: list[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        functionality = self._relation_functionality()
        if not functionality:
            return sorted(body_pairs)
        return sorted(
            body_pairs,
            key=lambda p: (
                -(functionality.get(p[0], 0.0) + functionality.get(p[1], 0.0)),
                -functionality.get(p[0], 0.0),
                -functionality.get(p[1], 0.0),
                p[0],
                p[1],
            ),
        )

    def _sort_length3_bodies_by_functionality(
        self,
        body_triples: list[tuple[str, str, str]],
    ) -> list[tuple[str, str, str]]:
        functionality = self._relation_functionality()
        if not functionality:
            return sorted(body_triples)
        return sorted(
            body_triples,
            key=lambda t: (
                -(functionality.get(t[0], 0.0) + functionality.get(t[1], 0.0) + functionality.get(t[2], 0.0)),
                -functionality.get(t[0], 0.0),
                -functionality.get(t[1], 0.0),
                -functionality.get(t[2], 0.0),
                t[0],
                t[1],
                t[2],
            ),
        )

    def _apply_beam_budget_length2(
        self,
        body_pairs: list[tuple[str, str]],
        support_map: dict[tuple[str, str], int],
        beam_width: int | None,
    ) -> list[tuple[str, str]]:
        if not beam_width or beam_width <= 0 or len(body_pairs) <= beam_width:
            return body_pairs
        functionality = self._relation_functionality()
        ranked = sorted(
            body_pairs,
            key=lambda pair: (
                -int(support_map.get(pair, 0)),
                -(functionality.get(pair[0], 0.0) + functionality.get(pair[1], 0.0)),
                -functionality.get(pair[0], 0.0),
                -functionality.get(pair[1], 0.0),
                pair[0],
                pair[1],
            ),
        )
        return ranked[:beam_width]

    def _apply_beam_budget_length3(
        self,
        body_triples: list[tuple[str, str, str]],
        support_map: dict[tuple[str, str, str], int],
        beam_width: int | None,
    ) -> list[tuple[str, str, str]]:
        if not beam_width or beam_width <= 0 or len(body_triples) <= beam_width:
            return body_triples
        functionality = self._relation_functionality()
        ranked = sorted(
            body_triples,
            key=lambda tri: (
                -int(support_map.get(tri, 0)),
                -(functionality.get(tri[0], 0.0) + functionality.get(tri[1], 0.0) + functionality.get(tri[2], 0.0)),
                -functionality.get(tri[0], 0.0),
                -functionality.get(tri[1], 0.0),
                -functionality.get(tri[2], 0.0),
                tri[0],
                tri[1],
                tri[2],
            ),
        )
        return ranked[:beam_width]

    @staticmethod
    def _apply_head_bucket_budget(candidates, per_head_budget: int | None) -> list:
        if not per_head_budget or per_head_budget <= 0:
            return list(candidates)
        buckets: dict[str, list] = defaultdict(list)
        for candidate in candidates:
            buckets[candidate.head_relation].append(candidate)

        kept: list = []
        for items in buckets.values():
            ranked = sorted(
                items,
                key=lambda candidate: (
                    -candidate.pca_confidence,
                    -int(candidate.support),
                    int(candidate.pca_denominator),
                    candidate.body_relations,
                ),
            )
            kept.extend(ranked[:per_head_budget])
        kept.sort(
            key=lambda candidate: (
                -candidate.pca_confidence,
                -int(candidate.support),
                int(candidate.pca_denominator),
                candidate.body_relations,
                candidate.head_relation,
            )
        )
        return kept

    @staticmethod
    def _prune_low_confidence_upper_bound(
        candidates,
        min_confidence: float,
        confidence_ub_weight: float = 0.0,
    ) -> list:
        # Optimistic confidence upper bound: support / max(1, support) = 1 for strict support-only.
        # Here we start from exact candidate denominator (safe bound), then optionally
        # tighten with per-head local denominator ratio statistics.
        pruned: list = []
        weight = max(0.0, min(1.0, float(confidence_ub_weight)))
        ratio_by_head: dict[str, float] = {}
        if weight > 0.0:
            ratio_sum: dict[str, float] = defaultdict(float)
            ratio_cnt: dict[str, int] = defaultdict(int)
            for candidate in candidates:
                support = max(1, int(candidate.support))
                denom = max(1, int(candidate.pca_denominator))
                ratio_sum[candidate.head_relation] += float(denom) / float(support)
                ratio_cnt[candidate.head_relation] += 1
            ratio_by_head = {
                head: (ratio_sum[head] / float(ratio_cnt[head])) if ratio_cnt[head] > 0 else 1.0
                for head in ratio_sum
            }

        for candidate in candidates:
            support = max(1, int(candidate.support))
            denom_lb = max(1, int(candidate.pca_denominator))
            effective_denom = denom_lb
            if weight > 0.0:
                local_ratio = max(1.0, float(ratio_by_head.get(candidate.head_relation, 1.0)))
                local_expected = int(round(float(support) * local_ratio))
                blended = int(round((1.0 - weight) * float(denom_lb) + weight * float(local_expected)))
                effective_denom = max(denom_lb, max(1, blended))
            upper_bound = float(candidate.support) / float(effective_denom)
            if upper_bound < min_confidence:
                continue
            pruned.append(candidate)
        return pruned

    def mine_length2_rules(self, config: MiningConfig) -> list[Rule]:
        self._factual_only = bool(config.factual_only)
        # AMIE dangling step (len-2): enumerate body candidates first, then close to head.
        bodies = self._repo_call(
            "length2_body_candidates",
            limit=config.candidate_limit,
            affected_relations=config.changed_relations,
            factual_only=config.factual_only,
        )
        body_pairs, support_map = self._support_prune_length2_bodies(bodies, config.min_support)
        body_pairs = self._redundancy_prune_length2_bodies(body_pairs, support_map)
        body_pairs = self._sort_length2_bodies_by_functionality(body_pairs)
        body_pairs = self._apply_beam_budget_length2(body_pairs, support_map, config.beam_width)
        body_pairs = body_pairs[: config.candidate_limit]
        raw_candidates = self._repo_call(
            "length2_path_rule_candidates_for_bodies",
            body_pairs=body_pairs,
            limit=config.candidate_limit,
            factual_only=config.factual_only,
        )
        raw_candidates = self._dedup_by_signature(raw_candidates)
        raw_candidates = self._prune_low_confidence_upper_bound(
            raw_candidates,
            config.min_pca_confidence,
            config.confidence_ub_weight,
        )
        raw_candidates = self._apply_head_bucket_budget(raw_candidates, config.head_budget_per_relation)
        head_counts = self._repo_call(
            "head_relation_counts",
            factual_only=config.factual_only,
        )
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def build_rules_from_relation_triples(
        self,
        triples: list[tuple[str, str, str]],
        config: MiningConfig,
    ) -> list[Rule]:
        if not triples:
            return []

        head_counts = self._repo_call(
            "head_relation_counts",
            factual_only=config.factual_only,
        )
        rules: list[Rule] = []
        seen: set[str] = set()
        for r1, r2, head_rel in triples:
            metrics = self._repo_call(
                "compute_length2_rule_metrics",
                r1,
                r2,
                head_rel,
                factual_only=config.factual_only,
            )
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
        self._factual_only = bool(config.factual_only)
        normalized = [normalize_relation_token(item) for item in affected_relations]
        normalized = [item for item in normalized if item]
        if not normalized:
            return []
        bodies = self._repo_call(
            "length2_body_candidates",
            limit=config.candidate_limit,
            affected_relations=normalized,
            factual_only=config.factual_only,
        )
        body_pairs, support_map = self._support_prune_length2_bodies(bodies, config.min_support)
        body_pairs = self._redundancy_prune_length2_bodies(body_pairs, support_map)
        body_pairs = self._sort_length2_bodies_by_functionality(body_pairs)
        body_pairs = self._apply_beam_budget_length2(body_pairs, support_map, config.beam_width)
        body_pairs = body_pairs[: config.candidate_limit]
        raw_candidates = self._repo_call(
            "length2_path_rule_candidates_for_bodies",
            body_pairs=body_pairs,
            limit=config.candidate_limit,
            factual_only=config.factual_only,
        )
        raw_candidates = self._dedup_by_signature(raw_candidates)
        raw_candidates = self._prune_low_confidence_upper_bound(
            raw_candidates,
            config.min_pca_confidence,
            config.confidence_ub_weight,
        )
        raw_candidates = self._apply_head_bucket_budget(raw_candidates, config.head_budget_per_relation)
        head_counts = self._repo_call(
            "head_relation_counts",
            factual_only=config.factual_only,
        )
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def mine_length3_rules_incremental(
        self,
        config: MiningConfig,
        affected_relations: list[str],
    ) -> list[Rule]:
        self._factual_only = bool(config.factual_only)
        normalized = [normalize_relation_token(item) for item in affected_relations]
        normalized = [item for item in normalized if item]
        if not normalized:
            return []
        # AMIE dangling+closing (len-3 incremental):
        # 1) mine length-2 prefix bodies under affected relations
        # 2) expand dangling third atom constrained by prefixes
        # 3) close to head relations
        prefix_bodies = self._repo_call(
            "length2_body_candidates",
            limit=config.candidate_limit,
            affected_relations=normalized,
            factual_only=config.factual_only,
        )
        prefixes, prefix_support_map = self._support_prune_length2_bodies(prefix_bodies, config.min_support)
        prefixes = self._redundancy_prune_length2_bodies(prefixes, prefix_support_map)
        prefixes = self._sort_length2_bodies_by_functionality(prefixes)[: config.candidate_limit]
        len3_bodies = self._repo_call(
            "length3_body_candidates",
            limit=config.candidate_limit,
            affected_relations=normalized,
            prefixes=prefixes,
            factual_only=config.factual_only,
        )
        body_triples, triple_support_map = self._support_prune_length3_bodies(len3_bodies, config.min_support)
        body_triples = self._prune_non_improving_length3_bodies(
            body_triples,
            triple_support_map,
            prefix_support_map,
        )
        body_triples = self._sort_length3_bodies_by_functionality(body_triples)
        body_triples = self._apply_beam_budget_length3(body_triples, triple_support_map, config.beam_width)
        body_triples = body_triples[: config.candidate_limit]
        raw_candidates = self._repo_call(
            "length3_path_rule_candidates_for_bodies",
            body_triples=body_triples,
            limit=config.candidate_limit,
            factual_only=config.factual_only,
        )
        raw_candidates = self._dedup_by_signature(raw_candidates)
        raw_candidates = self._prune_low_confidence_upper_bound(
            raw_candidates,
            config.min_pca_confidence,
            config.confidence_ub_weight,
        )
        raw_candidates = self._apply_head_bucket_budget(raw_candidates, config.head_budget_per_relation)
        head_counts = self._repo_call(
            "head_relation_counts",
            factual_only=config.factual_only,
        )
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)

    def mine_length3_rules(self, config: MiningConfig) -> list[Rule]:
        self._factual_only = bool(config.factual_only)
        # AMIE dangling+closing (len-3 full):
        # 1) enumerate length-2 bodies
        # 2) dangle third body atom on top of prefixes
        # 3) close to head relations
        prefix_bodies = self._repo_call(
            "length2_body_candidates",
            limit=config.candidate_limit,
            affected_relations=config.changed_relations,
            factual_only=config.factual_only,
        )
        prefixes, prefix_support_map = self._support_prune_length2_bodies(prefix_bodies, config.min_support)
        prefixes = self._redundancy_prune_length2_bodies(prefixes, prefix_support_map)
        prefixes = self._sort_length2_bodies_by_functionality(prefixes)[: config.candidate_limit]
        len3_bodies = self._repo_call(
            "length3_body_candidates",
            limit=config.candidate_limit,
            affected_relations=config.changed_relations,
            prefixes=prefixes,
            factual_only=config.factual_only,
        )
        body_triples, triple_support_map = self._support_prune_length3_bodies(len3_bodies, config.min_support)
        body_triples = self._prune_non_improving_length3_bodies(
            body_triples,
            triple_support_map,
            prefix_support_map,
        )
        body_triples = self._sort_length3_bodies_by_functionality(body_triples)
        body_triples = self._apply_beam_budget_length3(body_triples, triple_support_map, config.beam_width)
        body_triples = body_triples[: config.candidate_limit]
        raw_candidates = self._repo_call(
            "length3_path_rule_candidates_for_bodies",
            body_triples=body_triples,
            limit=config.candidate_limit,
            factual_only=config.factual_only,
        )
        raw_candidates = self._dedup_by_signature(raw_candidates)
        raw_candidates = self._prune_low_confidence_upper_bound(
            raw_candidates,
            config.min_pca_confidence,
            config.confidence_ub_weight,
        )
        raw_candidates = self._apply_head_bucket_budget(raw_candidates, config.head_budget_per_relation)
        head_counts = self._repo_call(
            "head_relation_counts",
            factual_only=config.factual_only,
        )
        return self._to_rules_from_candidates(raw_candidates, head_counts, config)
