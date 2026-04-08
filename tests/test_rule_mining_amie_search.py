from __future__ import annotations

from neo_infer.query import PathRuleCandidate
from neo_infer.rule_mining import MiningConfig, RuleMiningService


class StubRepo:
    def __init__(self) -> None:
        self.length2_bodies_called_with = None
        self.length2_closing_pairs = None
        self.length3_bodies_called_with = None
        self.length3_closing_triples = None
        self.length2_body_support_called_with = None
        self.length3_body_support_called_with = None

    def head_relation_counts(self) -> dict[str, int]:
        return {"nationality": 10, "region": 10}

    def length2_body_candidates(self, limit: int = 5000, affected_relations: list[str] | None = None):
        self.length2_bodies_called_with = {"limit": limit, "affected_relations": affected_relations}
        return [
            ("bornIn", "locatedIn", 3),
            ("low", "support", 1),
        ]

    def length2_path_rule_candidates_for_bodies(self, body_pairs: list[tuple[str, str]], limit: int = 5000):
        self.length2_closing_pairs = {"pairs": body_pairs, "limit": limit}
        return [
            PathRuleCandidate(
                body_relations=("bornIn", "locatedIn"),
                head_relation="nationality",
                support=3,
                pca_denominator=3,
            )
        ]

    def length2_body_support_for_pairs(self, body_pairs: list[tuple[str, str]]):
        self.length2_body_support_called_with = body_pairs
        return {
            ("bornIn", "locatedIn"): 3,
            ("low", "support"): 1,
        }

    def length2_path_rule_candidates_incremental(self, limit: int, affected_relations: list[str]):
        return [
            PathRuleCandidate(
                body_relations=("bornIn", "locatedIn"),
                head_relation="nationality",
                support=3,
                pca_denominator=3,
            )
        ]

    def length3_body_candidates(
        self,
        limit: int = 5000,
        affected_relations: list[str] | None = None,
        prefixes: list[tuple[str, str]] | None = None,
    ):
        self.length3_bodies_called_with = {
            "limit": limit,
            "affected_relations": affected_relations,
            "prefixes": prefixes,
        }
        return [
            ("bornIn", "locatedIn", "partOf", 2),
            ("low", "support", "x", 1),
        ]

    def length3_path_rule_candidates_for_bodies(self, body_triples: list[tuple[str, str, str]], limit: int = 5000):
        self.length3_closing_triples = {"triples": body_triples, "limit": limit}
        return [
            PathRuleCandidate(
                body_relations=("bornIn", "locatedIn", "partOf"),
                head_relation="region",
                support=2,
                pca_denominator=2,
            )
        ]

    def length3_body_support_for_triples(self, body_triples: list[tuple[str, str, str]]):
        self.length3_body_support_called_with = body_triples
        return {
            ("bornIn", "locatedIn", "partOf"): 2,
            ("low", "support", "x"): 1,
        }


def test_length2_uses_dangling_then_closing_with_support_pruning():
    repo = StubRepo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    config = MiningConfig(
        min_support=2,
        min_pca_confidence=0.1,
        top_k=50,
        candidate_limit=100,
        body_length=2,
    )

    rules = miner.mine_length2_rules(config)

    assert repo.length2_bodies_called_with == {"limit": 100, "affected_relations": None}
    assert repo.length2_body_support_called_with == [("bornIn", "locatedIn"), ("low", "support")]
    assert repo.length2_closing_pairs == {"pairs": [("bornIn", "locatedIn")], "limit": 100}
    assert len(rules) == 1
    assert rules[0].body_relations == ("bornIn", "locatedIn")
    assert rules[0].head_relation == "nationality"


def test_length3_incremental_uses_prefix_expansion_and_pruning():
    repo = StubRepo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    config = MiningConfig(
        min_support=2,
        min_pca_confidence=0.1,
        top_k=50,
        candidate_limit=100,
        body_length=3,
    )

    rules = miner.mine_length3_rules_incremental(config, [" bornIn ", "partOf"])

    # Prefixes come from len-2 bodies with support >= min_support.
    assert repo.length3_bodies_called_with == {
        "limit": 100,
        "affected_relations": ["bornIn", "partOf"],
        "prefixes": [("bornIn", "locatedIn")],
    }
    assert repo.length3_body_support_called_with == [
        ("bornIn", "locatedIn", "partOf"),
        ("low", "support", "x"),
    ]
    # Closing only receives len-3 bodies passing support pruning.
    assert repo.length3_closing_triples == {
        "triples": [("bornIn", "locatedIn", "partOf")],
        "limit": 100,
    }
    assert len(rules) == 1
    assert len(rules[0].body_relations) == 3
    assert rules[0].head_relation == "region"


def test_length2_redundancy_pruning_removes_same_support_specialization():
    class Repo(StubRepo):
        def length2_body_candidates(self, limit: int = 5000, affected_relations: list[str] | None = None):
            self.length2_bodies_called_with = {"limit": limit, "affected_relations": affected_relations}
            return [
                ("bornIn", "locatedIn", 3),
                ("bornIn", "locatedInX", 3),
            ]

        def length2_body_support_for_pairs(self, body_pairs: list[tuple[str, str]]):
            self.length2_body_support_called_with = body_pairs
            return {
                ("bornIn", "locatedIn"): 3,
                ("bornIn", "locatedInX"): 3,
            }

        def length2_path_rule_candidates_for_bodies(self, body_pairs: list[tuple[str, str]], limit: int = 5000):
            self.length2_closing_pairs = {"pairs": body_pairs, "limit": limit}
            return [
                PathRuleCandidate(
                    body_relations=("bornIn", body_pairs[0][1]),
                    head_relation="nationality",
                    support=3,
                    pca_denominator=3,
                )
            ]

    repo = Repo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    config = MiningConfig(min_support=2, min_pca_confidence=0.1, top_k=50, candidate_limit=100, body_length=2)
    rules = miner.mine_length2_rules(config)
    # Redundancy pruning keeps only one specialization for the same prefix/support signature.
    assert repo.length2_closing_pairs["pairs"] == [("bornIn", "locatedIn")]
    assert len(rules) == 1


def test_length2_beam_budget_limits_body_pairs_before_closing():
    class Repo(StubRepo):
        def length2_body_candidates(self, limit: int = 5000, affected_relations: list[str] | None = None):
            self.length2_bodies_called_with = {"limit": limit, "affected_relations": affected_relations}
            return [
                ("rA", "rB", 5),
                ("rC", "rD", 4),
                ("rE", "rF", 3),
            ]

        def length2_body_support_for_pairs(self, body_pairs: list[tuple[str, str]]):
            self.length2_body_support_called_with = body_pairs
            return {
                ("rA", "rB"): 5,
                ("rC", "rD"): 4,
                ("rE", "rF"): 3,
            }

        def length2_path_rule_candidates_for_bodies(self, body_pairs: list[tuple[str, str]], limit: int = 5000):
            self.length2_closing_pairs = {"pairs": body_pairs, "limit": limit}
            return [
                PathRuleCandidate(
                    body_relations=pair,
                    head_relation="nationality",
                    support=3,
                    pca_denominator=3,
                )
                for pair in body_pairs
            ]

    repo = Repo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    config = MiningConfig(
        min_support=1,
        min_pca_confidence=0.1,
        top_k=50,
        candidate_limit=100,
        body_length=2,
        beam_width=2,
    )
    rules = miner.mine_length2_rules(config)
    assert repo.length2_closing_pairs["pairs"] == [("rA", "rB"), ("rC", "rD")]
    assert len(rules) == 2


def test_length3_beam_budget_limits_body_triples_before_closing():
    class Repo(StubRepo):
        def length2_body_candidates(self, limit: int = 5000, affected_relations: list[str] | None = None):
            self.length2_bodies_called_with = {"limit": limit, "affected_relations": affected_relations}
            return [("r1", "r2", 5)]

        def length2_body_support_for_pairs(self, body_pairs: list[tuple[str, str]]):
            self.length2_body_support_called_with = body_pairs
            return {("r1", "r2"): 5}

        def length3_body_candidates(
            self,
            limit: int = 5000,
            affected_relations: list[str] | None = None,
            prefixes: list[tuple[str, str]] | None = None,
        ):
            self.length3_bodies_called_with = {
                "limit": limit,
                "affected_relations": affected_relations,
                "prefixes": prefixes,
            }
            return [
                ("r1", "r2", "r3a", 4),
                ("r1", "r2", "r3b", 3),
                ("r1", "r2", "r3c", 2),
            ]

        def length3_body_support_for_triples(self, body_triples: list[tuple[str, str, str]]):
            self.length3_body_support_called_with = body_triples
            return {
                ("r1", "r2", "r3a"): 4,
                ("r1", "r2", "r3b"): 3,
                ("r1", "r2", "r3c"): 2,
            }

        def length3_path_rule_candidates_for_bodies(
            self,
            body_triples: list[tuple[str, str, str]],
            limit: int = 5000,
        ):
            self.length3_closing_triples = {"triples": body_triples, "limit": limit}
            return [
                PathRuleCandidate(
                    body_relations=tri,
                    head_relation="region",
                    support=2,
                    pca_denominator=2,
                )
                for tri in body_triples
            ]

    repo = Repo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    config = MiningConfig(
        min_support=1,
        min_pca_confidence=0.1,
        top_k=50,
        candidate_limit=100,
        body_length=3,
        beam_width=2,
    )
    rules = miner.mine_length3_rules(config)
    assert repo.length3_closing_triples["triples"] == [("r1", "r2", "r3a"), ("r1", "r2", "r3b")]
    assert len(rules) == 2


def test_head_bucket_budget_keeps_top_rule_per_head():
    class Repo(StubRepo):
        def length2_body_candidates(self, limit: int = 5000, affected_relations: list[str] | None = None):
            self.length2_bodies_called_with = {"limit": limit, "affected_relations": affected_relations}
            return [
                ("r1", "r2", 3),
                ("r3", "r4", 2),
                ("r5", "r6", 1),
            ]

        def length2_body_support_for_pairs(self, body_pairs: list[tuple[str, str]]):
            self.length2_body_support_called_with = body_pairs
            return {
                ("r1", "r2"): 3,
                ("r3", "r4"): 2,
                ("r5", "r6"): 1,
            }

        def length2_path_rule_candidates_for_bodies(self, body_pairs: list[tuple[str, str]], limit: int = 5000):
            self.length2_closing_pairs = {"pairs": body_pairs, "limit": limit}
            return [
                PathRuleCandidate(
                    body_relations=("r1", "r2"),
                    head_relation="h1",
                    support=3,
                    pca_denominator=3,
                ),
                PathRuleCandidate(
                    body_relations=("r3", "r4"),
                    head_relation="h1",
                    support=2,
                    pca_denominator=2,
                ),
                PathRuleCandidate(
                    body_relations=("r5", "r6"),
                    head_relation="h2",
                    support=1,
                    pca_denominator=1,
                ),
            ]

        def head_relation_counts(self) -> dict[str, int]:
            return {"h1": 10, "h2": 10}

    repo = Repo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    config = MiningConfig(
        min_support=1,
        min_pca_confidence=0.1,
        top_k=50,
        candidate_limit=100,
        body_length=2,
        head_budget_per_relation=1,
    )
    rules = miner.mine_length2_rules(config)
    assert len(rules) == 2
    assert sorted(rule.head_relation for rule in rules) == ["h1", "h2"]
    assert sum(1 for rule in rules if rule.head_relation == "h1" and rule.body_relations == ("r1", "r2")) == 1


def test_confidence_upper_bound_tightening_is_optional():
    repo = StubRepo()
    miner = RuleMiningService(repo)  # type: ignore[arg-type]
    candidates = [
        PathRuleCandidate(
            body_relations=("r1", "r2"),
            head_relation="h1",
            support=10,
            pca_denominator=10,
        ),
        PathRuleCandidate(
            body_relations=("r3", "r4"),
            head_relation="h1",
            support=2,
            pca_denominator=20,
        ),
    ]
    baseline = miner._prune_low_confidence_upper_bound(candidates, min_confidence=0.2, confidence_ub_weight=0.0)
    tightened = miner._prune_low_confidence_upper_bound(candidates, min_confidence=0.2, confidence_ub_weight=1.0)
    assert len(baseline) == 1
    assert len(tightened) == 0
