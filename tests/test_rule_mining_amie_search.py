from __future__ import annotations

from neo_infer.query import PathRuleCandidate
from neo_infer.rule_mining import MiningConfig, RuleMiningService


class StubRepo:
    def __init__(self) -> None:
        self.length2_bodies_called_with = None
        self.length2_closing_pairs = None
        self.length3_bodies_called_with = None
        self.length3_closing_triples = None

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
    # Closing only receives len-3 bodies passing support pruning.
    assert repo.length3_closing_triples == {
        "triples": [("bornIn", "locatedIn", "partOf")],
        "limit": 100,
    }
    assert len(rules) == 1
    assert len(rules[0].body_relations) == 3
    assert rules[0].head_relation == "region"
