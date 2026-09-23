from __future__ import annotations

from neo_infer.incremental_counters import length2_stat_delta, length3_stat_delta
from neo_infer.incremental_mining import IncrementalMiningService
from neo_infer.models import ChangeEdge, DeltaBatch, MineRulesRequest, Rule


def _delta(present, added=(), removed=()):
    return length2_stat_delta(
        r1="bornIn",
        r2="locatedIn",
        head="nationality",
        present=set(present),
        added=list(added),
        removed=list(removed),
    )


def test_added_body_edge_completes_one_support_pair():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "china"),
        ("alice", "nationality", "china"),
        ("bob", "bornIn", "shanghai"),
        ("shanghai", "locatedIn", "china"),
        ("bob", "nationality", "china"),
    }
    support, pca, head = _delta(present, added=[("bob", "bornIn", "shanghai")])
    assert (support, pca, head) == (1, 1, 0)


def test_extra_body_path_does_not_increase_support():
    present = {
        ("alice", "bornIn", "beijing"),
        ("alice", "bornIn", "tianjin"),
        ("beijing", "locatedIn", "china"),
        ("tianjin", "locatedIn", "china"),
        ("alice", "nationality", "china"),
    }
    support, pca, head = _delta(present, added=[("alice", "bornIn", "tianjin")])
    assert (support, pca, head) == (0, 0, 0)


def test_removing_the_last_body_path_decreases_support():
    present = {
        ("beijing", "locatedIn", "china"),
        ("alice", "nationality", "china"),
    }
    support, pca, head = _delta(present, removed=[("alice", "bornIn", "beijing")])
    assert (support, pca, head) == (-1, -1, 0)


def test_first_head_edge_counts_every_body_pair_in_pca():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "china"),
        ("alice", "bornIn", "osaka"),
        ("osaka", "locatedIn", "japan"),
        ("alice", "nationality", "china"),
    }
    support, pca, head = _delta(present, added=[("alice", "nationality", "china")])
    assert (support, pca, head) == (1, 2, 1)


def test_second_head_edge_does_not_change_pca():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "china"),
        ("alice", "bornIn", "osaka"),
        ("osaka", "locatedIn", "japan"),
        ("alice", "nationality", "china"),
        ("alice", "nationality", "japan"),
    }
    support, pca, head = _delta(present, added=[("alice", "nationality", "japan")])
    assert (support, pca, head) == (1, 0, 1)


def test_two_added_edges_form_one_new_pair():
    present = {
        ("bob", "bornIn", "shanghai"),
        ("shanghai", "locatedIn", "china"),
        ("bob", "nationality", "china"),
    }
    support, pca, head = _delta(
        present,
        added=[
            ("bob", "bornIn", "shanghai"),
            ("shanghai", "locatedIn", "china"),
        ],
    )
    assert (support, pca, head) == (1, 1, 0)


def test_inferred_head_absent_from_factual_neighborhood_is_ignored():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "china"),
    }
    support, pca, head = _delta(present, added=[("alice", "nationality", "china")])
    assert (support, pca, head) == (0, 0, 0)


def test_same_relation_on_both_body_atoms():
    present = {
        ("beijing", "locatedIn", "china"),
        ("alice", "locatedIn", "beijing"),
        ("alice", "nationality", "china"),
    }
    support, pca, head = length2_stat_delta(
        r1="locatedIn",
        r2="locatedIn",
        head="nationality",
        present=present,
        added=[("beijing", "locatedIn", "china")],
        removed=[],
    )
    assert (support, pca, head) == (1, 1, 0)


class _Stat:
    def __init__(self, support: int, pca_denominator: int, head_count: int) -> None:
        self.support = support
        self.pca_denominator = pca_denominator
        self.head_count = head_count


class _Store:
    def __init__(self, rule_ids: list[str] | None = None) -> None:
        self.stats = {}
        self.cursor = 0
        self.rule_ids = rule_ids or ["rule__bornin__locatedin__to__nationality"]

    def get_rule_stat(self, rule_id: str):
        return self.stats.get(rule_id)

    def affected_rule_ids(self, relations):
        _ = relations
        return list(self.rule_ids)

    def consume_delta(self, limit: int = 2000):
        _ = limit
        return DeltaBatch(
            added_edges=[ChangeEdge(src="bob", rel="bornIn", dst="shanghai")],
            removed_edges=[],
            cursor=4,
        )

    def update_rule_indexes(self, rules):
        _ = rules

    def update_rule_stats(self, rules):
        _ = rules

    def upsert_rule_stats(self, rule, support, pca_denominator, head_count):
        self.stats[rule.rule_id] = _Stat(support, pca_denominator, head_count)

    def mark_consumed(self, cursor: int):
        self.cursor = cursor


class _RuleStore:
    def __init__(self, rule: Rule) -> None:
        self.rule = rule
        self.saved: list[Rule] = []

    def list_rules_by_ids(self, rule_ids):
        _ = rule_ids
        return [self.rule]

    def upsert_rules(self, rules):
        self.saved = list(rules)


class _Repo:
    def __init__(self) -> None:
        self.factual_only = None
        self.full_calls = 0

    def length2_neighborhood_edges(self, *, r1, r2, head_rel, node_keys, factual_only=True):
        _ = (r1, r2, head_rel, node_keys)
        self.factual_only = factual_only
        return [
            ("alice", "bornIn", "beijing"),
            ("beijing", "locatedIn", "china"),
            ("alice", "nationality", "china"),
            ("bob", "bornIn", "shanghai"),
            ("shanghai", "locatedIn", "china"),
            ("bob", "nationality", "china"),
        ]

    def compute_length2_rule_metrics(self, r1, r2, head_rel, factual_only=True):
        _ = (r1, r2, head_rel)
        self.full_calls += 1
        self.factual_only = factual_only
        return {"support": 9, "pca_denominator": 9, "head_count": 9}


def _rule() -> Rule:
    return Rule(
        rule_id="rule__bornin__locatedin__to__nationality",
        body_relations=("bornIn", "locatedIn"),
        head_relation="nationality",
        support=1,
        pca_confidence=0.5,
        head_coverage=0.5,
        status="discovered",
        version=1,
    )


class _Miner:
    def __init__(self, repo) -> None:
        self._repository = repo

    def mine_rules(self, config):
        _ = config
        return []


def test_service_applies_length2_event_delta_and_keeps_exact_stats():
    repo = _Repo()
    store = _Store()
    store.stats[_rule().rule_id] = _Stat(1, 2, 2)
    rules = _RuleStore(_rule())
    service = IncrementalMiningService(_Miner(repo), rules, store)
    result = service.run_incremental(
        MineRulesRequest(limit=10, min_support=1, min_pca_confidence=0.0, factual_only=False, body_length=2),
        body_length=2,
    )
    updated = next(rule for rule in result.rules if rule.rule_id == _rule().rule_id)
    assert updated.support == 2
    assert updated.pca_confidence == 2 / 3
    assert updated.head_coverage == 1.0
    assert repo.full_calls == 0
    assert repo.factual_only is False
    assert store.stats[updated.rule_id].pca_denominator == 3
    assert store.stats[updated.rule_id].head_count == 2


def test_factual_only_ignores_inferred_removal():
    repo = _Repo()

    def neighborhood(**kwargs):
        repo.factual_only = kwargs["factual_only"]
        return [
            ("alice", "bornIn", "beijing"),
            ("beijing", "locatedIn", "china"),
            ("alice", "nationality", "china"),
            ("shanghai", "locatedIn", "china"),
            ("bob", "nationality", "china"),
        ]

    repo.length2_neighborhood_edges = neighborhood
    store = _Store()
    store.stats[_rule().rule_id] = _Stat(1, 1, 1)
    store.consume_delta = lambda limit=2000: DeltaBatch(
        added_edges=[],
        removed_edges=[ChangeEdge(src="bob", rel="bornIn", dst="shanghai", is_inferred=True)],
        cursor=8,
    )
    service = IncrementalMiningService(_Miner(repo), _RuleStore(_rule()), store)
    result = service.run_incremental(
        MineRulesRequest(limit=10, min_support=0, min_pca_confidence=0.0, factual_only=True, body_length=2),
        body_length=2,
    )
    updated = next(rule for rule in result.rules if rule.rule_id == _rule().rule_id)
    assert updated.support == 1
    assert store.stats[updated.rule_id].support == 1
    assert repo.full_calls == 0


def test_service_recomputes_with_factual_only_when_neighborhood_is_unavailable():
    class Repo:
        def __init__(self) -> None:
            self.factual_only = None

        def compute_length2_rule_metrics(self, r1, r2, head_rel, factual_only=True):
            _ = (r1, r2, head_rel)
            self.factual_only = factual_only
            return {"support": 4, "pca_denominator": 5, "head_count": 6}

    repo = Repo()
    store = _Store()
    store.stats[_rule().rule_id] = _Stat(1, 2, 2)
    service = IncrementalMiningService(_Miner(repo), _RuleStore(_rule()), store)
    result = service.run_incremental(
        MineRulesRequest(limit=10, min_support=0, min_pca_confidence=0.0, factual_only=False, body_length=2),
        body_length=2,
    )
    updated = result.rules[0]
    assert updated.support == 4
    assert repo.factual_only is False
    assert store.stats[updated.rule_id].pca_denominator == 5
    assert store.stats[updated.rule_id].head_count == 6


def _delta3(present, added=(), removed=()):
    return length3_stat_delta(
        r1="bornIn",
        r2="locatedIn",
        r3="partOf",
        head="region",
        present=set(present),
        added=list(added),
        removed=list(removed),
    )


def test_length3_added_body_edge_completes_one_support_pair():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "hebei"),
        ("hebei", "partOf", "china"),
        ("alice", "region", "china"),
        ("bob", "bornIn", "shanghai"),
        ("shanghai", "locatedIn", "zhejiang"),
        ("zhejiang", "partOf", "china"),
        ("bob", "region", "china"),
    }
    assert _delta3(present, added=[("bob", "bornIn", "shanghai")]) == (1, 1, 0)


def test_length3_extra_body_path_does_not_increase_support():
    present = {
        ("alice", "bornIn", "beijing"),
        ("alice", "bornIn", "tianjin"),
        ("beijing", "locatedIn", "hebei"),
        ("tianjin", "locatedIn", "hebei"),
        ("hebei", "partOf", "china"),
        ("alice", "region", "china"),
    }
    assert _delta3(present, added=[("alice", "bornIn", "tianjin")]) == (0, 0, 0)


def test_length3_removing_the_middle_edge_drops_the_only_path():
    present = {
        ("alice", "bornIn", "beijing"),
        ("hebei", "partOf", "china"),
        ("alice", "region", "china"),
    }
    assert _delta3(present, removed=[("beijing", "locatedIn", "hebei")]) == (-1, -1, 0)


def test_length3_removing_one_path_keeps_support_while_another_remains():
    present = {
        ("alice", "bornIn", "beijing"),
        ("alice", "bornIn", "tianjin"),
        ("tianjin", "locatedIn", "hebei"),
        ("hebei", "partOf", "china"),
        ("alice", "region", "china"),
    }
    assert _delta3(present, removed=[("beijing", "locatedIn", "hebei")]) == (0, 0, 0)


def test_length3_first_head_edge_counts_every_body_pair_in_pca():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "hebei"),
        ("hebei", "partOf", "china"),
        ("alice", "bornIn", "osaka"),
        ("osaka", "locatedIn", "kansai"),
        ("kansai", "partOf", "japan"),
        ("alice", "region", "china"),
    }
    assert _delta3(present, added=[("alice", "region", "china")]) == (1, 2, 1)


def test_length3_second_head_edge_does_not_change_pca():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "hebei"),
        ("hebei", "partOf", "china"),
        ("alice", "bornIn", "osaka"),
        ("osaka", "locatedIn", "kansai"),
        ("kansai", "partOf", "japan"),
        ("alice", "region", "china"),
        ("alice", "region", "japan"),
    }
    assert _delta3(present, added=[("alice", "region", "japan")]) == (1, 0, 1)


def test_length3_two_added_edges_form_one_new_pair():
    present = {
        ("bob", "bornIn", "shanghai"),
        ("shanghai", "locatedIn", "zhejiang"),
        ("zhejiang", "partOf", "china"),
        ("bob", "region", "china"),
    }
    assert _delta3(
        present,
        added=[
            ("shanghai", "locatedIn", "zhejiang"),
            ("zhejiang", "partOf", "china"),
        ],
    ) == (1, 1, 0)


def test_length3_path_does_not_reuse_one_edge():
    present = {
        ("alice", "link", "beijing"),
        ("beijing", "link", "alice"),
        ("alice", "region", "beijing"),
    }
    support, pca, head = length3_stat_delta(
        r1="link",
        r2="link",
        r3="link",
        head="region",
        present=present,
        added=[("beijing", "link", "alice")],
        removed=[],
    )
    assert (support, pca, head) == (0, 0, 0)


def test_length3_same_relation_on_three_body_atoms():
    present = {
        ("beijing", "locatedIn", "hebei"),
        ("hebei", "locatedIn", "china"),
        ("china", "locatedIn", "asia"),
        ("beijing", "region", "asia"),
    }
    support, pca, head = length3_stat_delta(
        r1="locatedIn",
        r2="locatedIn",
        r3="locatedIn",
        head="region",
        present=present,
        added=[("china", "locatedIn", "asia")],
        removed=[],
    )
    assert (support, pca, head) == (1, 1, 0)


def test_length3_inferred_head_absent_from_factual_neighborhood_is_ignored():
    present = {
        ("alice", "bornIn", "beijing"),
        ("beijing", "locatedIn", "hebei"),
        ("hebei", "partOf", "china"),
    }
    assert _delta3(present, added=[("alice", "region", "china")]) == (0, 0, 0)


def _rule_len3() -> Rule:
    return Rule(
        rule_id="rule__bornin__locatedin__partof__to__region",
        body_relations=("bornIn", "locatedIn", "partOf"),
        head_relation="region",
        support=1,
        pca_confidence=1.0,
        head_coverage=0.5,
        status="discovered",
        version=1,
    )


def test_service_applies_length3_event_delta_and_keeps_exact_stats():
    repo = _Repo()
    seen: dict[str, object] = {}

    def neighborhood(**kwargs):
        seen.update(kwargs)
        return [
            ("alice", "bornIn", "beijing"),
            ("beijing", "locatedIn", "hebei"),
            ("hebei", "partOf", "china"),
            ("alice", "region", "china"),
            ("bob", "bornIn", "shanghai"),
            ("shanghai", "locatedIn", "zhejiang"),
            ("zhejiang", "partOf", "china"),
            ("bob", "region", "china"),
        ]

    repo.length3_neighborhood_edges = neighborhood
    store = _Store(rule_ids=[_rule_len3().rule_id])
    store.stats[_rule_len3().rule_id] = _Stat(1, 1, 2)
    service = IncrementalMiningService(_Miner(repo), _RuleStore(_rule_len3()), store)
    result = service.run_incremental(
        MineRulesRequest(limit=10, min_support=1, min_pca_confidence=0.0, factual_only=False, body_length=3),
        body_length=3,
    )
    updated = next(rule for rule in result.rules if rule.rule_id == _rule_len3().rule_id)
    assert updated.support == 2
    assert updated.pca_confidence == 1.0
    assert updated.head_coverage == 1.0
    assert repo.full_calls == 0
    assert seen["factual_only"] is False
    assert "bob" in seen["node_keys"]
    assert store.stats[updated.rule_id].pca_denominator == 2
    assert store.stats[updated.rule_id].head_count == 2


def test_length3_factual_only_ignores_inferred_removal():
    repo = _Repo()
    repo.length3_neighborhood_edges = lambda **kwargs: (_ for _ in ()).throw(AssertionError("neighborhood"))
    store = _Store(rule_ids=[_rule_len3().rule_id])
    store.stats[_rule_len3().rule_id] = _Stat(1, 1, 1)
    store.consume_delta = lambda limit=2000: DeltaBatch(
        added_edges=[],
        removed_edges=[ChangeEdge(src="bob", rel="bornIn", dst="shanghai", is_inferred=True)],
        cursor=9,
    )
    service = IncrementalMiningService(_Miner(repo), _RuleStore(_rule_len3()), store)
    result = service.run_incremental(
        MineRulesRequest(limit=10, min_support=0, min_pca_confidence=0.0, factual_only=True, body_length=3),
        body_length=3,
    )
    updated = next(rule for rule in result.rules if rule.rule_id == _rule_len3().rule_id)
    assert updated.support == 1
    assert repo.full_calls == 0


def test_service_recomputes_length3_when_neighborhood_is_unavailable():
    class Repo:
        def __init__(self) -> None:
            self.factual_only = None

        def compute_length3_rule_metrics(self, r1, r2, r3, head_rel, factual_only=True):
            _ = (r1, r2, r3, head_rel)
            self.factual_only = factual_only
            return {"support": 4, "pca_denominator": 5, "head_count": 6}

    repo = Repo()
    store = _Store(rule_ids=[_rule_len3().rule_id])
    store.stats[_rule_len3().rule_id] = _Stat(1, 1, 1)
    service = IncrementalMiningService(_Miner(repo), _RuleStore(_rule_len3()), store)
    result = service.run_incremental(
        MineRulesRequest(limit=10, min_support=0, min_pca_confidence=0.0, factual_only=False, body_length=3),
        body_length=3,
    )
    updated = result.rules[0]
    assert updated.support == 4
    assert repo.factual_only is False
    assert store.stats[updated.rule_id].pca_denominator == 5
    assert store.stats[updated.rule_id].head_count == 6
