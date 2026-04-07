from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient
import pytest

import neo_infer.api as api_module
from neo_infer.models import ChangeEdge, ConflictCase, DeltaBatch, Rule


@pytest.fixture()
def client_and_state(monkeypatch: pytest.MonkeyPatch):
    state: dict[str, Any] = {
        "rules": {},
        "conflict_pairs": {},
        "conflict_cases": [],
        "apply_counts": {},
    }

    class FakeDB:
        driver = object()

        def run_write(self, query: str, parameters: dict[str, Any] | None = None):
            _ = query
            _ = parameters
            state.setdefault("schema_statements", []).append(query)
            return []

        def close(self):
            return None

    class FakeQueryRepository:
        def __init__(self, driver: object, database: str = "neo4j") -> None:
            _ = driver
            _ = database

        def apply_length2_rule(self, rule: Rule) -> int:
            key = ("len2", rule.rule_id)
            seen = state["apply_counts"].get(key, 0)
            state["apply_counts"][key] = seen + 1
            return 1 if seen == 0 else 0

        def apply_length3_rule(self, rule: Rule) -> int:
            key = ("len3", rule.rule_id)
            seen = state["apply_counts"].get(key, 0)
            state["apply_counts"][key] = seen + 1
            return 1 if seen == 0 else 0

        def count_conflicts_for_rule(self, rule: Rule, negative_relation: str) -> int:
            rels = state["conflict_pairs"].get(rule.head_relation, set())
            base = 1 if negative_relation in rels else 0
            if len(rule.body_relations) == 3:
                return base + 1
            return base

        def compute_length2_rule_metrics(self, r1: str, r2: str, head_rel: str) -> dict[str, int]:
            _ = (r1, r2, head_rel)
            return {"support": 2, "pca_denominator": 2, "head_count": 2}

        def compute_length3_rule_metrics(self, r1: str, r2: str, r3: str, head_rel: str) -> dict[str, int]:
            _ = (r1, r2, r3, head_rel)
            return {"support": 1, "pca_denominator": 1, "head_count": 1}

    class FakeRuleMiningService:
        def __init__(self, repository: FakeQueryRepository) -> None:
            self._repository = repository

        def _rule_len2(self) -> Rule:
            return Rule(
                rule_id="rule__bornin__locatedin__to__nationality",
                body_relations=("bornIn", "locatedIn"),
                head_relation="nationality",
                support=2,
                pca_confidence=1.0,
                head_coverage=1.0,
                status="discovered",
                version=1,
            )

        def _rule_len3(self) -> Rule:
            return Rule(
                rule_id="rule__bornin__locatedin__partof__to__region",
                body_relations=("bornIn", "locatedIn", "partOf"),
                head_relation="region",
                support=1,
                pca_confidence=1.0,
                head_coverage=1.0,
                status="discovered",
                version=1,
            )

        def mine_length2_rules(self, config: Any) -> list[Rule]:
            _ = config
            return [self._rule_len2()]

        def mine_length3_rules(self, config: Any) -> list[Rule]:
            _ = config
            return [self._rule_len3()]

        def mine_length2_rules_incremental(self, config: Any, affected_relations: list[str]) -> list[Rule]:
            _ = config
            rels = set(affected_relations)
            if rels.intersection({"bornIn", "locatedIn", "nationality"}):
                return [self._rule_len2()]
            return []

        def mine_length3_rules_incremental(self, config: Any, affected_relations: list[str]) -> list[Rule]:
            _ = config
            rels = set(affected_relations)
            if rels.intersection({"bornIn", "locatedIn", "partOf", "region"}):
                return [self._rule_len3()]
            return []

        def mine_rules(self, config: Any) -> list[Rule]:
            changed = getattr(config, "changed_relations", None)
            body_length = int(getattr(config, "body_length", 2))
            if body_length == 3:
                if changed:
                    return self.mine_length3_rules_incremental(config, list(changed))
                return self.mine_length3_rules(config)
            if changed:
                return self.mine_length2_rules_incremental(config, list(changed))
            return self.mine_length2_rules(config)

    class FakeRuleStore:
        def __init__(self, db: FakeDB) -> None:
            _ = db

        def upsert_rules(self, rules: list[Rule]) -> None:
            for rule in rules:
                prev = state["rules"].get(rule.rule_id)
                if prev is None:
                    state["rules"][rule.rule_id] = rule.model_copy(deep=True)
                else:
                    merged = rule.model_copy(
                        update={
                            "status": prev.status,
                            "version": prev.version,
                        }
                    )
                    state["rules"][rule.rule_id] = merged

        def list_rules(self, status: str | None = None, limit: int = 100) -> list[Rule]:
            rules = list(state["rules"].values())
            if status is not None:
                rules = [item for item in rules if item.status == status]
            return rules[:limit]

        def update_rule_status(self, rule_id: str, status: str) -> bool:
            rule = state["rules"].get(rule_id)
            if rule is None:
                return False
            state["rules"][rule_id] = rule.model_copy(update={"status": status})
            return True

        def bump_rule_version(self, rule_id: str) -> bool:
            rule = state["rules"].get(rule_id)
            if rule is None:
                return False
            state["rules"][rule_id] = rule.model_copy(update={"version": rule.version + 1})
            return True

        def replace_rules(self, rules: list[Rule]) -> None:
            state["rules"] = {rule.rule_id: rule.model_copy(deep=True) for rule in rules}

        def list_rules_by_ids(self, rule_ids: list[str]) -> list[Rule]:
            return [state["rules"][rid] for rid in rule_ids if rid in state["rules"]]

    class FakeConflictStore:
        def __init__(self, db: FakeDB) -> None:
            _ = db

        def list_pairs(self) -> dict[str, set[str]]:
            return state["conflict_pairs"]

        def replace_pairs(self, pairs: dict[str, list[str]]) -> None:
            state["conflict_pairs"] = {k: set(v) for k, v in pairs.items()}

        def upsert_pair(self, head_relation: str, conflict_relation: str) -> None:
            state["conflict_pairs"].setdefault(head_relation, set()).add(conflict_relation)

        def delete_pair(self, head_relation: str, conflict_relation: str) -> bool:
            rels = state["conflict_pairs"].get(head_relation)
            if not rels or conflict_relation not in rels:
                return False
            rels.remove(conflict_relation)
            return True

        def record_conflict_cases(self, rule: Rule, negative_relation: str, iteration: int) -> int:
            case = ConflictCase(
                rule_id=rule.rule_id,
                inferred_relation=rule.head_relation,
                conflicting_relation=negative_relation,
                source_x="x-1",
                source_y="y-1",
                detect_count=1,
                first_iteration=iteration,
                last_iteration=iteration,
            )
            state["conflict_cases"].append(case)
            return 1

        def list_conflict_cases(self, limit: int = 200) -> list[ConflictCase]:
            return state["conflict_cases"][:limit]

    monkeypatch.setattr(api_module, "QueryRepository", FakeQueryRepository)
    monkeypatch.setattr(api_module, "RuleMiningService", FakeRuleMiningService)
    monkeypatch.setattr(api_module, "RuleStore", FakeRuleStore)
    monkeypatch.setattr(api_module, "ConflictStore", FakeConflictStore)

    class FakeDelta:
        def __init__(self, op: str, src: str, rel: str, dst: str) -> None:
            self.op = op
            self.src = src
            self.rel = rel
            self.dst = dst

    class FakeIncrementalStore:
        def __init__(self, db: FakeDB) -> None:
            _ = db
            state.setdefault("changelog", [])
            state.setdefault("cursor", 0)
            state.setdefault("stats", {})

        def consume_delta(self, limit: int = 2000) -> DeltaBatch:
            _ = limit
            return state.get("delta", DeltaBatch(added_edges=[], removed_edges=[], cursor=int(state.get("cursor", 0))))

        def affected_rule_ids(self, relations: set[str], limit: int = 5000):
            _ = limit
            if not relations:
                return []
            found: list[str] = []
            for rid, rule in state["rules"].items():
                touched = set(rule.body_relations) | {rule.head_relation}
                if touched.intersection(relations):
                    found.append(rid)
            return found

        def get_rule_stat(self, rule_id: str):
            data = state["stats"].get(rule_id)
            if data is None:
                return None
            return type("RuleStatObj", (), data)()

        def append_changes(self, changes):
            state["changelog"].extend(changes)

        def fetch_unprocessed_changes(self):
            idx = state["cursor"]
            return state["changelog"][idx:]

        def mark_changes_processed(self, count: int):
            state["cursor"] += count

        def set_rule_stats(self, rule_id: str, support: int, pca_denominator: int, head_count: int):
            state["stats"][rule_id] = {
                "support": support,
                "pca_denominator": pca_denominator,
                "head_count": head_count,
            }

        def get_rule_stats(self, rule_id: str):
            return state["stats"].get(rule_id)

        def clear_rule_stats(self):
            state["stats"] = {}

        def update_rule_indexes(self, rules):
            _ = rules

        def update_rule_stats(self, rules):
            for rule in rules:
                pca_denominator = int(round(rule.support / rule.pca_confidence)) if rule.pca_confidence > 0 else 0
                head_count = int(round(rule.support / rule.head_coverage)) if rule.head_coverage > 0 else 0
                state["stats"][rule.rule_id] = {
                    "rule_id": rule.rule_id,
                    "support": int(rule.support),
                    "pca_denominator": pca_denominator,
                    "head_count": head_count,
                }

        def mark_consumed(self, cursor: int):
            state["cursor"] = int(cursor)

    class FakeIncrementalMiner:
        def __init__(self, repository, rule_store, incremental_store) -> None:
            _ = repository
            _ = rule_store
            _ = incremental_store

        def consume_changelog_length2(self, config):
            _ = config
            # 模拟：消费日志后返回一个规则
            return [
                Rule(
                    rule_id="rule__bornin__locatedin__to__nationality",
                    body_relations=("bornIn", "locatedIn"),
                    head_relation="nationality",
                    support=3,
                    pca_confidence=1.0,
                    head_coverage=1.0,
                    status="discovered",
                    version=1,
                )
            ]

        def consume_changelog_length3(self, config):
            _ = config
            return [
                Rule(
                    rule_id="rule__bornin__locatedin__partof__to__region",
                    body_relations=("bornIn", "locatedIn", "partOf"),
                    head_relation="region",
                    support=2,
                    pca_confidence=1.0,
                    head_coverage=1.0,
                    status="discovered",
                    version=1,
                )
            ]

        def discover_new_rules_from_delta_length2(self, config):
            _ = config
            return []

        def discover_new_rules_from_delta_length3(self, config):
            _ = config
            return []

    monkeypatch.setattr(api_module, "IncrementalStore", FakeIncrementalStore)
    if hasattr(api_module, "IncrementalMiner"):
        monkeypatch.setattr(api_module, "IncrementalMiner", FakeIncrementalMiner)
    if hasattr(api_module, "DeltaEdge"):
        monkeypatch.setattr(
            api_module,
            "DeltaEdge",
            FakeDelta,
        )
    monkeypatch.setattr(api_module, "Neo4jClient", lambda settings: FakeDB())
    api_module.app.dependency_overrides[api_module.get_db] = lambda: FakeDB()

    with TestClient(api_module.app) as client:
        yield client, state

    api_module.app.dependency_overrides.clear()


def test_rules_mine_and_inference_smoke(client_and_state):
    client, _state = client_and_state

    mine_resp = client.post(
        "/rules/mine",
        json={"body_length": 2, "limit": 50, "min_support": 1, "min_pca_confidence": 0.1},
    )
    assert mine_resp.status_code == 200
    mined = mine_resp.json()["rules"]
    assert mined
    rule_id = mined[0]["rule_id"]

    list_resp = client.get("/rules", params={"status": "discovered", "limit": 100})
    assert list_resp.status_code == 200
    assert any(item["rule_id"] == rule_id for item in list_resp.json()["rules"])

    adopt_resp = client.post(f"/rules/{rule_id}/adopt")
    assert adopt_resp.status_code == 200
    assert adopt_resp.json()["status"] == "adopted"

    infer_resp = client.post(
        "/inference/run",
        json={"limit_rules": 10, "fixpoint": False, "max_iterations": 3, "check_conflicts": False},
    )
    assert infer_resp.status_code == 200
    payload = infer_resp.json()
    assert payload["iterations_run"] == 1
    assert payload["total_created"] >= 0
    assert payload["results"]


def test_incremental_mining_smoke(client_and_state):
    client, _state = client_and_state

    resp_len2 = client.post(
        "/rules/mine/incremental/length2",
        json={
            "affected_relations": ["bornIn", "locatedIn"],
            "limit": 50,
            "min_support": 1,
            "min_pca_confidence": 0.1,
        },
    )
    assert resp_len2.status_code == 200
    assert resp_len2.json()["rules"]

    resp_len3 = client.post(
        "/rules/mine/incremental/length3",
        json={
            "affected_relations": ["partOf"],
            "limit": 50,
            "min_support": 1,
            "min_pca_confidence": 0.1,
        },
    )
    assert resp_len3.status_code == 200
    assert resp_len3.json()["rules"]
    assert len(resp_len3.json()["rules"][0]["body_relations"]) == 3


def test_conflicts_and_conflict_cases_smoke(client_and_state):
    client, _state = client_and_state

    put_resp = client.put("/conflicts", json={"pairs": {"nationality": ["noNationality"]}})
    assert put_resp.status_code == 200
    assert put_resp.json()["pairs"]["nationality"] == ["noNationality"]

    get_resp = client.get("/conflicts")
    assert get_resp.status_code == 200
    assert get_resp.json()["pairs"]["nationality"] == ["noNationality"]

    mine_resp = client.post(
        "/rules/mine",
        json={"body_length": 2, "limit": 10, "min_support": 1, "min_pca_confidence": 0.1},
    )
    rule_id = mine_resp.json()["rules"][0]["rule_id"]
    client.post(f"/rules/{rule_id}/adopt")

    infer_resp = client.post(
        "/inference/run",
        json={"limit_rules": 10, "fixpoint": False, "check_conflicts": True},
    )
    assert infer_resp.status_code == 200
    assert infer_resp.json()["total_conflicts"] >= 1

    cases_resp = client.get("/conflicts/cases", params={"limit": 50})
    assert cases_resp.status_code == 200
    assert len(cases_resp.json()["cases"]) >= 1

    alias_resp = client.get("/conflict-cases", params={"limit": 50})
    assert alias_resp.status_code == 200
    assert len(alias_resp.json()["cases"]) >= 1


def test_true_incremental_mining_from_changelog_smoke(client_and_state):
    client, _state = client_and_state
    # 先确保已有规则注册，便于增量更新命中 relation_to_rules。
    mine_resp = client.post(
        "/rules/mine",
        json={"body_length": 2, "limit": 10, "min_support": 1, "min_pca_confidence": 0.1},
    )
    assert mine_resp.status_code == 200

    # 记录新增边到 changelog
    log_resp = client.post(
        "/changes/log",
        json={
            "added_edges": [
                {"src_id": "n1", "rel": "bornIn", "dst_id": "n2"},
                {"src_id": "n2", "rel": "locatedIn", "dst_id": "n3"},
            ],
            "removed_edges": [],
        },
    )
    assert log_resp.status_code == 200
    assert log_resp.json()["added"] == 2

    run_resp = client.post(
        "/rules/mine/incremental/changelog",
        json={"body_length": 2, "limit": 50, "min_support": 1, "min_pca_confidence": 0.0},
    )
    assert run_resp.status_code == 200
    payload = run_resp.json()
    assert "rules" in payload

def test_incremental_changelog_flow_smoke(client_and_state):
    client, _state = client_and_state
    append_resp = client.post(
        "/changelog/append",
        json={
            "changes": [
                {"op": "add", "src": "n1", "rel": "bornIn", "dst": "n2"},
                {"op": "add", "src": "n2", "rel": "locatedIn", "dst": "n3"},
            ]
        },
    )
    assert append_resp.status_code == 200
    assert append_resp.json()["appended"] == 2

    run_resp = client.post(
        "/rules/mine/incremental/consume/length2",
        json={"limit": 50, "min_support": 1, "min_pca_confidence": 0.1},
    )
    assert run_resp.status_code == 200
    assert run_resp.json()["rules"]


def test_incremental_from_changelog_empty_delta_contract(client_and_state):
    client, _state = client_and_state
    resp = client.post(
        "/rules/mine/incremental/from-changelog",
        json={"limit": 100, "min_support": 1, "min_pca_confidence": 0.1, "body_length": 2},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["processed_changes"] == 0
    assert payload["affected_relations"] == []
    assert payload["rules"] == []


def test_incremental_from_changelog_updates_existing_length2_rule(client_and_state):
    client, state = client_and_state
    state.setdefault("stats", {})
    existing_rule = Rule(
        rule_id="rule__bornin__locatedin__to__nationality",
        body_relations=("bornIn", "locatedIn"),
        head_relation="nationality",
        support=1,
        pca_confidence=0.5,
        head_coverage=0.5,
        status="discovered",
        version=1,
    )
    state["rules"][existing_rule.rule_id] = existing_rule
    state["stats"][existing_rule.rule_id] = {
        "rule_id": existing_rule.rule_id,
        "support": 1,
        "pca_denominator": 2,
        "head_count": 2,
    }
    state["delta"] = DeltaBatch(
        added_edges=[{"src": "n1", "rel": "bornIn", "dst": "n2"}],
        removed_edges=[],
        cursor=1,
    )

    resp = client.post(
        "/rules/mine/incremental/from-changelog",
        json={
            "limit": 100,
            "min_support": 1,
            "min_pca_confidence": 0.0,
            "body_length": 2,
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["processed_changes"] == 1
    assert "bornIn" in payload["affected_relations"]
    assert any(item["rule_id"] == existing_rule.rule_id for item in payload["rules"])


def test_incremental_length3_uses_incremental_candidates(client_and_state):
    client, state = client_and_state
    state["delta"] = DeltaBatch(
        added_edges=[{"src": "n2", "rel": "partOf", "dst": "n3"}],
        removed_edges=[],
        cursor=2,
    )
    resp = client.post(
        "/rules/mine/incremental/from-changelog",
        json={
            "limit": 100,
            "min_support": 1,
            "min_pca_confidence": 0.0,
            "body_length": 3,
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["processed_changes"] == 1
    assert "partOf" in payload["affected_relations"]
    assert payload["rules"]
    assert len(payload["rules"][0]["body_relations"]) == 3


def test_length3_conflict_detection_consistency_smoke(client_and_state):
    client, state = client_and_state
    rule = Rule(
        rule_id="rule__bornin__locatedin__partof__to__region",
        body_relations=("bornIn", "locatedIn", "partOf"),
        head_relation="region",
        support=1,
        pca_confidence=1.0,
        head_coverage=1.0,
        status="discovered",
        version=1,
    )
    state["rules"][rule.rule_id] = rule
    put_resp = client.put("/conflicts", json={"pairs": {"region": ["noRegion"]}})
    assert put_resp.status_code == 200
    client.post(f"/rules/{rule.rule_id}/adopt")
    infer_resp = client.post(
        "/inference/run",
        json={"limit_rules": 10, "fixpoint": False, "check_conflicts": True},
    )
    assert infer_resp.status_code == 200
    payload = infer_resp.json()
    assert payload["total_conflicts"] >= 1


def test_from_changelog_non_empty_mixed_and_idempotent_contract(client_and_state):
    client, state = client_and_state
    state.setdefault("stats", {})
    state["delta"] = DeltaBatch(
        added_edges=[ChangeEdge(src="n1", rel="bornIn", dst="n2")],
        removed_edges=[ChangeEdge(src="n2", rel="locatedIn", dst="n3")],
        cursor=5,
    )
    first = client.post(
        "/rules/mine/incremental/from-changelog",
        json={
            "limit": 100,
            "min_support": 1,
            "min_pca_confidence": 0.0,
            "body_length": 2,
        },
    )
    assert first.status_code == 200
    p1 = first.json()
    assert p1["processed_changes"] == 2
    assert sorted(p1["affected_relations"]) == ["bornIn", "locatedIn"]
    assert isinstance(p1["rules"], list)

    state["delta"] = DeltaBatch(added_edges=[], removed_edges=[], cursor=5)
    second = client.post(
        "/rules/mine/incremental/from-changelog",
        json={
            "limit": 100,
            "min_support": 1,
            "min_pca_confidence": 0.0,
            "body_length": 2,
        },
    )
    assert second.status_code == 200
    p2 = second.json()
    assert p2["processed_changes"] == 0
    assert p2["affected_relations"] == []
    assert p2["rules"] == []


def test_schema_bootstrap_is_called(client_and_state):
    client, state = client_and_state
    _ = client
    executed = state.get("schema_statements", [])
    # Startup should attempt to create all schema indexes/constraints.
    assert len(executed) >= 1
    assert any("CREATE CONSTRAINT rule_rule_id_unique" in stmt for stmt in executed)
    assert any("CREATE CONSTRAINT changelog_change_seq_unique" in stmt for stmt in executed)

