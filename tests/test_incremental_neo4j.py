"""Live Neo4j checks for length-2 and length-3 event counters.

The neighborhood Cypher and the stored changelog are compared with a full
recount on the same graph. The module skips when bolt is closed so the
default suite still runs without a database.
"""

from __future__ import annotations

import os
import re
import uuid

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from neo_infer.api import ensure_neo4j_schema
from neo_infer.config import Settings
from neo_infer.db import Neo4jClient
from neo_infer.incremental_counters import length2_stat_delta, length3_stat_delta
from neo_infer.incremental_mining import IncrementalMiningService
from neo_infer.incremental_store import IncrementalStore
from neo_infer.models import ChangeEdge, MineRulesRequest, Rule, build_rule_id
from neo_infer.query import QueryRepository
from neo_infer.rule_management import RuleStore
from neo_infer.rule_mining import RuleMiningService

_REL = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
BORN = "ItBornIn"
LOC = "ItLocatedIn"
NAT = "ItNationality"
PART = "ItPartOf"
REGION = "ItRegion"


def _settings() -> Settings:
    return Settings(
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "neo4jpass"),
        neo4j_database=os.getenv("NEO4J_DATABASE", "neo4j"),
    )


def _reachable() -> bool:
    settings = _settings()
    try:
        driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
            connection_timeout=2,
        )
        driver.verify_connectivity()
        driver.close()
        return True
    except (ServiceUnavailable, OSError, Exception):
        return False


# NEO4J_IT=1 makes a closed bolt port a failure instead of a skip, so CI cannot
# pass while these checks are silently omitted.
pytestmark = pytest.mark.skipif(
    os.getenv("NEO4J_IT") != "1" and not _reachable(),
    reason="Neo4j is not reachable",
)


class LiveGraph:
    def __init__(self, client: Neo4jClient) -> None:
        self.client = client
        self.repo = QueryRepository(client.driver, database=client.settings.neo4j_database)
        self.store = IncrementalStore(client)
        self.rules = RuleStore(client)

    def reset(self) -> None:
        self.client.run_write("MATCH (n:ItEntity) DETACH DELETE n")
        self.client.run_write("MATCH (c:ChangeLog) WHERE c.source = 'it' DETACH DELETE c")
        tip = self.client.run_read("MATCH (c:ChangeLog) RETURN max(toInteger(c.change_seq)) AS seq")
        seq = int(tip[0]["seq"] or 0) if tip else 0
        self.store.set_cursor(seq)

    def append(self, added: list[ChangeEdge], removed: list[ChangeEdge] | None = None) -> None:
        self.store.append_changes(
            added,
            removed or [],
            source="it",
            batch_id=f"it-{uuid.uuid4()}",
        )

    def add(self, src: str, rel: str, dst: str, *, inferred: bool = False) -> ChangeEdge:
        if not _REL.match(rel):
            raise AssertionError(rel)
        self.client.run_write(
            f"""
            MERGE (a:ItEntity {{id: $src}})
            MERGE (b:ItEntity {{id: $dst}})
            MERGE (a)-[r:`{rel}`]->(b)
            SET r.is_inferred = $inferred
            """,
            {"src": src, "dst": dst, "inferred": inferred},
        )
        return ChangeEdge(src=src, rel=rel, dst=dst, is_inferred=inferred)

    def remove(self, src: str, rel: str, dst: str, *, inferred: bool = False) -> ChangeEdge:
        if not _REL.match(rel):
            raise AssertionError(rel)
        self.client.run_write(
            f"""
            MATCH (a:ItEntity {{id: $src}})-[r:`{rel}`]->(b:ItEntity {{id: $dst}})
            DELETE r
            """,
            {"src": src, "dst": dst},
        )
        return ChangeEdge(src=src, rel=rel, dst=dst, is_inferred=inferred)

    def metrics(self, body: tuple[str, ...], head: str, *, factual_only: bool) -> dict[str, int]:
        return self.repo.compute_rule_metrics(body, head, factual_only=factual_only)

    def assert_delta(
        self,
        body: tuple[str, ...],
        head: str,
        before: dict[str, int],
        added: list[ChangeEdge],
        removed: list[ChangeEdge],
        *,
        factual_only: bool,
    ) -> dict[str, int]:
        counted_added = [edge for edge in added if not (factual_only and edge.is_inferred)]
        counted_removed = [edge for edge in removed if not (factual_only and edge.is_inferred)]
        keys = [item for edge in (*counted_added, *counted_removed) for item in (edge.src, edge.dst)]
        added_tuples = [(edge.src, edge.rel, edge.dst) for edge in counted_added]
        removed_tuples = [(edge.src, edge.rel, edge.dst) for edge in counted_removed]
        if len(body) == 2 and keys:
            present = {
                (src, rel, dst)
                for src, rel, dst in self.repo.length2_neighborhood_edges(
                    r1=body[0],
                    r2=body[1],
                    head_rel=head,
                    node_keys=keys,
                    factual_only=factual_only,
                )
            }
            delta = length2_stat_delta(
                r1=body[0],
                r2=body[1],
                head=head,
                present=present,
                added=added_tuples,
                removed=removed_tuples,
            )
        elif len(body) == 3 and keys:
            present = {
                (src, rel, dst)
                for src, rel, dst in self.repo.length3_neighborhood_edges(
                    r1=body[0],
                    r2=body[1],
                    r3=body[2],
                    head_rel=head,
                    node_keys=keys,
                    factual_only=factual_only,
                )
            }
            delta = length3_stat_delta(
                r1=body[0],
                r2=body[1],
                r3=body[2],
                head=head,
                present=present,
                added=added_tuples,
                removed=removed_tuples,
            )
        else:
            delta = (0, 0, 0)
        after = self.metrics(body, head, factual_only=factual_only)
        assert after["support"] == before["support"] + delta[0]
        assert after["pca_denominator"] == before["pca_denominator"] + delta[1]
        assert after["head_count"] == before["head_count"] + delta[2]
        return after


@pytest.fixture()
def graph() -> LiveGraph:
    client = Neo4jClient(_settings())
    ensure_neo4j_schema(client)
    live = LiveGraph(client)
    live.reset()
    try:
        yield live
    finally:
        live.reset()
        client.close()


def test_length2_event_deltas_match_full_recount(graph: LiveGraph):
    body = (BORN, LOC)
    head = NAT
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-china")
    graph.add("it-alice", NAT, "it-china")
    current = graph.metrics(body, head, factual_only=True)
    assert current == {"support": 1, "pca_denominator": 1, "head_count": 1}

    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-shanghai", LOC, "it-china")],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 1

    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-bob", NAT, "it-china")],
        removed=[],
        factual_only=True,
    )
    assert current["head_count"] == 2
    assert current["support"] == 1

    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-bob", BORN, "it-shanghai")],
        removed=[],
        factual_only=True,
    )
    assert current == {"support": 2, "pca_denominator": 2, "head_count": 2}

    # A second body path to the same head pair must not increase support.
    # Dropping one of those paths must keep support while the other remains.
    current = graph.assert_delta(
        body, head, current,
        added=[
            graph.add("it-alice", BORN, "it-tianjin"),
            graph.add("it-tianjin", LOC, "it-china"),
        ],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 2
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-beijing", LOC, "it-china")],
        factual_only=True,
    )
    assert current["support"] == 2
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-alice", BORN, "it-tianjin")],
        factual_only=True,
    )
    assert current["support"] == 1

    # First head edge from a node pulls every body pair into the PCA denominator.
    # The next head edge from that node does not.
    current = graph.assert_delta(
        body, head, current,
        added=[
            graph.add("it-carol", BORN, "it-osaka"),
            graph.add("it-osaka", LOC, "it-japan"),
            graph.add("it-carol", BORN, "it-seoul"),
            graph.add("it-seoul", LOC, "it-korea"),
        ],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 1
    pca_before_head = current["pca_denominator"]
    support_before_head = current["support"]
    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-carol", NAT, "it-japan")],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == support_before_head + 1
    assert current["pca_denominator"] == pca_before_head + 2
    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-carol", NAT, "it-korea")],
        removed=[],
        factual_only=True,
    )
    assert current["pca_denominator"] == pca_before_head + 2
    assert current["support"] == support_before_head + 2
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-carol", NAT, "it-japan")],
        factual_only=True,
    )
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-carol", NAT, "it-korea")],
        factual_only=True,
    )
    assert current["support"] == 1
    assert current["head_count"] == 2


def test_same_relation_on_both_body_atoms(graph: LiveGraph):
    body = (LOC, LOC)
    head = NAT
    before = graph.metrics(body, head, factual_only=True)
    after = graph.assert_delta(
        body, head, before,
        added=[
            graph.add("it-beijing", LOC, "it-hebei"),
            graph.add("it-hebei", LOC, "it-china"),
            graph.add("it-beijing", NAT, "it-china"),
        ],
        removed=[],
        factual_only=True,
    )
    assert after["support"] == before["support"] + 1
    after = graph.assert_delta(
        body, head, after,
        added=[graph.add("it-hebei", LOC, "it-asia")],
        removed=[],
        factual_only=True,
    )
    # beijing-hebei-asia is a new body pair, but beijing has no nationality to asia.
    assert after["support"] == before["support"] + 1


def test_factual_only_matches_full_recount_for_inferred_edges(graph: LiveGraph):
    body = (BORN, LOC)
    head = NAT
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-china")
    graph.add("it-alice", NAT, "it-china")
    factual = graph.metrics(body, head, factual_only=True)
    included = graph.metrics(body, head, factual_only=False)
    assert factual == included

    inferred = graph.add("it-bob", BORN, "it-shanghai", inferred=True)
    bridge = graph.add("it-shanghai", LOC, "it-china")
    head_edge = graph.add("it-bob", NAT, "it-china")
    factual_after = graph.assert_delta(
        body, head, factual,
        added=[inferred, bridge, head_edge],
        removed=[],
        factual_only=True,
    )
    # The body edge is inferred, so support and PCA stay put. The head edge is factual.
    assert factual_after["support"] == factual["support"]
    assert factual_after["pca_denominator"] == factual["pca_denominator"]
    assert factual_after["head_count"] == factual["head_count"] + 1
    included_after = graph.assert_delta(
        body, head, included,
        added=[inferred, bridge, head_edge],
        removed=[],
        factual_only=False,
    )
    assert included_after["support"] == included["support"] + 1

    # The neighborhood itself must hide the inferred body edge.
    present = graph.repo.length2_neighborhood_edges(
        r1=BORN,
        r2=LOC,
        head_rel=NAT,
        node_keys=["it-bob", "it-shanghai", "it-china"],
        factual_only=True,
    )
    assert ("it-bob", BORN, "it-shanghai") not in present
    present_all = graph.repo.length2_neighborhood_edges(
        r1=BORN,
        r2=LOC,
        head_rel=NAT,
        node_keys=["it-bob", "it-shanghai", "it-china"],
        factual_only=False,
    )
    assert ("it-bob", BORN, "it-shanghai") in present_all

    before_remove = graph.metrics(body, head, factual_only=True)
    removed = graph.remove("it-bob", BORN, "it-shanghai", inferred=True)
    after_remove = graph.assert_delta(
        body, head, before_remove,
        added=[],
        removed=[removed],
        factual_only=True,
    )
    assert after_remove == before_remove


def test_element_id_endpoints_match_full_recount(graph: LiveGraph):
    body = (BORN, LOC)
    head = NAT
    with graph.client.driver.session(database=graph.client.settings.neo4j_database) as session:
        created = session.run(
            f"""
            CREATE (a:ItEntity {{token: 'src'}})
            CREATE (b:ItEntity {{token: 'mid'}})
            CREATE (c:ItEntity {{token: 'dst'}})
            CREATE (a)-[:`{BORN}`]->(b)
            CREATE (b)-[:`{LOC}`]->(c)
            CREATE (a)-[:`{NAT}`]->(c)
            RETURN elementId(a) AS a, elementId(b) AS b, elementId(c) AS c
            """
        ).single()
    assert created is not None
    before = graph.metrics(body, head, factual_only=True)
    with graph.client.driver.session(database=graph.client.settings.neo4j_database) as session:
        extra = session.run(
            f"""
            MATCH (a:ItEntity {{token: 'src'}}), (c:ItEntity {{token: 'dst'}})
            CREATE (m:ItEntity {{token: 'mid2'}})
            CREATE (a)-[:`{BORN}`]->(m)
            CREATE (m)-[:`{LOC}`]->(c)
            RETURN elementId(a) AS a, elementId(m) AS m, elementId(c) AS c
            """
        ).single()
    assert extra is not None
    after = graph.assert_delta(
        body, head, before,
        added=[
            ChangeEdge(src=str(extra["a"]), rel=BORN, dst=str(extra["m"])),
            ChangeEdge(src=str(extra["m"]), rel=LOC, dst=str(extra["c"])),
        ],
        removed=[],
        factual_only=True,
    )
    assert after["support"] == before["support"]


def _seed(graph: LiveGraph, body: tuple[str, ...], head: str, *, factual_only: bool) -> Rule:
    metrics = graph.metrics(body, head, factual_only=factual_only)
    denom = metrics["pca_denominator"]
    heads = metrics["head_count"]
    rule = Rule(
        rule_id=build_rule_id(body, head),
        body_relations=body,
        head_relation=head,
        support=metrics["support"],
        pca_confidence=(metrics["support"] / denom) if denom else 0.0,
        head_coverage=(metrics["support"] / heads) if heads else 0.0,
        status="discovered",
        version=1,
    )
    graph.rules.upsert_rules([rule])
    graph.store.upsert_rule_stats(rule, metrics["support"], denom, heads)
    graph.store.set_rule_relations(rule)
    return rule


def _run(graph: LiveGraph, *, body_length: int, factual_only: bool):
    preexisting = graph.client.run_read("MATCH ()-[r]->() RETURN count(r) AS n")
    count = int(preexisting[0]["n"]) if preexisting else 0
    miner = RuleMiningService(graph.repo)
    if count > 2000:
        miner.mine_rules = lambda config: []  # type: ignore[method-assign]
    service = IncrementalMiningService(miner, graph.rules, graph.store)
    return service.run_incremental(
        MineRulesRequest(
            limit=50,
            min_support=0,
            min_pca_confidence=0.0,
            min_head_coverage=0.0,
            factual_only=factual_only,
            body_length=body_length,
        ),
        body_length=body_length,
    )


def test_service_persists_length2_delta_on_neo4j(graph: LiveGraph):
    body = (BORN, LOC)
    head = NAT
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-china")
    graph.add("it-alice", NAT, "it-china")
    rule = _seed(graph, body, head, factual_only=True)

    added = [
        graph.add("it-bob", BORN, "it-shanghai"),
        graph.add("it-shanghai", LOC, "it-china"),
        graph.add("it-bob", NAT, "it-china"),
    ]
    graph.append(added)
    result = _run(graph, body_length=2, factual_only=True)
    stored = graph.store.get_rule_stat(rule.rule_id)
    full = graph.metrics(body, head, factual_only=True)
    assert stored is not None
    assert (stored.support, stored.pca_denominator, stored.head_count) == (
        full["support"],
        full["pca_denominator"],
        full["head_count"],
    )
    assert full["support"] == 2
    returned = next(item for item in result.rules if item.rule_id == rule.rule_id)
    assert returned.support == 2

    removed = [graph.remove("it-bob", BORN, "it-shanghai")]
    graph.append([], removed)
    _run(graph, body_length=2, factual_only=True)
    stored = graph.store.get_rule_stat(rule.rule_id)
    full = graph.metrics(body, head, factual_only=True)
    assert stored is not None
    assert stored.support == full["support"] == 1

    empty = _run(graph, body_length=2, factual_only=True)
    assert empty.rules == []
    assert empty.processed_events == 0


def test_changelog_roundtrip_folds_and_keeps_inferred_flag(graph: LiveGraph):
    edge = ChangeEdge(src="it-bob", rel=BORN, dst="it-shanghai", is_inferred=True)
    graph.append([edge], [edge])
    folded = graph.store.consume_delta()
    assert folded.added_edges == []
    assert folded.removed_edges == []
    graph.store.mark_consumed(folded.cursor)

    graph.append([], [edge])
    delta = graph.store.consume_delta()
    assert len(delta.removed_edges) == 1
    assert delta.removed_edges[0].is_inferred is True
    graph.store.mark_consumed(delta.cursor)


def test_length3_event_deltas_match_full_recount(graph: LiveGraph):
    body = (BORN, LOC, PART)
    head = REGION
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-hebei")
    graph.add("it-hebei", PART, "it-china")
    graph.add("it-alice", REGION, "it-china")
    current = graph.metrics(body, head, factual_only=True)
    assert current == {"support": 1, "pca_denominator": 1, "head_count": 1}

    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-shanghai", LOC, "it-zhejiang")],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 1

    current = graph.assert_delta(
        body, head, current,
        added=[
            graph.add("it-bob", BORN, "it-shanghai"),
            graph.add("it-zhejiang", PART, "it-china"),
        ],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 1
    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-bob", REGION, "it-china")],
        removed=[],
        factual_only=True,
    )
    assert current == {"support": 2, "pca_denominator": 2, "head_count": 2}

    # A second body path to the same head pair must not increase support.
    current = graph.assert_delta(
        body, head, current,
        added=[
            graph.add("it-bob", BORN, "it-hangzhou"),
            graph.add("it-hangzhou", LOC, "it-zhejiang"),
        ],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 2
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-shanghai", LOC, "it-zhejiang")],
        factual_only=True,
    )
    assert current["support"] == 2
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-bob", BORN, "it-hangzhou")],
        factual_only=True,
    )
    assert current["support"] == 1

    # Dropping the last middle edge removes alice's only path.
    current = graph.assert_delta(
        body, head, current,
        added=[],
        removed=[graph.remove("it-beijing", LOC, "it-hebei")],
        factual_only=True,
    )
    assert current["support"] == 0
    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-beijing", LOC, "it-hebei")],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 1

    # First head edge from a node pulls every body pair into the PCA denominator.
    current = graph.assert_delta(
        body, head, current,
        added=[
            graph.add("it-carol", BORN, "it-osaka"),
            graph.add("it-osaka", LOC, "it-kansai"),
            graph.add("it-kansai", PART, "it-japan"),
            graph.add("it-carol", BORN, "it-seoul"),
            graph.add("it-seoul", LOC, "it-gyeonggi"),
            graph.add("it-gyeonggi", PART, "it-korea"),
        ],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == 1
    pca_before_head = current["pca_denominator"]
    support_before_head = current["support"]
    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-carol", REGION, "it-japan")],
        removed=[],
        factual_only=True,
    )
    assert current["support"] == support_before_head + 1
    assert current["pca_denominator"] == pca_before_head + 2
    current = graph.assert_delta(
        body, head, current,
        added=[graph.add("it-carol", REGION, "it-korea")],
        removed=[],
        factual_only=True,
    )
    assert current["pca_denominator"] == pca_before_head + 2
    assert current["support"] == support_before_head + 2


def test_length3_same_relation_on_body_atoms(graph: LiveGraph):
    body = (LOC, LOC, LOC)
    head = NAT
    before = graph.metrics(body, head, factual_only=True)
    after = graph.assert_delta(
        body, head, before,
        added=[
            graph.add("it-beijing", LOC, "it-hebei"),
            graph.add("it-hebei", LOC, "it-china"),
            graph.add("it-china", LOC, "it-asia"),
            graph.add("it-beijing", NAT, "it-asia"),
        ],
        removed=[],
        factual_only=True,
    )
    assert after["support"] == before["support"] + 1
    after = graph.assert_delta(
        body, head, after,
        added=[
            graph.add("it-beijing", LOC, "it-tianjin"),
            graph.add("it-tianjin", LOC, "it-china"),
        ],
        removed=[],
        factual_only=True,
    )
    assert after["support"] == before["support"] + 1


def test_length3_element_id_endpoints_match_full_recount(graph: LiveGraph):
    body = (BORN, LOC, PART)
    head = REGION
    before = graph.metrics(body, head, factual_only=True)
    with graph.client.driver.session(database=graph.client.settings.neo4j_database) as session:
        created = session.run(
            f"""
            CREATE (a:ItEntity {{token: 'src'}})
            CREATE (b:ItEntity {{token: 'm1'}})
            CREATE (c:ItEntity {{token: 'm2'}})
            CREATE (d:ItEntity {{token: 'dst'}})
            CREATE (a)-[:`{BORN}`]->(b)
            CREATE (b)-[:`{LOC}`]->(c)
            CREATE (c)-[:`{PART}`]->(d)
            CREATE (a)-[:`{REGION}`]->(d)
            RETURN elementId(a) AS a, elementId(b) AS b, elementId(c) AS c, elementId(d) AS d
            """
        ).single()
    assert created is not None
    after = graph.assert_delta(
        body, head, before,
        added=[
            ChangeEdge(src=str(created["a"]), rel=BORN, dst=str(created["b"])),
            ChangeEdge(src=str(created["b"]), rel=LOC, dst=str(created["c"])),
            ChangeEdge(src=str(created["c"]), rel=PART, dst=str(created["d"])),
            ChangeEdge(src=str(created["a"]), rel=REGION, dst=str(created["d"])),
        ],
        removed=[],
        factual_only=True,
    )
    assert after["support"] == before["support"] + 1
    assert after["pca_denominator"] == before["pca_denominator"] + 1
    assert after["head_count"] == before["head_count"] + 1


def test_service_persists_length3_delta_on_neo4j(graph: LiveGraph):
    body = (BORN, LOC, PART)
    head = REGION
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-hebei")
    graph.add("it-hebei", PART, "it-china")
    graph.add("it-alice", REGION, "it-china")
    rule = _seed(graph, body, head, factual_only=True)

    added = [
        graph.add("it-bob", BORN, "it-shanghai"),
        graph.add("it-shanghai", LOC, "it-zhejiang"),
        graph.add("it-zhejiang", PART, "it-china"),
        graph.add("it-bob", REGION, "it-china"),
    ]
    graph.append(added)
    _run(graph, body_length=3, factual_only=True)
    stored = graph.store.get_rule_stat(rule.rule_id)
    full = graph.metrics(body, head, factual_only=True)
    assert stored is not None
    assert (stored.support, stored.pca_denominator, stored.head_count) == (
        full["support"],
        full["pca_denominator"],
        full["head_count"],
    )
    assert full["support"] == 2

    removed = [graph.remove("it-shanghai", LOC, "it-zhejiang")]
    graph.append([], removed)
    _run(graph, body_length=3, factual_only=True)
    stored = graph.store.get_rule_stat(rule.rule_id)
    full = graph.metrics(body, head, factual_only=True)
    assert stored is not None
    assert stored.support == full["support"] == 1


def test_length3_recompute_respects_factual_only(graph: LiveGraph):
    body = (BORN, LOC, PART)
    head = REGION
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-hebei")
    graph.add("it-hebei", PART, "it-china")
    graph.add("it-alice", REGION, "it-china")
    rule = _seed(graph, body, head, factual_only=True)
    before = graph.metrics(body, head, factual_only=True)
    assert before["support"] == 1

    inferred = [
        graph.add("it-bob", BORN, "it-shanghai", inferred=True),
        graph.add("it-shanghai", LOC, "it-zhejiang", inferred=True),
        graph.add("it-zhejiang", PART, "it-china", inferred=True),
        graph.add("it-bob", REGION, "it-china", inferred=True),
    ]
    graph.append(inferred)
    _run(graph, body_length=3, factual_only=True)
    stored = graph.store.get_rule_stat(rule.rule_id)
    factual = graph.metrics(body, head, factual_only=True)
    assert stored is not None
    assert (stored.support, stored.pca_denominator, stored.head_count) == (
        factual["support"],
        factual["pca_denominator"],
        factual["head_count"],
    )
    assert factual["support"] == 1
    assert graph.metrics(body, head, factual_only=False)["support"] == 2

    graph.reset()
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-hebei")
    graph.add("it-hebei", PART, "it-china")
    graph.add("it-alice", REGION, "it-china")
    rule = _seed(graph, body, head, factual_only=False)
    graph.append(
        [
            graph.add("it-bob", BORN, "it-shanghai", inferred=True),
            graph.add("it-shanghai", LOC, "it-zhejiang", inferred=True),
            graph.add("it-zhejiang", PART, "it-china", inferred=True),
            graph.add("it-bob", REGION, "it-china", inferred=True),
        ]
    )
    _run(graph, body_length=3, factual_only=False)
    stored = graph.store.get_rule_stat(rule.rule_id)
    included = graph.metrics(body, head, factual_only=False)
    assert stored is not None
    assert stored.support == included["support"] == 2


def test_missing_baseline_falls_back_to_full_recount(graph: LiveGraph):
    body = (BORN, LOC)
    head = NAT
    graph.add("it-alice", BORN, "it-beijing")
    graph.add("it-beijing", LOC, "it-china")
    graph.add("it-alice", NAT, "it-china")
    rule = _seed(graph, body, head, factual_only=True)
    graph.client.run_write(
        "MATCH (s:RuleStat {rule_id: $rule_id}) DETACH DELETE s",
        {"rule_id": rule.rule_id},
    )
    added = graph.add("it-bob", BORN, "it-shanghai")
    graph.add("it-shanghai", LOC, "it-china")
    graph.add("it-bob", NAT, "it-china")
    graph.append([added])
    _run(graph, body_length=2, factual_only=True)
    stored = graph.store.get_rule_stat(rule.rule_id)
    full = graph.metrics(body, head, factual_only=True)
    assert stored is not None
    assert stored.support == full["support"]
    assert stored.pca_denominator == full["pca_denominator"]
    assert stored.head_count == full["head_count"]
