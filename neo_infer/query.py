from __future__ import annotations

from dataclasses import dataclass

from neo4j import Driver

from neo_infer.models import ConflictCase, Rule


@dataclass(frozen=True)
class PathRuleCandidate:
    body_relations: tuple[str, ...]
    head_relation: str
    support: int
    pca_denominator: int

    @property
    def pca_confidence(self) -> float:
        if self.pca_denominator <= 0:
            return 0.0
        return self.support / self.pca_denominator

    @property
    def body_r1(self) -> str:
        return self.body_relations[0]

    @property
    def body_r2(self) -> str:
        return self.body_relations[1]

    @property
    def head_r3(self) -> str:
        return self.head_relation

    @property
    def body_r3(self) -> str:
        return self.body_relations[2]

    @property
    def head_r4(self) -> str:
        return self.head_relation


class QueryRepository:
    def __init__(self, driver: Driver, database: str = "neo4j") -> None:
        self._driver = driver
        self._database = database

    @staticmethod
    def _fact_filter(var_name: str, factual_only: bool) -> str:
        if not factual_only:
            return ""
        return f" AND coalesce({var_name}.is_inferred, false) = false"

    def list_relations(self) -> list[str]:
        query = """
        MATCH ()-[r]->()
        RETURN type(r) AS relation, count(*) AS freq
        ORDER BY freq DESC
        """
        with self._driver.session(database=self._database) as session:
            records = session.run(query)
            return [str(record["relation"]) for record in records]

    def head_relation_counts(self, factual_only: bool = True) -> dict[str, int]:
        query = """
        MATCH ()-[r]->()
        WHERE (NOT $factual_only) OR coalesce(r.is_inferred, false) = false
        RETURN type(r) AS relation, count(*) AS freq
        """
        with self._driver.session(database=self._database) as session:
            records = session.run(query, {"factual_only": factual_only})
            return {str(record["relation"]): int(record["freq"]) for record in records}

    def relation_functionality(self, factual_only: bool = True) -> dict[str, float]:
        """Return relation functionality in [0, 1]: distinct_sources / edges."""
        query = """
        MATCH (s)-[r]->()
        WHERE (NOT $factual_only) OR coalesce(r.is_inferred, false) = false
        WITH type(r) AS relation, count(r) AS edge_count, count(DISTINCT s) AS src_count
        RETURN relation,
               CASE WHEN edge_count = 0 THEN 0.0 ELSE toFloat(src_count) / toFloat(edge_count) END AS functionality
        """
        with self._driver.session(database=self._database) as session:
            records = session.run(query, {"factual_only": factual_only})
            return {
                str(record["relation"]): float(record["functionality"])
                for record in records
            }

    @staticmethod
    def _batch_length2_pca_denominators(
        session,
        phase1_records,
        *,
        factual_only: bool = True,
    ) -> dict[tuple[str, str, str], int]:
        by_head: dict[str, set[tuple[str, str]]] = {}
        for record in phase1_records:
            r1 = str(record["r1"])
            r2 = str(record["r2"])
            r3 = str(record["r3"])
            by_head.setdefault(r3, set()).add((r1, r2))

        denominator_cache: dict[tuple[str, str, str], int] = {}
        for head_rel, body_pairs in by_head.items():
            escaped_head = head_rel.replace("`", "")
            body_filter_a = "AND coalesce(a.is_inferred, false) = false" if factual_only else ""
            body_filter_b = "AND coalesce(b.is_inferred, false) = false" if factual_only else ""
            head_filter = "AND coalesce(hh.is_inferred, false) = false" if factual_only else ""
            query = f"""
            UNWIND $pairs AS pair
            MATCH (x)-[a]->(z)-[b]->(y)
            WHERE type(a) = pair.r1 AND type(b) = pair.r2
              {body_filter_a}
              {body_filter_b}
              AND EXISTS {{ MATCH (x)-[hh:`{escaped_head}`]->() WHERE true {head_filter} }}
            RETURN pair.r1 AS r1, pair.r2 AS r2, count(DISTINCT [x, y]) AS pca_denominator
            """
            payload = [{"r1": r1, "r2": r2} for r1, r2 in sorted(body_pairs)]
            rows = list(session.run(query, {"pairs": payload}))
            found_keys: set[tuple[str, str]] = set()
            for row in rows:
                r1 = str(row["r1"])
                r2 = str(row["r2"])
                found_keys.add((r1, r2))
                denominator_cache[(r1, r2, head_rel)] = int(row["pca_denominator"])
            for r1, r2 in body_pairs:
                denominator_cache.setdefault((r1, r2, head_rel), 0)
        return denominator_cache

    @staticmethod
    def _batch_length3_pca_denominators(
        session,
        phase1_records,
        *,
        factual_only: bool = True,
    ) -> dict[tuple[str, str, str, str], int]:
        by_head: dict[str, set[tuple[str, str, str]]] = {}
        for record in phase1_records:
            r1 = str(record["r1"])
            r2 = str(record["r2"])
            r3 = str(record["r3"])
            head = str(record["head"])
            by_head.setdefault(head, set()).add((r1, r2, r3))

        denominator_cache: dict[tuple[str, str, str, str], int] = {}
        for head_rel, body_triples in by_head.items():
            escaped_head = head_rel.replace("`", "")
            body_filter_a = "AND coalesce(a.is_inferred, false) = false" if factual_only else ""
            body_filter_b = "AND coalesce(b.is_inferred, false) = false" if factual_only else ""
            body_filter_c = "AND coalesce(c.is_inferred, false) = false" if factual_only else ""
            head_filter = "AND coalesce(hh.is_inferred, false) = false" if factual_only else ""
            query = f"""
            UNWIND $triples AS tri
            MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
            WHERE type(a) = tri.r1 AND type(b) = tri.r2 AND type(c) = tri.r3
              {body_filter_a}
              {body_filter_b}
              {body_filter_c}
              AND EXISTS {{ MATCH (x)-[hh:`{escaped_head}`]->() WHERE true {head_filter} }}
            RETURN tri.r1 AS r1, tri.r2 AS r2, tri.r3 AS r3, count(DISTINCT [x, y]) AS pca_denominator
            """
            payload = [{"r1": r1, "r2": r2, "r3": r3} for r1, r2, r3 in sorted(body_triples)]
            rows = list(session.run(query, {"triples": payload}))
            found_keys: set[tuple[str, str, str]] = set()
            for row in rows:
                r1 = str(row["r1"])
                r2 = str(row["r2"])
                r3 = str(row["r3"])
                found_keys.add((r1, r2, r3))
                denominator_cache[(r1, r2, r3, head_rel)] = int(row["pca_denominator"])
            for r1, r2, r3 in body_triples:
                denominator_cache.setdefault((r1, r2, r3, head_rel), 0)
        return denominator_cache

    def length2_path_rule_candidates(self, limit: int = 5000, factual_only: bool = True) -> list[PathRuleCandidate]:
        bodies = self.length2_body_candidates(limit=limit, affected_relations=None, factual_only=factual_only)
        body_pairs = [(r1, r2) for r1, r2, _ in bodies]
        return self.length2_path_rule_candidates_for_bodies(
            body_pairs=body_pairs,
            limit=limit,
            factual_only=factual_only,
        )

    def length2_path_rule_candidates_incremental(
        self,
        limit: int,
        affected_relations: list[str],
        factual_only: bool = True,
    ) -> list[PathRuleCandidate]:
        touched = [rel.strip().replace("`", "") for rel in affected_relations if rel.strip()]
        bodies = self.length2_body_candidates(limit=limit, affected_relations=touched, factual_only=factual_only)
        body_pairs = [(r1, r2) for r1, r2, _ in bodies]
        return self.length2_path_rule_candidates_for_bodies(
            body_pairs=body_pairs,
            limit=limit,
            factual_only=factual_only,
        )

    def length3_path_rule_candidates(self, limit: int = 5000, factual_only: bool = True) -> list[PathRuleCandidate]:
        bodies = self.length3_body_candidates(limit=limit, affected_relations=None, prefixes=None, factual_only=factual_only)
        body_triples = [(r1, r2, r3) for r1, r2, r3, _ in bodies]
        return self.length3_path_rule_candidates_for_bodies(
            body_triples=body_triples,
            limit=limit,
            factual_only=factual_only,
        )

    def length3_path_rule_candidates_incremental(
        self,
        limit: int,
        affected_relations: list[str],
        factual_only: bool = True,
    ) -> list[PathRuleCandidate]:
        touched = [rel.strip().replace("`", "") for rel in affected_relations if rel.strip()]
        bodies = self.length3_body_candidates(
            limit=limit,
            affected_relations=touched,
            prefixes=None,
            factual_only=factual_only,
        )
        body_triples = [(r1, r2, r3) for r1, r2, r3, _ in bodies]
        return self.length3_path_rule_candidates_for_bodies(
            body_triples=body_triples,
            limit=limit,
            factual_only=factual_only,
        )

    def length2_body_candidates(
        self,
        limit: int = 5000,
        affected_relations: list[str] | None = None,
        factual_only: bool = True,
    ) -> list[tuple[str, str, int]]:
        rels = [item.strip().replace("`", "") for item in (affected_relations or []) if item.strip()]
        query = """
        MATCH (x)-[a]->(z)-[b]->(y)
        WHERE (NOT $factual_only)
           OR (coalesce(a.is_inferred, false) = false AND coalesce(b.is_inferred, false) = false)
        WITH type(a) AS r1, type(b) AS r2, collect(DISTINCT [x, y]) AS pairs
        WHERE size($rels) = 0 OR r1 IN $rels OR r2 IN $rels
        RETURN r1, r2, size(pairs) AS body_support
        ORDER BY body_support DESC
        LIMIT $limit
        """
        with self._driver.session(database=self._database) as session:
            rows = list(session.run(query, {"rels": rels, "limit": int(limit), "factual_only": factual_only}))
            return [(str(row["r1"]), str(row["r2"]), int(row["body_support"])) for row in rows]

    def length3_body_candidates(
        self,
        limit: int = 5000,
        affected_relations: list[str] | None = None,
        prefixes: list[tuple[str, str]] | None = None,
        factual_only: bool = True,
    ) -> list[tuple[str, str, str, int]]:
        rels = [item.strip().replace("`", "") for item in (affected_relations or []) if item.strip()]
        prefix_payload = [{"r1": r1, "r2": r2} for r1, r2 in (prefixes or [])]
        query = """
        MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
        WHERE (NOT $factual_only)
           OR (coalesce(a.is_inferred, false) = false
               AND coalesce(b.is_inferred, false) = false
               AND coalesce(c.is_inferred, false) = false)
        WITH type(a) AS r1, type(b) AS r2, type(c) AS r3, collect(DISTINCT [x, y]) AS pairs
        WHERE (size($rels) = 0 OR r1 IN $rels OR r2 IN $rels OR r3 IN $rels)
          AND (size($prefixes) = 0 OR any(p IN $prefixes WHERE p.r1 = r1 AND p.r2 = r2))
        RETURN r1, r2, r3, size(pairs) AS body_support
        ORDER BY body_support DESC
        LIMIT $limit
        """
        with self._driver.session(database=self._database) as session:
            rows = list(
                session.run(
                    query,
                    {
                        "rels": rels,
                        "prefixes": prefix_payload,
                        "limit": int(limit),
                        "factual_only": factual_only,
                    },
                )
            )
            return [
                (str(row["r1"]), str(row["r2"]), str(row["r3"]), int(row["body_support"]))
                for row in rows
            ]

    def length2_path_rule_candidates_for_bodies(
        self,
        body_pairs: list[tuple[str, str]],
        limit: int = 5000,
        factual_only: bool = True,
    ) -> list[PathRuleCandidate]:
        if not body_pairs:
            return []
        query = """
        UNWIND $pairs AS pair
        MATCH (x)-[a]->(z)-[b]->(y)
        WHERE type(a) = pair.r1 AND type(b) = pair.r2
          AND ((NOT $factual_only)
            OR (coalesce(a.is_inferred, false) = false AND coalesce(b.is_inferred, false) = false))
        WITH pair.r1 AS r1, pair.r2 AS r2, x, y
        MATCH (x)-[h]->(y)
        WHERE (NOT $factual_only) OR coalesce(h.is_inferred, false) = false
        WITH r1, r2, type(h) AS r3, collect(DISTINCT [x, y]) AS pairs
        RETURN r1, r2, r3, size(pairs) AS support
        ORDER BY support DESC
        LIMIT $limit
        """
        payload = [{"r1": r1, "r2": r2} for r1, r2 in body_pairs]
        with self._driver.session(database=self._database) as session:
            phase1_records = list(
                session.run(query, {"pairs": payload, "limit": int(limit), "factual_only": factual_only})
            )
            denominator_cache = self._batch_length2_pca_denominators(
                session,
                phase1_records,
                factual_only=factual_only,
            )
            return [
                PathRuleCandidate(
                    body_relations=(str(record["r1"]), str(record["r2"])),
                    head_relation=str(record["r3"]),
                    support=int(record["support"]),
                    pca_denominator=denominator_cache.get(
                        (str(record["r1"]), str(record["r2"]), str(record["r3"])),
                        0,
                    ),
                )
                for record in phase1_records
            ]

    def length3_path_rule_candidates_for_bodies(
        self,
        body_triples: list[tuple[str, str, str]],
        limit: int = 5000,
        factual_only: bool = True,
    ) -> list[PathRuleCandidate]:
        if not body_triples:
            return []
        query = """
        UNWIND $triples AS tri
        MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
        WHERE type(a) = tri.r1 AND type(b) = tri.r2 AND type(c) = tri.r3
          AND ((NOT $factual_only)
            OR (coalesce(a.is_inferred, false) = false
                AND coalesce(b.is_inferred, false) = false
                AND coalesce(c.is_inferred, false) = false))
        WITH tri.r1 AS r1, tri.r2 AS r2, tri.r3 AS r3, x, y
        MATCH (x)-[h]->(y)
        WHERE (NOT $factual_only) OR coalesce(h.is_inferred, false) = false
        WITH r1, r2, r3, type(h) AS head, collect(DISTINCT [x, y]) AS pairs
        RETURN r1, r2, r3, head, size(pairs) AS support
        ORDER BY support DESC
        LIMIT $limit
        """
        payload = [{"r1": r1, "r2": r2, "r3": r3} for r1, r2, r3 in body_triples]
        with self._driver.session(database=self._database) as session:
            phase1_records = list(
                session.run(query, {"triples": payload, "limit": int(limit), "factual_only": factual_only})
            )
            denominator_cache = self._batch_length3_pca_denominators(
                session,
                phase1_records,
                factual_only=factual_only,
            )
            return [
                PathRuleCandidate(
                    body_relations=(str(record["r1"]), str(record["r2"]), str(record["r3"])),
                    head_relation=str(record["head"]),
                    support=int(record["support"]),
                    pca_denominator=denominator_cache.get(
                        (
                            str(record["r1"]),
                            str(record["r2"]),
                            str(record["r3"]),
                            str(record["head"]),
                        ),
                        0,
                    ),
                )
                for record in phase1_records
            ]

    def compute_length2_rule_metrics(
        self,
        r1: str,
        r2: str,
        head_rel: str,
        factual_only: bool = True,
    ) -> dict[str, int]:
        query = """
        CALL () {
          MATCH (x)-[a]->(z)-[b]->(y)
          WHERE type(a) = $r1 AND type(b) = $r2
            AND ((NOT $factual_only)
              OR (coalesce(a.is_inferred, false) = false AND coalesce(b.is_inferred, false) = false))
          MATCH (x)-[h]->(y)
          WHERE type(h) = $head_rel
            AND ((NOT $factual_only) OR coalesce(h.is_inferred, false) = false)
          RETURN count(DISTINCT [x, y]) AS support
        }
        CALL () {
          MATCH (x)-[a]->(z)-[b]->(y)
          WHERE type(a) = $r1 AND type(b) = $r2
            AND ((NOT $factual_only)
              OR (coalesce(a.is_inferred, false) = false AND coalesce(b.is_inferred, false) = false))
            AND EXISTS {
              MATCH (x)-[hh]->()
              WHERE type(hh) = $head_rel
                AND ((NOT $factual_only) OR coalesce(hh.is_inferred, false) = false)
            }
          RETURN count(DISTINCT [x, y]) AS pca_denominator
        }
        CALL () {
          MATCH ()-[h]->()
          WHERE type(h) = $head_rel
            AND ((NOT $factual_only) OR coalesce(h.is_inferred, false) = false)
          RETURN count(h) AS head_count
        }
        RETURN support, pca_denominator, head_count
        """
        with self._driver.session(database=self._database) as session:
            row = session.run(
                query,
                {"r1": r1, "r2": r2, "head_rel": head_rel, "factual_only": factual_only},
            ).single()
            if row is None:
                return {"support": 0, "pca_denominator": 0, "head_count": 0}
            return {
                "support": int(row["support"]),
                "pca_denominator": int(row["pca_denominator"]),
                "head_count": int(row["head_count"]),
            }

    def compute_length3_rule_metrics(
        self,
        r1: str,
        r2: str,
        r3: str,
        head_rel: str,
        factual_only: bool = True,
    ) -> dict[str, int]:
        query = """
        CALL () {
          MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
          WHERE type(a) = $r1 AND type(b) = $r2 AND type(c) = $r3
            AND ((NOT $factual_only)
              OR (coalesce(a.is_inferred, false) = false
                  AND coalesce(b.is_inferred, false) = false
                  AND coalesce(c.is_inferred, false) = false))
          MATCH (x)-[h]->(y)
          WHERE type(h) = $head_rel
            AND ((NOT $factual_only) OR coalesce(h.is_inferred, false) = false)
          RETURN count(DISTINCT [x, y]) AS support
        }
        CALL () {
          MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
          WHERE type(a) = $r1 AND type(b) = $r2 AND type(c) = $r3
            AND ((NOT $factual_only)
              OR (coalesce(a.is_inferred, false) = false
                  AND coalesce(b.is_inferred, false) = false
                  AND coalesce(c.is_inferred, false) = false))
            AND EXISTS {
              MATCH (x)-[hh]->()
              WHERE type(hh) = $head_rel
                AND ((NOT $factual_only) OR coalesce(hh.is_inferred, false) = false)
            }
          RETURN count(DISTINCT [x, y]) AS pca_denominator
        }
        CALL () {
          MATCH ()-[h]->()
          WHERE type(h) = $head_rel
            AND ((NOT $factual_only) OR coalesce(h.is_inferred, false) = false)
          RETURN count(h) AS head_count
        }
        RETURN support, pca_denominator, head_count
        """
        with self._driver.session(database=self._database) as session:
            row = session.run(
                query,
                {"r1": r1, "r2": r2, "r3": r3, "head_rel": head_rel, "factual_only": factual_only},
            ).single()
            if row is None:
                return {"support": 0, "pca_denominator": 0, "head_count": 0}
            return {
                "support": int(row["support"]),
                "pca_denominator": int(row["pca_denominator"]),
                "head_count": int(row["head_count"]),
            }

    def apply_length2_rule(self, rule: Rule) -> int:
        body_r1 = rule.body_relations[0].replace("`", "")
        body_r2 = rule.body_relations[1].replace("`", "")
        head_r3 = rule.head_relation.replace("`", "")

        query = f"""
        MATCH (x)-[:`{body_r1}`]->(z)-[:`{body_r2}`]->(y)
        WITH DISTINCT x, y
        WHERE NOT EXISTS {{ MATCH (x)-[:`{head_r3}`]->(y) }}
        MERGE (x)-[h:`{head_r3}`]->(y)
        ON CREATE SET h.is_inferred = true,
                      h.source_rule_id = $rule_id,
                      h.rule_confidence = $confidence,
                      h.inferred_at = datetime()
        RETURN count(h) AS created_count
        """
        with self._driver.session(database=self._database) as session:
            record = session.run(
                query,
                {"rule_id": rule.rule_id, "confidence": rule.pca_confidence},
            ).single()
            return int(record["created_count"]) if record else 0

    def apply_length3_rule(self, rule: Rule) -> int:
        body_r1 = rule.body_relations[0].replace("`", "")
        body_r2 = rule.body_relations[1].replace("`", "")
        body_r3 = rule.body_relations[2].replace("`", "")
        head_r4 = rule.head_relation.replace("`", "")

        query = f"""
        MATCH (x)-[:`{body_r1}`]->(m1)-[:`{body_r2}`]->(m2)-[:`{body_r3}`]->(y)
        WITH DISTINCT x, y
        WHERE NOT EXISTS {{ MATCH (x)-[:`{head_r4}`]->(y) }}
        MERGE (x)-[h:`{head_r4}`]->(y)
        ON CREATE SET h.is_inferred = true,
                      h.source_rule_id = $rule_id,
                      h.rule_confidence = $confidence,
                      h.inferred_at = datetime()
        RETURN count(h) AS created_count
        """
        with self._driver.session(database=self._database) as session:
            record = session.run(
                query,
                {"rule_id": rule.rule_id, "confidence": rule.pca_confidence},
            ).single()
            return int(record["created_count"]) if record else 0

    def count_conflicts_for_rule(
        self,
        rule: Rule,
        negative_relation: str,
    ) -> int:
        """Count candidate inferred pairs that already have a negative relation."""
        body_len = len(rule.body_relations)
        head_rel = rule.head_relation.replace("`", "")
        neg_rel = negative_relation.replace("`", "")
        if body_len == 2:
            body_r1 = rule.body_relations[0].replace("`", "")
            body_r2 = rule.body_relations[1].replace("`", "")
            query = f"""
            MATCH (x)-[:`{body_r1}`]->(z)-[:`{body_r2}`]->(y)
            WITH DISTINCT x, y
            WHERE NOT EXISTS {{ MATCH (x)-[:`{head_rel}`]->(y) }}
              AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
            RETURN count(*) AS conflict_count
            """
        elif body_len == 3:
            body_r1 = rule.body_relations[0].replace("`", "")
            body_r2 = rule.body_relations[1].replace("`", "")
            body_r3 = rule.body_relations[2].replace("`", "")
            query = f"""
            MATCH (x)-[:`{body_r1}`]->(m1)-[:`{body_r2}`]->(m2)-[:`{body_r3}`]->(y)
            WITH DISTINCT x, y
            WHERE NOT EXISTS {{ MATCH (x)-[:`{head_rel}`]->(y) }}
              AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
            RETURN count(*) AS conflict_count
            """
        else:
            return 0
        with self._driver.session(database=self._database) as session:
            record = session.run(query).single()
            return int(record["conflict_count"]) if record else 0

    def list_conflict_cases_for_rule(self, rule: Rule, negative_relation: str, limit: int = 1000) -> list[ConflictCase]:
        body_len = len(rule.body_relations)
        if body_len not in (2, 3):
            return []
        head_rel = rule.head_relation.replace("`", "")
        neg_rel = negative_relation.replace("`", "")
        if body_len == 2:
            body_rel_1 = rule.body_relations[0].replace("`", "")
            body_rel_2 = rule.body_relations[1].replace("`", "")
            query = f"""
            MATCH (x)-[:`{body_rel_1}`]->(z)-[:`{body_rel_2}`]->(y)
            WITH DISTINCT x, y
            WHERE NOT EXISTS {{ MATCH (x)-[:`{head_rel}`]->(y) }}
              AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
            RETURN elementId(x) AS x_id, elementId(y) AS y_id
            LIMIT $limit
            """
        else:
            body_rel_1 = rule.body_relations[0].replace("`", "")
            body_rel_2 = rule.body_relations[1].replace("`", "")
            body_rel_3 = rule.body_relations[2].replace("`", "")
            query = f"""
            MATCH (x)-[:`{body_rel_1}`]->(m1)-[:`{body_rel_2}`]->(m2)-[:`{body_rel_3}`]->(y)
            WITH DISTINCT x, y
            WHERE NOT EXISTS {{ MATCH (x)-[:`{head_rel}`]->(y) }}
              AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
            RETURN elementId(x) AS x_id, elementId(y) AS y_id
            LIMIT $limit
            """
        with self._driver.session(database=self._database) as session:
            rows = list(session.run(query, {"limit": limit}))
            result: list[ConflictCase] = []
            for row in rows:
                result.append(
                    ConflictCase(
                        rule_id=rule.rule_id,
                        inferred_relation=rule.head_relation,
                        conflicting_relation=negative_relation,
                        source_x=str(row["x_id"]),
                        source_y=str(row["y_id"]),
                        detect_count=1,
                        first_iteration=1,
                        last_iteration=1,
                    )
                )
            return result
