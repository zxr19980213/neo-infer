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

    def list_relations(self) -> list[str]:
        query = """
        MATCH ()-[r]->()
        RETURN type(r) AS relation, count(*) AS freq
        ORDER BY freq DESC
        """
        with self._driver.session(database=self._database) as session:
            records = session.run(query)
            return [str(record["relation"]) for record in records]

    def head_relation_counts(self) -> dict[str, int]:
        query = """
        MATCH ()-[r]->()
        RETURN type(r) AS relation, count(*) AS freq
        """
        with self._driver.session(database=self._database) as session:
            records = session.run(query)
            return {str(record["relation"]): int(record["freq"]) for record in records}

    def length2_path_rule_candidates(self, limit: int = 5000) -> list[PathRuleCandidate]:
        # 规则模板：r1(X,Z) ∧ r2(Z,Y) -> r3(X,Y)
        # support: 同时满足 body 与 head 的 (X, Y) 去重数量
        # pca_denominator: 满足 body 且 X 存在任意 r3 出边的 (X, Y) 去重数量
        phase1_query = """
        MATCH (x)-[a]->(z)-[b]->(y)
        WITH type(a) AS r1, type(b) AS r2, x, y
        MATCH (x)-[h]->(y)
        WITH r1, r2, type(h) AS r3, collect(DISTINCT [x, y]) AS pairs
        RETURN r1, r2, r3, size(pairs) AS support
        ORDER BY support DESC
        LIMIT $limit
        """

        with self._driver.session(database=self._database) as session:
            phase1_records = list(session.run(phase1_query, {"limit": limit}))
            candidates: list[PathRuleCandidate] = []
            denominator_cache: dict[tuple[str, str, str], int] = {}

            for record in phase1_records:
                r1 = str(record["r1"])
                r2 = str(record["r2"])
                r3 = str(record["r3"])
                support = int(record["support"])
                key = (r1, r2, r3)

                if key not in denominator_cache:
                    escaped_r3 = r3.replace("`", "")
                    pca_query = f"""
                    MATCH (x)-[a]->(z)-[b]->(y)
                    WHERE type(a) = $r1 AND type(b) = $r2
                      AND EXISTS {{ MATCH (x)-[:`{escaped_r3}`]->() }}
                    RETURN count(DISTINCT [x, y]) AS pca_denominator
                    """
                    pca_record = session.run(pca_query, {"r1": r1, "r2": r2}).single()
                    denominator_cache[key] = int(pca_record["pca_denominator"]) if pca_record else 0

                candidates.append(
                    PathRuleCandidate(
                        body_relations=(r1, r2),
                        head_relation=r3,
                        support=support,
                        pca_denominator=denominator_cache[key],
                    )
                )
            return candidates

    def length2_path_rule_candidates_incremental(
        self,
        limit: int,
        affected_relations: list[str],
    ) -> list[PathRuleCandidate]:
        touched = [rel.strip().replace("`", "") for rel in affected_relations if rel.strip()]
        if not touched:
            return []

        phase1_query = """
        MATCH (x)-[a]->(z)-[b]->(y)
        WITH type(a) AS r1, type(b) AS r2, x, y
        MATCH (x)-[h]->(y)
        WITH r1, r2, type(h) AS r3, collect(DISTINCT [x, y]) AS pairs
        WHERE r1 IN $rels OR r2 IN $rels OR r3 IN $rels
        RETURN r1, r2, r3, size(pairs) AS support
        ORDER BY support DESC
        LIMIT $limit
        """

        with self._driver.session(database=self._database) as session:
            phase1_records = list(session.run(phase1_query, {"limit": limit, "rels": touched}))
            candidates: list[PathRuleCandidate] = []
            denominator_cache: dict[tuple[str, str, str], int] = {}

            for record in phase1_records:
                r1 = str(record["r1"])
                r2 = str(record["r2"])
                r3 = str(record["r3"])
                support = int(record["support"])
                key = (r1, r2, r3)

                if key not in denominator_cache:
                    escaped_r3 = r3.replace("`", "")
                    pca_query = f"""
                    MATCH (x)-[a]->(z)-[b]->(y)
                    WHERE type(a) = $r1 AND type(b) = $r2
                      AND EXISTS {{ MATCH (x)-[:`{escaped_r3}`]->() }}
                    RETURN count(DISTINCT [x, y]) AS pca_denominator
                    """
                    pca_record = session.run(pca_query, {"r1": r1, "r2": r2}).single()
                    denominator_cache[key] = int(pca_record["pca_denominator"]) if pca_record else 0

                candidates.append(
                    PathRuleCandidate(
                        body_relations=(r1, r2),
                        head_relation=r3,
                        support=support,
                        pca_denominator=denominator_cache[key],
                    )
                )
            return candidates

    def length3_path_rule_candidates(self, limit: int = 5000) -> list[PathRuleCandidate]:
        # 规则模板：r1(X,A) ∧ r2(A,B) ∧ r3(B,Y) -> h(X,Y)
        phase1_query = """
        MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
        WITH type(a) AS r1, type(b) AS r2, type(c) AS r3, x, y
        MATCH (x)-[h]->(y)
        WITH r1, r2, r3, type(h) AS head, collect(DISTINCT [x, y]) AS pairs
        RETURN r1, r2, r3, head, size(pairs) AS support
        ORDER BY support DESC
        LIMIT $limit
        """
        with self._driver.session(database=self._database) as session:
            phase1_records = list(session.run(phase1_query, {"limit": limit}))
            candidates: list[PathRuleCandidate] = []
            denominator_cache: dict[tuple[str, str, str, str], int] = {}

            for record in phase1_records:
                r1 = str(record["r1"])
                r2 = str(record["r2"])
                r3 = str(record["r3"])
                head = str(record["head"])
                support = int(record["support"])
                key = (r1, r2, r3, head)
                if key not in denominator_cache:
                    escaped_head = head.replace("`", "")
                    pca_query = f"""
                    MATCH (x)-[a]->(m1)-[b]->(m2)-[c]->(y)
                    WHERE type(a) = $r1 AND type(b) = $r2 AND type(c) = $r3
                      AND EXISTS {{ MATCH (x)-[:`{escaped_head}`]->() }}
                    RETURN count(DISTINCT [x, y]) AS pca_denominator
                    """
                    pca_record = session.run(pca_query, {"r1": r1, "r2": r2, "r3": r3}).single()
                    denominator_cache[key] = int(pca_record["pca_denominator"]) if pca_record else 0

                candidates.append(
                    PathRuleCandidate(
                        body_relations=(r1, r2, r3),
                        head_relation=head,
                        support=support,
                        pca_denominator=denominator_cache[key],
                    )
                )
            return candidates

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
        body_r1 = rule.body_relations[0].replace("`", "")
        body_r2 = rule.body_relations[1].replace("`", "")
        head_r3 = rule.head_relation.replace("`", "")
        neg_rel = negative_relation.replace("`", "")

        query = f"""
        MATCH (x)-[:`{body_r1}`]->(z)-[:`{body_r2}`]->(y)
        WITH DISTINCT x, y
        WHERE NOT EXISTS {{ MATCH (x)-[:`{head_r3}`]->(y) }}
          AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
        RETURN count(*) AS conflict_count
        """
        with self._driver.session(database=self._database) as session:
            record = session.run(query).single()
            return int(record["conflict_count"]) if record else 0

    def list_conflict_cases_for_rule(self, rule: Rule, negative_relation: str, limit: int = 1000) -> list[ConflictCase]:
        if len(rule.body_relations) != 2:
            return []
        body_rel_1 = rule.body_relations[0].replace("`", "")
        body_rel_2 = rule.body_relations[1].replace("`", "")
        head_rel = rule.head_relation.replace("`", "")
        neg_rel = negative_relation.replace("`", "")

        query = f"""
        MATCH (x)-[:`{body_rel_1}`]->(z)-[:`{body_rel_2}`]->(y)
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
