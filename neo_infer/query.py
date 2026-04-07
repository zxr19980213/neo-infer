from __future__ import annotations

from dataclasses import dataclass

from neo4j import Driver

from neo_infer.models import Rule


@dataclass(frozen=True)
class PathRuleCandidate:
    body_r1: str
    body_r2: str
    head_r3: str
    support: int
    pca_denominator: int

    @property
    def pca_confidence(self) -> float:
        if self.pca_denominator <= 0:
            return 0.0
        return self.support / self.pca_denominator


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
                        body_r1=r1,
                        body_r2=r2,
                        head_r3=r3,
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
