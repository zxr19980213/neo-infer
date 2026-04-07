from __future__ import annotations

from neo_infer.db import Neo4jClient
from neo_infer.models import ConflictCase, Rule


class ConflictStore:
    def __init__(self, client: Neo4jClient) -> None:
        self._client = client

    def list_pairs(self) -> dict[str, set[str]]:
        rows = self._client.run_read(
            """
            MATCH (c:ConflictRule)
            RETURN c.head_relation AS head_relation, c.conflict_relation AS conflict_relation
            ORDER BY head_relation, conflict_relation
            """
        )
        result: dict[str, set[str]] = {}
        for row in rows:
            head = str(row["head_relation"])
            conflict = str(row["conflict_relation"])
            result.setdefault(head, set()).add(conflict)
        return result

    def upsert_pair(self, head_relation: str, conflict_relation: str) -> None:
        self._client.run_write(
            """
            MERGE (c:ConflictRule {head_relation: $head_relation, conflict_relation: $conflict_relation})
            ON CREATE SET c.created_at = datetime()
            SET c.updated_at = datetime()
            """,
            {"head_relation": head_relation, "conflict_relation": conflict_relation},
        )

    def delete_pair(self, head_relation: str, conflict_relation: str) -> bool:
        rows = self._client.run_write(
            """
            MATCH (c:ConflictRule {head_relation: $head_relation, conflict_relation: $conflict_relation})
            DELETE c
            RETURN count(*) AS deleted
            """,
            {"head_relation": head_relation, "conflict_relation": conflict_relation},
        )
        return bool(rows and int(rows[0]["deleted"]) > 0)

    def replace_pairs(self, pairs: dict[str, list[str]]) -> None:
        self._client.run_write("MATCH (c:ConflictRule) DELETE c")
        payload: list[dict[str, str]] = []
        for head_relation, conflicts in pairs.items():
            head = head_relation.strip().replace("`", "")
            if not head:
                continue
            for conflict_relation in conflicts:
                conflict = str(conflict_relation).strip().replace("`", "")
                if not conflict:
                    continue
                payload.append(
                    {
                        "head_relation": head,
                        "conflict_relation": conflict,
                    }
                )
        if not payload:
            return
        self._client.run_write(
            """
            UNWIND $pairs AS item
            MERGE (c:ConflictRule {head_relation: item.head_relation, conflict_relation: item.conflict_relation})
            ON CREATE SET c.created_at = datetime()
            SET c.updated_at = datetime()
            """,
            {"pairs": payload},
        )

    def record_conflict_cases(self, rule: Rule, negative_relation: str, iteration: int) -> int:
        body_len = len(rule.body_relations)
        if body_len < 2:
            return 0
        head_rel = rule.head_relation.replace("`", "")
        neg_rel = negative_relation.replace("`", "")
        if body_len == 2:
            body_1 = rule.body_relations[0].replace("`", "")
            body_2 = rule.body_relations[1].replace("`", "")
            query = f"""
            MATCH (x)-[:`{body_1}`]->(z)-[:`{body_2}`]->(y)
            WITH DISTINCT x, y
            WHERE NOT EXISTS {{ MATCH (x)-[:`{head_rel}`]->(y) }}
              AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
            MERGE (cc:ConflictCase {{
                rule_id: $rule_id,
                inferred_relation: $inferred_relation,
                conflicting_relation: $conflicting_relation,
                source_x: elementId(x),
                source_y: elementId(y)
            }})
            ON CREATE SET cc.created_at = datetime(),
                          cc.detect_count = 1,
                          cc.first_iteration = $iteration,
                          cc.last_iteration = $iteration
            SET cc.updated_at = datetime(),
                cc.detect_count = CASE
                  WHEN cc.last_iteration = $iteration THEN coalesce(cc.detect_count, 0)
                  ELSE coalesce(cc.detect_count, 0) + 1
                END,
                cc.last_iteration = $iteration
            RETURN count(cc) AS cnt
            """
        elif body_len == 3:
            body_1 = rule.body_relations[0].replace("`", "")
            body_2 = rule.body_relations[1].replace("`", "")
            body_3 = rule.body_relations[2].replace("`", "")
            query = f"""
            MATCH (x)-[:`{body_1}`]->(m1)-[:`{body_2}`]->(m2)-[:`{body_3}`]->(y)
            WITH DISTINCT x, y
            WHERE NOT EXISTS {{ MATCH (x)-[:`{head_rel}`]->(y) }}
              AND EXISTS {{ MATCH (x)-[:`{neg_rel}`]->(y) }}
            MERGE (cc:ConflictCase {{
                rule_id: $rule_id,
                inferred_relation: $inferred_relation,
                conflicting_relation: $conflicting_relation,
                source_x: elementId(x),
                source_y: elementId(y)
            }})
            ON CREATE SET cc.created_at = datetime(),
                          cc.detect_count = 1,
                          cc.first_iteration = $iteration,
                          cc.last_iteration = $iteration
            SET cc.updated_at = datetime(),
                cc.detect_count = CASE
                  WHEN cc.last_iteration = $iteration THEN coalesce(cc.detect_count, 0)
                  ELSE coalesce(cc.detect_count, 0) + 1
                END,
                cc.last_iteration = $iteration
            RETURN count(cc) AS cnt
            """
        else:
            return 0
        rows = self._client.run_write(
            query,
            {
                "rule_id": rule.rule_id,
                "inferred_relation": rule.head_relation,
                "conflicting_relation": negative_relation,
                "iteration": iteration,
            },
        )
        return int(rows[0]["cnt"]) if rows else 0

    def list_conflict_cases(self, limit: int = 200) -> list[ConflictCase]:
        rows = self._client.run_read(
            """
            MATCH (cc:ConflictCase)
            RETURN cc.rule_id AS rule_id,
                   cc.inferred_relation AS inferred_relation,
                   cc.conflicting_relation AS conflicting_relation,
                   cc.source_x AS source_x,
                   cc.source_y AS source_y,
                   coalesce(cc.detect_count, 0) AS detect_count,
                   coalesce(cc.first_iteration, 1) AS first_iteration,
                   coalesce(cc.last_iteration, 1) AS last_iteration
            ORDER BY cc.updated_at DESC, cc.detect_count DESC
            LIMIT $limit
            """,
            {"limit": limit},
        )
        return [
            ConflictCase(
                rule_id=str(row["rule_id"]),
                inferred_relation=str(row["inferred_relation"]),
                conflicting_relation=str(row["conflicting_relation"]),
                source_x=str(row["source_x"]),
                source_y=str(row["source_y"]),
                detect_count=int(row["detect_count"]),
                first_iteration=int(row["first_iteration"]),
                last_iteration=int(row["last_iteration"]),
            )
            for row in rows
        ]
