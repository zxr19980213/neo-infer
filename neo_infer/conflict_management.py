from __future__ import annotations

from neo_infer.db import Neo4jClient


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
