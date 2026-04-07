from __future__ import annotations

from typing import Iterable

from neo_infer.db import Neo4jClient
from neo_infer.models import Rule


class RuleStore:
    def __init__(self, client: Neo4jClient) -> None:
        self._client = client

    def upsert_rules(self, rules: Iterable[Rule]) -> None:
        payload = [
            {
                "rule_id": rule.rule_id,
                "body_relations": list(rule.body_relations),
                "head_relation": rule.head_relation,
                "support": rule.support,
                "pca_confidence": rule.pca_confidence,
                "head_coverage": rule.head_coverage,
                "status": rule.status,
                "version": rule.version,
                "rule_text": rule.text,
            }
            for rule in rules
        ]
        if not payload:
            return
        self._client.run_write(
            """
            UNWIND $rules AS rule
            MERGE (r:Rule {rule_id: rule.rule_id})
            ON CREATE SET r.status = rule.status,
                          r.version = rule.version
            SET r.body_relations = rule.body_relations,
                r.head_relation = rule.head_relation,
                r.support = rule.support,
                r.pca_confidence = rule.pca_confidence,
                r.head_coverage = rule.head_coverage,
                r.rule_text = rule.rule_text,
                r.version = CASE
                  WHEN r.status IN ['adopted', 'applied', 'rejected'] THEN coalesce(r.version, 1) + 1
                  ELSE coalesce(r.version, 1)
                END
            """,
            {"rules": payload},
        )

    def list_rules(self, status: str | None = None, limit: int = 100) -> list[Rule]:
        rows = self._client.run_read(
            """
            MATCH (r:Rule)
            WHERE $status IS NULL OR r.status = $status
            RETURN r.rule_id AS rule_id,
                   r.body_relations AS body_relations,
                   r.head_relation AS head_relation,
                   r.support AS support,
                   r.pca_confidence AS pca_confidence,
                   r.head_coverage AS head_coverage,
                   r.status AS status,
                   r.version AS version
            ORDER BY r.pca_confidence DESC, r.support DESC
            LIMIT $limit
            """,
            {"status": status, "limit": limit},
        )
        result: list[Rule] = []
        for row in rows:
            result.append(
                Rule(
                    rule_id=row["rule_id"],
                    body_relations=tuple(row["body_relations"]),
                    head_relation=row["head_relation"],
                    support=int(row["support"]),
                    pca_confidence=float(row["pca_confidence"]),
                    head_coverage=float(row["head_coverage"]),
                    status=row.get("status", "discovered"),
                    version=int(row.get("version", 1)),
                )
            )
        return result

    def update_rule_status(self, rule_id: str, status: str) -> bool:
        rows = self._client.run_write(
            """
            MATCH (r:Rule {rule_id: $rule_id})
            SET r.status = $status
            RETURN count(r) AS updated
            """,
            {"rule_id": rule_id, "status": status},
        )
        return bool(rows and rows[0]["updated"] > 0)

