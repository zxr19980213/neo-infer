from __future__ import annotations

from dataclasses import dataclass
import json
import uuid
from typing import Iterable

from neo_infer.db import Neo4jClient
from neo_infer.models import ChangeEdge, ChangeLogEntry, DeltaBatch, EdgeDeltaBatch, Rule


@dataclass(frozen=True)
class RuleStat:
    rule_id: str
    support: int
    pca_denominator: int
    head_count: int


class IncrementalStore:
    """Persistence layer for changelog and incremental rule stats."""

    def __init__(self, client: Neo4jClient) -> None:
        self._client = client

    @staticmethod
    def _normalize_key(value: str | None, fallback: str) -> str:
        raw = (value or "").strip()
        return raw if raw else fallback

    @staticmethod
    def _metadata_json(metadata: dict[str, str] | None) -> str | None:
        if not metadata:
            return None
        return json.dumps(metadata, ensure_ascii=True, sort_keys=True)

    def append_changes(
        self,
        added_edges: Iterable[ChangeEdge],
        removed_edges: Iterable[ChangeEdge],
        *,
        source: str = "app",
        batch_id: str | None = None,
        idempotency_key: str | None = None,
        context: dict[str, str] | None = None,
    ) -> None:
        payload: list[dict[str, object]] = []
        source_norm = self._normalize_key(source, "app")
        batch_norm = self._normalize_key(batch_id, str(uuid.uuid4()))
        idem_norm = self._normalize_key(idempotency_key, batch_norm)
        context_json = self._metadata_json(context)
        dedup_prefix = f"{source_norm}|{batch_norm}|{idem_norm}"
        seq = 0

        for edge in added_edges:
            dedup_key = f"{dedup_prefix}|added|{edge.src}|{edge.rel}|{edge.dst}|{seq}"
            payload.append(
                {
                    "event_type": "added",
                    "src": edge.src,
                    "rel": edge.rel,
                    "dst": edge.dst,
                    "created_at": edge.created_at,
                    "source": source_norm,
                    "batch_id": batch_norm,
                    "idempotency_key": idem_norm,
                    "metadata": context_json,
                    "dedup_key": dedup_key,
                }
            )
            seq += 1
        for edge in removed_edges:
            dedup_key = f"{dedup_prefix}|removed|{edge.src}|{edge.rel}|{edge.dst}|{seq}"
            payload.append(
                {
                    "event_type": "removed",
                    "src": edge.src,
                    "rel": edge.rel,
                    "dst": edge.dst,
                    "created_at": edge.created_at,
                    "source": source_norm,
                    "batch_id": batch_norm,
                    "idempotency_key": idem_norm,
                    "metadata": context_json,
                    "dedup_key": dedup_key,
                }
            )
            seq += 1
        if not payload:
            return

        self._client.run_write(
            """
            MERGE (counter:IdSequence {name: 'ChangeLog'})
            ON CREATE SET counter.next_seq = 1
            WITH counter, $events AS events
            WITH counter, events, toInteger(counter.next_seq) AS start_seq
            UNWIND range(0, size(events) - 1) AS idx
            WITH counter, start_seq, events[idx] AS event, start_seq + idx AS seq
            MERGE (c:ChangeLog {dedup_key: event.dedup_key})
            ON CREATE SET c.change_seq = seq,
                          c.event_type = event.event_type,
                          c.src = event.src,
                          c.rel = event.rel,
                          c.dst = event.dst,
                          c.source = coalesce(event.source, 'app'),
                          c.batch_id = event.batch_id,
                          c.idempotency_key = event.idempotency_key,
                          c.metadata = event.metadata,
                          c.created_at = coalesce(event.created_at, datetime())
            WITH counter, start_seq, max(seq) AS last_seq
            SET counter.next_seq = coalesce(last_seq, start_seq - 1) + 1
            """,
            {"events": payload},
        )

    def append_changelog(self, batch: EdgeDeltaBatch) -> tuple[int, int, int]:
        self.append_changes(batch.added_edges, batch.removed_edges)
        added_count = len(batch.added_edges)
        removed_count = len(batch.removed_edges)
        cursor = self.get_cursor()
        return cursor, added_count, removed_count

    def get_cursor(self) -> int:
        rows = self._client.run_read(
            """
            MERGE (s:IncrementalState {name: 'default'})
            ON CREATE SET s.cursor = 0
            RETURN toInteger(coalesce(s.cursor, 0)) AS cursor
            """
        )
        if not rows:
            return 0
        return int(rows[0]["cursor"])

    def set_cursor(self, cursor: int) -> None:
        self._client.run_write(
            """
            MERGE (s:IncrementalState {name: 'default'})
            SET s.cursor = $cursor
            """,
            {"cursor": int(cursor)},
        )

    def consume_delta(self, limit: int = 2000) -> DeltaBatch:
        cursor = self.get_cursor()
        rows = self._client.run_read(
            """
            MATCH (c:ChangeLog)
            WHERE toInteger(coalesce(c.change_seq, 0)) > $cursor
            RETURN toInteger(coalesce(c.change_seq, 0)) AS change_id,
                   c.event_type AS event_type,
                   c.src AS src,
                   c.rel AS rel,
                   c.dst AS dst,
                   toString(c.created_at) AS created_at
            ORDER BY change_id ASC
            LIMIT $limit
            """,
            {"cursor": cursor, "limit": int(limit)},
        )
        if not rows:
            return DeltaBatch(added_edges=[], removed_edges=[], cursor=cursor)
        return self._fold_rows_to_delta(rows=rows, cursor=cursor)

    @staticmethod
    def _fold_rows_to_delta(rows: list[dict[str, object]], cursor: int) -> DeltaBatch:
        """Merge duplicate/cancelling events within one consumed window."""
        state_map: dict[tuple[str, str, str], str] = {}
        ts_map: dict[tuple[str, str, str], str | None] = {}
        max_id = cursor
        for row in rows:
            key = (str(row["src"]), str(row["rel"]), str(row["dst"]))
            op = "removed" if str(row["event_type"]) == "removed" else "added"
            prev = state_map.get(key)
            if prev == "added" and op == "removed":
                # add then remove in same window -> net zero
                state_map.pop(key, None)
                ts_map.pop(key, None)
            elif prev == "removed" and op == "added":
                # remove then add -> net add
                state_map[key] = "added"
                ts_map[key] = row.get("created_at")
            else:
                state_map[key] = op
                ts_map[key] = row.get("created_at")
            max_id = max(max_id, int(row["change_id"]))

        added = [
            ChangeEdge(src=s, rel=r, dst=d, created_at=ts_map.get((s, r, d)))
            for (s, r, d), op in state_map.items()
            if op == "added"
        ]
        removed = [
            ChangeEdge(src=s, rel=r, dst=d, created_at=ts_map.get((s, r, d)))
            for (s, r, d), op in state_map.items()
            if op == "removed"
        ]
        return DeltaBatch(added_edges=added, removed_edges=removed, cursor=max_id)

    def consume_changes(self, limit: int = 2000) -> tuple[list[ChangeEdge], int]:
        delta = self.consume_delta(limit=limit)
        events: list[ChangeEdge] = []
        events.extend(delta.added_edges)
        events.extend(delta.removed_edges)
        return events, delta.cursor

    def pending_changes(self, limit: int = 1000) -> list[ChangeLogEntry]:
        cursor = self.get_cursor()
        rows = self._client.run_read(
            """
            MATCH (c:ChangeLog)
            WHERE toInteger(coalesce(c.change_seq, 0)) > $cursor
            RETURN toString(toInteger(coalesce(c.change_seq, 0))) AS change_id,
                   c.event_type AS op,
                   c.src AS src_id,
                   c.rel AS relation,
                   c.dst AS dst_id,
                   toString(c.created_at) AS created_at,
                   c.source AS source,
                   c.batch_id AS batch_id
            ORDER BY toInteger(coalesce(c.change_seq, 0)) ASC
            LIMIT $limit
            """,
            {"cursor": cursor, "limit": int(limit)},
        )
        return [
            ChangeLogEntry(
                change_id=str(row["change_id"]),
                op=str(row["op"]),
                src_id=str(row["src_id"]),
                relation=str(row["relation"]),
                dst_id=str(row["dst_id"]),
                created_at=str(row["created_at"]),
                source=str(row.get("source")) if row.get("source") is not None else None,
                batch_id=str(row.get("batch_id")) if row.get("batch_id") is not None else None,
            )
            for row in rows
        ]

    def upsert_rule_stats(self, rule: Rule, support: int, pca_denominator: int, head_count: int) -> None:
        self._client.run_write(
            """
            MERGE (s:RuleStat {rule_id: $rule_id})
            SET s.support = $support,
                s.pca_denominator = $pca_denominator,
                s.head_count = $head_count,
                s.updated_at = datetime()
            """,
            {
                "rule_id": rule.rule_id,
                "support": int(support),
                "pca_denominator": int(pca_denominator),
                "head_count": int(head_count),
            },
        )

    def get_rule_stat(self, rule_id: str) -> RuleStat | None:
        rows = self._client.run_read(
            """
            MATCH (s:RuleStat {rule_id: $rule_id})
            RETURN s.rule_id AS rule_id,
                   toInteger(coalesce(s.support, 0)) AS support,
                   toInteger(coalesce(s.pca_denominator, 0)) AS pca_denominator,
                   toInteger(coalesce(s.head_count, 0)) AS head_count
            """,
            {"rule_id": rule_id},
        )
        if not rows:
            return None
        row = rows[0]
        return RuleStat(
            rule_id=str(row["rule_id"]),
            support=int(row["support"]),
            pca_denominator=int(row["pca_denominator"]),
            head_count=int(row["head_count"]),
        )

    def set_rule_relations(self, rule: Rule) -> None:
        rows = [{"rule_id": rule.rule_id, "rel": rel} for rel in [*rule.body_relations, rule.head_relation]]
        if not rows:
            return
        self._client.run_write(
            """
            MATCH (r:Rule {rule_id: $rule_id})
            OPTIONAL MATCH (r)-[old:INVOLVES_RELATION]->()
            DELETE old
            """,
            {"rule_id": rule.rule_id},
        )
        self._client.run_write(
            """
            UNWIND $items AS item
            MATCH (r:Rule {rule_id: item.rule_id})
            MERGE (rel:RelationType {name: item.rel})
            MERGE (r)-[:INVOLVES_RELATION]->(rel)
            """,
            {"items": rows},
        )

    def update_rule_indexes(self, rules: Iterable[Rule]) -> None:
        for rule in rules:
            self.set_rule_relations(rule)

    def affected_rule_ids(self, relations: set[str], limit: int = 5000) -> list[str]:
        if not relations:
            return []
        rows = self._client.run_read(
            """
            UNWIND $rels AS rel_name
            MATCH (:RelationType {name: rel_name})<-[:INVOLVES_RELATION]-(r:Rule)
            RETURN DISTINCT r.rule_id AS rule_id
            LIMIT $limit
            """,
            {"rels": sorted(relations), "limit": int(limit)},
        )
        return [str(row["rule_id"]) for row in rows]

    def update_rule_stats(self, rules: Iterable[Rule]) -> None:
        for rule in rules:
            pca_denominator = int(round(rule.support / rule.pca_confidence)) if rule.pca_confidence > 0 else 0
            head_count = int(round(rule.support / rule.head_coverage)) if rule.head_coverage > 0 else 0
            self.upsert_rule_stats(
                rule=rule,
                support=rule.support,
                pca_denominator=pca_denominator,
                head_count=head_count,
            )

    def mark_consumed(self, cursor: int) -> None:
        self.set_cursor(cursor)

