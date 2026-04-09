from __future__ import annotations

from neo_infer.db import Neo4jClient


class TriggerManager:
    """Manage APOC trigger lifecycle for changelog capture."""

    TRIGGER_NAME = "neo_infer_capture_changelog"
    INTERNAL_LABELS = ["ChangeLog", "IdSequence", "IncrementalState", "Rule", "RuleStat", "ConflictCase", "ConflictRule"]

    def __init__(self, db: Neo4jClient, trigger_name: str | None = None) -> None:
        self._db = db
        self._trigger_name = trigger_name.strip() if trigger_name and trigger_name.strip() else self.TRIGGER_NAME

    def _probe_apoc_trigger(self, database: str | None = None) -> bool:
        try:
            self._db.run_read("CALL apoc.help('trigger')", database=database)
            return True
        except Exception:
            pass
        try:
            self._db.run_read("CALL apoc.trigger.list()", database=database)
            return True
        except Exception:
            return False

    def ensure_trigger(self, *, enabled: bool) -> bool:
        if not enabled:
            self.drop_trigger()
            return False
        return self.upsert_trigger()

    def ensure_config_enabled(self) -> bool:
        """Best-effort check for APOC trigger availability."""
        # Some Neo4j/APOC distributions expose procedures differently between
        # the user database and the system database. Probe both to avoid
        # false negatives.
        return self._probe_apoc_trigger(None) or self._probe_apoc_trigger("system")

    def _trigger_statement(self) -> str:
        labels = ",".join(f'"{item}"' for item in self.INTERNAL_LABELS)
        # Uses apoc.trigger.install and tx metadata guard:
        # - Skip system writes tagged by app (skip_changelog=true).
        # - Skip writes touching ChangeLog / internal state labels to avoid self-loop.
        return f"""
        WITH $createdRelationships AS createdRels,
             $deletedRelationships AS deletedRels,
             coalesce($metaData,{{}}) AS meta
        WHERE coalesce(meta.skip_changelog, false) = false
        CALL {{
          WITH createdRels
          UNWIND createdRels AS rel
          WITH rel
          WHERE NOT any(lbl IN labels(startNode(rel)) WHERE lbl IN [{labels}])
            AND NOT any(lbl IN labels(endNode(rel)) WHERE lbl IN [{labels}])
          MERGE (counter:IdSequence {{name: "ChangeLog"}})
          ON CREATE SET counter.next_seq = 1
          WITH counter, rel, toInteger(counter.next_seq) AS seq
          MERGE (c:ChangeLog {{dedup_key: "trigger|add|" + elementId(rel)}})
          ON CREATE SET c.change_seq = seq,
                        c.event_type = "added",
                        c.src = coalesce(startNode(rel).id, elementId(startNode(rel))),
                        c.rel = type(rel),
                        c.dst = coalesce(endNode(rel).id, elementId(endNode(rel))),
                        c.source = "trigger",
                        c.batch_id = "trigger",
                        c.idempotency_key = "trigger",
                        c.created_at = datetime()
          WITH counter, c, seq
          WHERE c.change_seq = seq
          SET counter.next_seq = seq + 1
          RETURN count(*) AS created_count
        }}
        CALL {{
          WITH deletedRels
          UNWIND deletedRels AS rel
          WITH rel
          WHERE NOT any(lbl IN labels(startNode(rel)) WHERE lbl IN [{labels}])
            AND NOT any(lbl IN labels(endNode(rel)) WHERE lbl IN [{labels}])
          MERGE (counter:IdSequence {{name: "ChangeLog"}})
          ON CREATE SET counter.next_seq = 1
          WITH counter, rel, toInteger(counter.next_seq) AS seq
          MERGE (c:ChangeLog {{dedup_key: "trigger|del|" + elementId(rel)}})
          ON CREATE SET c.change_seq = seq,
                        c.event_type = "removed",
                        c.src = coalesce(startNode(rel).id, elementId(startNode(rel))),
                        c.rel = type(rel),
                        c.dst = coalesce(endNode(rel).id, elementId(endNode(rel))),
                        c.source = "trigger",
                        c.batch_id = "trigger",
                        c.idempotency_key = "trigger",
                        c.created_at = datetime()
          WITH counter, c, seq
          WHERE c.change_seq = seq
          SET counter.next_seq = seq + 1
          RETURN count(*) AS removed_count
        }}
        RETURN 1 AS ok
        """

    def upsert_trigger(self) -> bool:
        statement = self._trigger_statement()
        self.drop_trigger()
        # Neo4j 5+ APOC style.
        try:
            rows = self._db.run_write(
                """
                CALL apoc.trigger.install($database, $name, $statement, {}, {phase: "afterAsync"})
                """,
                {
                    "database": self._db.settings.neo4j_database,
                    "name": self._trigger_name,
                    "statement": statement,
                },
                database="system",
            )
            if rows:
                return any(bool(row.get("installed")) for row in rows)
            return True
        except Exception:
            pass
        # Neo4j 4.x APOC fallback.
        try:
            rows = self._db.run_write(
                """
                CALL apoc.trigger.add($name, $statement, {}, {phase: "afterAsync"})
                """,
                {
                    "name": self._trigger_name,
                    "statement": statement,
                },
                database="system",
            )
            if rows:
                return any(bool(row.get("installed")) for row in rows)
            return True
        except Exception:
            return False

    def diagnose_install(self) -> dict[str, object]:
        """Return verbose diagnostics for trigger install issues."""
        diagnostic: dict[str, object] = {
            "database": self._db.settings.neo4j_database,
            "trigger_name": self._trigger_name,
            "apoc_available": False,
            "install_attempt": None,
            "install_error": None,
            "list_error": None,
            "list_names": [],
        }
        diagnostic["apoc_available"] = self.ensure_config_enabled()
        if not diagnostic["apoc_available"]:
            return diagnostic

        statement = self._trigger_statement()
        self.drop_trigger()
        try:
            diagnostic["install_attempt"] = "install"
            self._db.run_write(
                """
                CALL apoc.trigger.install($database, $name, $statement, {}, {phase: "afterAsync"})
                """,
                {
                    "database": self._db.settings.neo4j_database,
                    "name": self._trigger_name,
                    "statement": statement,
                },
                database="system",
            )
        except Exception as exc:
            diagnostic["install_error"] = f"install:{type(exc).__name__}:{exc}"
            try:
                diagnostic["install_attempt"] = "add"
                self._db.run_write(
                    """
                    CALL apoc.trigger.add($name, $statement, {}, {phase: "afterAsync"})
                    """,
                    {
                        "name": self._trigger_name,
                        "statement": statement,
                    },
                    database="system",
                )
            except Exception as exc2:
                diagnostic["install_error"] = (
                    f"{diagnostic['install_error']} | add:{type(exc2).__name__}:{exc2}"
                )

        try:
            rows = self.list_triggers()
            diagnostic["list_names"] = [str(item.get("name", "")) for item in rows]
        except Exception as exc:
            diagnostic["list_error"] = f"{type(exc).__name__}:{exc}"
        return diagnostic

    def list_triggers(self) -> list[dict[str, object]]:
        try:
            rows = self._db.run_read("CALL apoc.trigger.list()")
            return rows
        except Exception:
            pass
        try:
            rows = self._db.run_read("CALL apoc.trigger.list()", database="system")
            return rows
        except Exception:
            return []

    def drop_trigger(self) -> bool:
        # Neo4j 5+ APOC style.
        try:
            self._db.run_write(
                """
                CALL apoc.trigger.drop($database, $name)
                """,
                {
                    "database": self._db.settings.neo4j_database,
                    "name": self._trigger_name,
                },
                database="system",
            )
            return True
        except Exception:
            pass
        # Neo4j 4.x APOC fallback.
        try:
            self._db.run_write(
                """
                CALL apoc.trigger.remove($name)
                """,
                {"name": self._trigger_name},
                database="system",
            )
            return True
        except Exception:
            return False
