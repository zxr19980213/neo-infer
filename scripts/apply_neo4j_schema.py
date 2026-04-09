from __future__ import annotations

from neo_infer.config import get_settings
from neo_infer.db import Neo4jClient


def main() -> None:
    settings = get_settings()
    client = Neo4jClient(settings)
    try:
        # Create constraints and indexes (idempotent).
        client.run_write(
            """
            CREATE CONSTRAINT rule_rule_id_unique IF NOT EXISTS
            FOR (r:Rule) REQUIRE r.rule_id IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT conflict_rule_pair_unique IF NOT EXISTS
            FOR (c:ConflictRule) REQUIRE (c.head_relation, c.conflict_relation) IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT relation_type_name_unique IF NOT EXISTS
            FOR (r:RelationType) REQUIRE r.name IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT rule_stat_rule_id_unique IF NOT EXISTS
            FOR (s:RuleStat) REQUIRE s.rule_id IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT incremental_state_name_unique IF NOT EXISTS
            FOR (s:IncrementalState) REQUIRE s.name IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT id_sequence_name_unique IF NOT EXISTS
            FOR (s:IdSequence) REQUIRE s.name IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT change_log_seq_unique IF NOT EXISTS
            FOR (c:ChangeLog) REQUIRE c.change_seq IS UNIQUE
            """
        )
        client.run_write(
            """
            CREATE CONSTRAINT change_log_dedup_key_unique IF NOT EXISTS
            FOR (c:ChangeLog) REQUIRE c.dedup_key IS UNIQUE
            """
        )

        client.run_write(
            """
            CREATE RANGE INDEX change_log_event_type_idx IF NOT EXISTS
            FOR (c:ChangeLog) ON (c.event_type)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX change_log_rel_idx IF NOT EXISTS
            FOR (c:ChangeLog) ON (c.rel)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX change_log_source_idx IF NOT EXISTS
            FOR (c:ChangeLog) ON (c.source)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX change_log_batch_id_idx IF NOT EXISTS
            FOR (c:ChangeLog) ON (c.batch_id)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX change_log_idempotency_key_idx IF NOT EXISTS
            FOR (c:ChangeLog) ON (c.idempotency_key)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX rule_head_relation_idx IF NOT EXISTS
            FOR (r:Rule) ON (r.head_relation)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX conflict_case_rule_idx IF NOT EXISTS
            FOR (cc:ConflictCase) ON (cc.rule_id)
            """
        )
        client.run_write(
            """
            CREATE RANGE INDEX conflict_case_rel_pair_idx IF NOT EXISTS
            FOR (cc:ConflictCase) ON (cc.inferred_relation, cc.conflicting_relation)
            """
        )
        print("Neo4j schema applied successfully.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
