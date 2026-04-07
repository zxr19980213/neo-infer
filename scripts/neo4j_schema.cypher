// ===== neo-infer schema (idempotent) =====
// Apply with:
//   cypher-shell -u neo4j -p neo4j -f scripts/neo4j_schema.cypher

// Rule
CREATE CONSTRAINT rule_rule_id_unique IF NOT EXISTS
FOR (r:Rule) REQUIRE r.rule_id IS UNIQUE;

// ConflictRule
CREATE CONSTRAINT conflict_rule_pair_unique IF NOT EXISTS
FOR (c:ConflictRule) REQUIRE (c.head_relation, c.conflict_relation) IS UNIQUE;

// ChangeLog / sequence
CREATE CONSTRAINT changelog_change_seq_unique IF NOT EXISTS
FOR (c:ChangeLog) REQUIRE c.change_seq IS UNIQUE;

CREATE CONSTRAINT id_sequence_name_unique IF NOT EXISTS
FOR (s:IdSequence) REQUIRE s.name IS UNIQUE;

// Incremental state
CREATE CONSTRAINT incremental_state_name_unique IF NOT EXISTS
FOR (s:IncrementalState) REQUIRE s.name IS UNIQUE;

// Rule statistics
CREATE CONSTRAINT rule_stat_rule_id_unique IF NOT EXISTS
FOR (s:RuleStat) REQUIRE s.rule_id IS UNIQUE;

// Relation type index node used by incremental rule lookup
CREATE CONSTRAINT relation_type_name_unique IF NOT EXISTS
FOR (r:RelationType) REQUIRE r.name IS UNIQUE;

// ConflictCase: upsert key and retrieval index
CREATE CONSTRAINT conflict_case_key_unique IF NOT EXISTS
FOR (cc:ConflictCase) REQUIRE (cc.rule_id, cc.conflicting_relation, cc.source_x, cc.source_y) IS UNIQUE;

CREATE INDEX conflict_case_updated_at_idx IF NOT EXISTS
FOR (cc:ConflictCase) ON (cc.updated_at);

