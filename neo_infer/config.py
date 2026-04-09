from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "neo4j")
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j")
    min_support: int = int(os.getenv("MIN_SUPPORT", "5"))
    min_confidence: float = float(os.getenv("MIN_CONFIDENCE", "0.1"))
    max_rule_length: int = int(os.getenv("MAX_RULE_LENGTH", "2"))
    # 格式示例: "parentOf:childOf,nationality:foreignNationality"
    # 含义: 当推理 head=parentOf 时，如同向已存在 childOf 则视为冲突。
    conflict_relation_pairs: str = os.getenv("CONFLICT_RELATION_PAIRS", "")
    changelog_trigger_auto_install: bool = os.getenv("CHANGELOG_TRIGGER_AUTO_INSTALL", "0") in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }
    changelog_trigger_name: str = os.getenv("CHANGELOG_TRIGGER_NAME", "neo_infer_changelog")


def get_settings() -> Settings:
    return Settings()


def parse_conflict_relation_pairs(raw: str) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    if not raw.strip():
        return mapping

    for item in raw.split(","):
        piece = item.strip()
        if not piece or ":" not in piece:
            continue
        left, right = piece.split(":", 1)
        inferred_rel = left.strip().replace("`", "")
        conflict_rel = right.strip().replace("`", "")
        if not inferred_rel or not conflict_rel:
            continue
        mapping.setdefault(inferred_rel, set()).add(conflict_rel)
    return mapping
