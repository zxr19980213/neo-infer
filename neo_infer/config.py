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


def get_settings() -> Settings:
    return Settings()
