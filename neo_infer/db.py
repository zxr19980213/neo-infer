from __future__ import annotations

from typing import Any

from neo4j import Driver, GraphDatabase

from neo_infer.config import Settings


class Neo4jClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.driver: Driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )

    def close(self) -> None:
        self.driver.close()

    def run_read(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        database: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.driver.session(database=database or self.settings.neo4j_database) as session:
            result = session.run(query, parameters or {})
            return [r.data() for r in result]

    def run_write(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        database: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.driver.session(database=database or self.settings.neo4j_database) as session:
            result = session.run(query, parameters or {})
            return [r.data() for r in result]
