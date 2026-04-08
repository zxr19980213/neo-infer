#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_BIN = sys.executable


@dataclass(frozen=True)
class Strategy:
    name: str
    statements: list[str]


def run_cypher(statement: str) -> None:
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    if shutil_which("cypher-shell"):
        cmd = [
            "cypher-shell",
            "-a",
            uri,
            "-u",
            user,
            "-p",
            password,
            "-d",
            database,
            statement,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"cypher-shell failed: {proc.stderr.strip()}")
        return

    try:
        from neo4j import GraphDatabase
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("neo4j driver is required when cypher-shell is unavailable") from exc

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=database) as session:
            session.run(statement).consume()
    finally:
        driver.close()


def shutil_which(binary: str) -> str | None:
    from shutil import which

    return which(binary)


def apply_strategy(strategy: Strategy) -> None:
    for stmt in strategy.statements:
        run_cypher(stmt)


def run_benchmark(
    *,
    api_base_url: str,
    body_length: int,
    mine_loops: int,
    infer_loops: int,
    top_k: int,
    min_support: int,
    min_pca: float,
    output_json: str,
) -> dict[str, object]:
    cmd = [
        PYTHON_BIN,
        os.path.join(ROOT_DIR, "scripts", "bench_api_perf.py"),
        "--base-url",
        api_base_url,
        "--out",
        output_json,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    with open(output_json, "r", encoding="utf-8") as fh:
        return json.load(fh)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Neo4j index strategies for mining/inference workload")
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--body-length", type=int, default=2, choices=[2, 3])
    parser.add_argument("--mine-loops", type=int, default=3)
    parser.add_argument("--infer-loops", type=int, default=3)
    parser.add_argument("--top-k", type=int, default=200)
    parser.add_argument("--min-support", type=int, default=1)
    parser.add_argument("--min-pca", type=float, default=0.1)
    parser.add_argument("--output-json", default="bench_index_strategy_report.json")
    args = parser.parse_args()

    base_setup = [
        "CREATE CONSTRAINT rule_rule_id_unique IF NOT EXISTS FOR (r:Rule) REQUIRE r.rule_id IS UNIQUE",
        "CREATE CONSTRAINT conflict_rule_unique IF NOT EXISTS FOR (c:ConflictRule) REQUIRE (c.head_relation, c.conflict_relation) IS UNIQUE",
        "CREATE CONSTRAINT relation_type_name_unique IF NOT EXISTS FOR (r:RelationType) REQUIRE r.name IS UNIQUE",
        "CREATE CONSTRAINT changelog_change_seq_unique IF NOT EXISTS FOR (c:ChangeLog) REQUIRE c.change_seq IS UNIQUE",
        "CREATE CONSTRAINT incremental_state_name_unique IF NOT EXISTS FOR (s:IncrementalState) REQUIRE s.name IS UNIQUE",
    ]

    strategies = [
        Strategy(
            name="baseline",
            statements=[
                *base_setup,
                "CREATE INDEX IF NOT EXISTS FOR (r:Rule) ON (r.head_relation)",
                "CREATE INDEX IF NOT EXISTS FOR (r:Rule) ON (r.status)",
            ],
        ),
        Strategy(
            name="lean",
            statements=[
                *base_setup,
                "CREATE INDEX IF NOT EXISTS FOR (c:ChangeLog) ON (c.rel)",
                "CREATE INDEX IF NOT EXISTS FOR (c:ChangeLog) ON (c.event_type)",
                "CREATE INDEX IF NOT EXISTS FOR (cc:ConflictCase) ON (cc.rule_id)",
            ],
        ),
        Strategy(
            name="aggressive",
            statements=[
                *base_setup,
                "CREATE INDEX IF NOT EXISTS FOR (r:Rule) ON (r.head_relation)",
                "CREATE INDEX IF NOT EXISTS FOR (r:Rule) ON (r.status)",
                "CREATE INDEX IF NOT EXISTS FOR (c:ChangeLog) ON (c.rel)",
                "CREATE INDEX IF NOT EXISTS FOR (c:ChangeLog) ON (c.event_type)",
                "CREATE INDEX IF NOT EXISTS FOR (cc:ConflictCase) ON (cc.rule_id)",
                "CREATE INDEX IF NOT EXISTS FOR (cc:ConflictCase) ON (cc.inferred_relation, cc.conflicting_relation)",
            ],
        ),
    ]

    report: dict[str, object] = {"strategies": []}
    for strategy in strategies:
        print(f"[index-bench] apply strategy: {strategy.name}")
        apply_strategy(strategy)
        out_path = os.path.join(ROOT_DIR, f"bench_{strategy.name}.json")
        perf = run_benchmark(
            api_base_url=args.api_base_url,
            body_length=args.body_length,
            mine_loops=args.mine_loops,
            infer_loops=args.infer_loops,
            top_k=args.top_k,
            min_support=args.min_support,
            min_pca=args.min_pca,
            output_json=out_path,
        )
        report["strategies"].append(
            {
                "name": strategy.name,
                "mine_mean_ms": perf["mine"]["mean_ms"],
                "infer_mean_ms": perf["inference"]["mean_ms"],
                "bench_json": out_path,
            }
        )

    with open(args.output_json, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(f"[index-bench] report written to: {args.output_json}")
    for row in report["strategies"]:
        print(
            "[index-bench]",
            row["name"],
            f"mine_mean_ms={row['mine_mean_ms']:.2f}",
            f"infer_mean_ms={row['infer_mean_ms']:.2f}",
        )


if __name__ == "__main__":
    main()
