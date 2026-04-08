#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Any


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_BIN = sys.executable


@dataclass(frozen=True)
class Strategy:
    name: str
    statements: list[str]


def neo4j_conn() -> tuple[str, str, str, str]:
    uri = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    return uri, user, password, database


def run_query(statement: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    uri, user, password, database = neo4j_conn()
    try:
        from neo4j import GraphDatabase
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("neo4j python driver is required for index strategy benchmark") from exc
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=database) as session:
            result = session.run(statement, parameters or {})
            return [row.data() for row in result]
    finally:
        driver.close()


def run_cypher(statement: str) -> None:
    _ = run_query(statement)


def shutil_which(binary: str) -> str | None:
    from shutil import which

    return which(binary)


def drop_benchmark_indexes() -> None:
    rows = run_query(
        """
        SHOW INDEXES YIELD name, entityType, labelsOrTypes, properties
        WHERE entityType = 'NODE'
        RETURN name, labelsOrTypes, properties
        """
    )
    target = {
        ("Rule", ("head_relation",)),
        ("Rule", ("status",)),
        ("ChangeLog", ("rel",)),
        ("ChangeLog", ("event_type",)),
        ("ConflictCase", ("rule_id",)),
        ("ConflictCase", ("inferred_relation", "conflicting_relation")),
    }
    for row in rows:
        labels = tuple(row.get("labelsOrTypes") or [])
        props = tuple(row.get("properties") or [])
        if len(labels) != 1:
            continue
        key = (labels[0], props)
        if key in target:
            run_cypher(f"DROP INDEX `{row['name']}` IF EXISTS")


def reset_runtime_state() -> None:
    # Keep base KG data, reset benchmark-generated state for fair strategy comparison.
    statements = [
        "MATCH ()-[r]->() WHERE coalesce(r.is_inferred, false) = true DELETE r",
        "MATCH (n:Rule) DETACH DELETE n",
        "MATCH (n:RuleStat) DETACH DELETE n",
        "MATCH (n:RelationType) DETACH DELETE n",
        "MATCH (n:ConflictCase) DETACH DELETE n",
        "MATCH (n:ConflictRule) DETACH DELETE n",
        "MATCH (n:ChangeLog) DETACH DELETE n",
        "MATCH (n:IncrementalState) DETACH DELETE n",
        "MERGE (s:IdSequence {name: 'ChangeLog'}) SET s.next_seq = 1",
    ]
    for stmt in statements:
        run_cypher(stmt)


def apply_strategy(strategy: Strategy) -> None:
    drop_benchmark_indexes()
    reset_runtime_state()
    for stmt in strategy.statements:
        run_cypher(stmt)
    try:
        run_cypher("CALL db.awaitIndexes(120)")
    except Exception:
        pass


def run_benchmark(
    strategy_name: str,
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
        "--body-length",
        str(body_length),
        "--mine-loops",
        str(mine_loops),
        "--infer-loops",
        str(infer_loops),
        "--top-k",
        str(top_k),
        "--min-support",
        str(min_support),
        "--min-pca",
        str(min_pca),
        "--out",
        output_json,
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stdout_lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        if line:
            print(f"[index-bench:{strategy_name}] {line}")
            stdout_lines.append(line)
    stderr_text = ""
    if proc.stderr is not None:
        stderr_text = proc.stderr.read().strip()
    return_code = proc.wait()
    if return_code != 0:
        msg = stderr_text or "\n".join(stdout_lines)
        raise RuntimeError(msg)
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
            strategy.name,
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
