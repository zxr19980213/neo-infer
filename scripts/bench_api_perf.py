#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass

import httpx


@dataclass
class BenchResult:
    name: str
    seconds: float
    status_code: int
    extra: dict[str, object]


def timed_post(
    client: httpx.Client,
    base_url: str,
    path: str,
    payload: dict[str, object],
    name: str,
) -> BenchResult:
    start = time.perf_counter()
    resp = client.post(f"{base_url}{path}", json=payload)
    elapsed = time.perf_counter() - start
    extra: dict[str, object] = {}
    try:
        data = resp.json()
        if isinstance(data, dict):
            if "rules" in data and isinstance(data["rules"], list):
                extra["rules"] = len(data["rules"])
            if "results" in data and isinstance(data["results"], list):
                extra["results"] = len(data["results"])
            if "total_created" in data:
                extra["total_created"] = data["total_created"]
            if "processed_changes" in data:
                extra["processed_changes"] = data["processed_changes"]
    except Exception:
        pass
    return BenchResult(name=name, seconds=elapsed, status_code=resp.status_code, extra=extra)


def ensure_ok(result: BenchResult) -> None:
    if result.status_code != 200:
        raise RuntimeError(f"{result.name} failed with HTTP {result.status_code}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark mining/inference API latency.")
    parser.add_argument("--base-url", default=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("BENCH_TIMEOUT", "180")))
    parser.add_argument("--out", default=os.getenv("BENCH_OUT", "bench_results.json"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_url = args.base_url
    timeout = args.timeout
    out_path = args.out

    results: list[BenchResult] = []
    with httpx.Client(timeout=timeout) as client:
        health = client.get(f"{base_url}/health")
        if health.status_code != 200:
            raise RuntimeError(f"health failed with HTTP {health.status_code}")

        r1 = timed_post(
            client,
            base_url,
            "/rules/mine",
            {
                "body_length": 2,
                "limit": 500,
                "candidate_limit": 20000,
                "min_support": 5,
                "min_pca_confidence": 0.05,
            },
            "mine_length2",
        )
        ensure_ok(r1)
        results.append(r1)

        r2 = timed_post(
            client,
            base_url,
            "/rules/mine/length3",
            {
                "body_length": 3,
                "limit": 500,
                "candidate_limit": 30000,
                "min_support": 5,
                "min_pca_confidence": 0.05,
            },
            "mine_length3",
        )
        ensure_ok(r2)
        results.append(r2)

        rules_resp = client.get(f"{base_url}/rules", params={"status": "discovered", "limit": 200})
        if rules_resp.status_code != 200:
            raise RuntimeError(f"list rules failed with HTTP {rules_resp.status_code}")
        rules_data = rules_resp.json()
        rule_ids: list[str] = []
        for item in rules_data.get("rules", []):
            rid = item.get("rule_id")
            if isinstance(rid, str):
                rule_ids.append(rid)
        for rid in rule_ids[:20]:
            _ = client.post(f"{base_url}/rules/{rid}/adopt")

        r3 = timed_post(
            client,
            base_url,
            "/inference/run",
            {
                "limit_rules": 100,
                "fixpoint": True,
                "max_iterations": 5,
                "check_conflicts": False,
            },
            "inference_fixpoint",
        )
        ensure_ok(r3)
        results.append(r3)

        append_resp = client.post(
            f"{base_url}/changes/append",
            json={
                "added_edges": [
                    {"src": "bench_u1", "rel": "bornIn", "dst": "bench_u2"},
                    {"src": "bench_u2", "rel": "locatedIn", "dst": "bench_u3"},
                    {"src": "bench_u3", "rel": "partOf", "dst": "bench_u4"},
                ],
                "removed_edges": [],
            },
        )
        if append_resp.status_code != 200:
            raise RuntimeError(f"append changes failed with HTTP {append_resp.status_code}")

        r4 = timed_post(
            client,
            base_url,
            "/rules/mine/incremental/from-changelog",
            {
                "limit": 300,
                "change_limit": 5000,
                "body_length": 2,
                "min_support": 1,
                "min_pca_confidence": 0.01,
            },
            "incremental_from_changelog_len2",
        )
        ensure_ok(r4)
        results.append(r4)

    output = {
        "base_url": base_url,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "results": [
            {
                "name": r.name,
                "seconds": round(r.seconds, 6),
                "status_code": r.status_code,
                "extra": r.extra,
            }
            for r in results
        ],
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
