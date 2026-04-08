#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from statistics import mean

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


def wait_health(client: httpx.Client, base_url: str, retries: int, interval: float) -> None:
    last_code: int | None = None
    for i in range(1, retries + 1):
        try:
            health = client.get(f"{base_url}/health")
            last_code = health.status_code
            if health.status_code == 200:
                return
        except httpx.HTTPError:
            last_code = None
        time.sleep(interval)
    if last_code is None:
        raise RuntimeError(f"health check failed after {retries} retries (network error)")
    raise RuntimeError(f"health failed after {retries} retries with HTTP {last_code}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark mining/inference API latency.")
    parser.add_argument(
        "--base-url",
        "--api-base-url",
        dest="base_url",
        default=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument("--timeout", type=float, default=float(os.getenv("BENCH_TIMEOUT", "180")))
    parser.add_argument("--out", "--output-json", dest="out", default=os.getenv("BENCH_OUT", "bench_results.json"))
    parser.add_argument("--health-retries", type=int, default=int(os.getenv("BENCH_HEALTH_RETRIES", "30")))
    parser.add_argument("--health-interval", type=float, default=float(os.getenv("BENCH_HEALTH_INTERVAL", "1.0")))
    parser.add_argument("--body-length", type=int, choices=[2, 3], default=int(os.getenv("BENCH_BODY_LENGTH", "2")))
    parser.add_argument("--mine-loops", type=int, default=int(os.getenv("BENCH_MINE_LOOPS", "1")))
    parser.add_argument("--infer-loops", type=int, default=int(os.getenv("BENCH_INFER_LOOPS", "1")))
    parser.add_argument("--top-k", type=int, default=int(os.getenv("BENCH_TOP_K", "500")))
    parser.add_argument("--min-support", type=int, default=int(os.getenv("BENCH_MIN_SUPPORT", "5")))
    parser.add_argument("--min-pca", type=float, default=float(os.getenv("BENCH_MIN_PCA", "0.05")))
    parser.add_argument("--mine-limit", type=int, default=int(os.getenv("BENCH_MINE_LIMIT", "500")))
    parser.add_argument(
        "--mine-candidate-limit",
        type=int,
        default=int(os.getenv("BENCH_MINE_CANDIDATE_LIMIT", "20000")),
    )
    parser.add_argument(
        "--mine3-candidate-limit",
        type=int,
        default=int(os.getenv("BENCH_MINE3_CANDIDATE_LIMIT", "30000")),
    )
    parser.add_argument("--infer-limit-rules", type=int, default=int(os.getenv("BENCH_INFER_LIMIT_RULES", "100")))
    parser.add_argument(
        "--incremental-change-limit",
        type=int,
        default=int(os.getenv("BENCH_INCREMENTAL_CHANGE_LIMIT", "5000")),
    )
    parser.add_argument("--adopt-rules", type=int, default=int(os.getenv("BENCH_ADOPT_RULES", "20")))
    parser.add_argument(
        "--skip-incremental",
        action="store_true",
        help="Skip append + from-changelog benchmark step.",
    )
    return parser.parse_args()


def p95(values: list[float]) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    idx = int(0.95 * (len(arr) - 1))
    return arr[idx]


def main() -> None:
    args = parse_args()
    base_url = args.base_url
    timeout = args.timeout
    out_path = args.out

    results: list[BenchResult] = []
    mine_times: list[float] = []
    infer_times: list[float] = []
    incr_times: list[float] = []
    with httpx.Client(timeout=timeout) as client:
        wait_health(client, base_url, retries=args.health_retries, interval=args.health_interval)

        mine_path = "/rules/mine" if args.body_length == 2 else "/rules/mine/length3"
        mine_candidate_limit = args.mine_candidate_limit if args.body_length == 2 else args.mine3_candidate_limit
        for i in range(args.mine_loops):
            mine_result = timed_post(
                client,
                base_url,
                mine_path,
                {
                    "body_length": args.body_length,
                    "limit": args.top_k,
                    "candidate_limit": mine_candidate_limit,
                    "min_support": args.min_support,
                    "min_pca_confidence": args.min_pca,
                },
                f"mine_length{args.body_length}_loop{i + 1}",
            )
            ensure_ok(mine_result)
            results.append(mine_result)
            mine_times.append(mine_result.seconds)

        rules_resp = client.get(f"{base_url}/rules", params={"status": "discovered", "limit": 200})
        if rules_resp.status_code != 200:
            raise RuntimeError(f"list rules failed with HTTP {rules_resp.status_code}")
        rules_data = rules_resp.json()
        rule_ids: list[str] = []
        for item in rules_data.get("rules", []):
            rid = item.get("rule_id")
            if isinstance(rid, str):
                rule_ids.append(rid)
        for rid in rule_ids[: args.adopt_rules]:
            _ = client.post(f"{base_url}/rules/{rid}/adopt")

        for i in range(args.infer_loops):
            infer_result = timed_post(
                client,
                base_url,
                "/inference/run",
                {
                    "limit_rules": args.infer_limit_rules,
                    "fixpoint": True,
                    "max_iterations": 5,
                    "check_conflicts": False,
                },
                f"inference_fixpoint_loop{i + 1}",
            )
            ensure_ok(infer_result)
            results.append(infer_result)
            infer_times.append(infer_result.seconds)

        if not args.skip_incremental:
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

            incr_result = timed_post(
                client,
                base_url,
                "/rules/mine/incremental/from-changelog",
                {
                    "limit": 300,
                    "change_limit": args.incremental_change_limit,
                    "body_length": 2,
                    "min_support": 1,
                    "min_pca_confidence": 0.01,
                },
                "incremental_from_changelog_len2",
            )
            ensure_ok(incr_result)
            results.append(incr_result)
            incr_times.append(incr_result.seconds)

    mine_summary = {
        "runs": len(mine_times),
        "mean_ms": round(mean(mine_times) * 1000, 3) if mine_times else 0.0,
        "p95_ms": round(p95(mine_times) * 1000, 3) if mine_times else 0.0,
        "max_ms": round(max(mine_times) * 1000, 3) if mine_times else 0.0,
    }
    infer_summary = {
        "runs": len(infer_times),
        "mean_ms": round(mean(infer_times) * 1000, 3) if infer_times else 0.0,
        "p95_ms": round(p95(infer_times) * 1000, 3) if infer_times else 0.0,
        "max_ms": round(max(infer_times) * 1000, 3) if infer_times else 0.0,
    }
    incr_summary = {
        "runs": len(incr_times),
        "mean_ms": round(mean(incr_times) * 1000, 3) if incr_times else 0.0,
        "p95_ms": round(p95(incr_times) * 1000, 3) if incr_times else 0.0,
        "max_ms": round(max(incr_times) * 1000, 3) if incr_times else 0.0,
    }

    output = {
        "base_url": base_url,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "mine": mine_summary,
        "inference": infer_summary,
        "incremental": incr_summary,
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
