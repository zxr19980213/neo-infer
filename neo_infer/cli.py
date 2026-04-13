from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib import error, parse, request


class CliError(RuntimeError):
    """User-facing CLI error."""


def _normalize_base_url(base_url: str) -> str:
    value = base_url.strip()
    if not value:
        raise CliError("api base url is empty")
    return value.rstrip("/")


def _request_json(
    *,
    api_base: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> Any:
    base = _normalize_base_url(api_base)
    url = f"{base}{path}"
    headers = {"Accept": "application/json"}
    body: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    req = request.Request(url=url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            content_type = resp.headers.get("Content-Type", "")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        detail = raw.strip() or exc.reason
        raise CliError(f"{method} {path} failed with HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise CliError(f"{method} {path} failed: {exc.reason}") from exc

    if "application/json" in content_type:
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            pass
    return {"raw": raw}


def _parse_triplet(raw: str) -> dict[str, str]:
    parts = [item.strip() for item in raw.split(",")]
    if len(parts) != 3 or any(not item for item in parts):
        raise CliError(f"invalid edge triplet '{raw}', expected format: src,rel,dst")
    src, rel, dst = parts
    return {"src": src, "rel": rel, "dst": dst}


def _parse_context(items: list[str]) -> dict[str, str]:
    context: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise CliError(f"invalid --context '{item}', expected key=value")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise CliError(f"invalid --context '{item}', key cannot be empty")
        context[key] = value
    return context


def _print_result(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _cmd_health(args: argparse.Namespace) -> Any:
    return _request_json(api_base=args.api_base, method="GET", path="/health")


def _cmd_mine(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {
        "body_length": args.body_length,
        "limit": args.limit,
        "factual_only": not args.include_inferred,
        "confidence_ub_weight": args.confidence_ub_weight,
    }
    if args.min_support is not None:
        payload["min_support"] = args.min_support
    if args.min_pca_confidence is not None:
        payload["min_pca_confidence"] = args.min_pca_confidence
    if args.min_head_coverage is not None:
        payload["min_head_coverage"] = args.min_head_coverage
    if args.candidate_limit is not None:
        payload["candidate_limit"] = args.candidate_limit
    if args.beam_width is not None:
        payload["beam_width"] = args.beam_width
    if args.head_budget_per_relation is not None:
        payload["head_budget_per_relation"] = args.head_budget_per_relation
    return _request_json(api_base=args.api_base, method="POST", path="/rules/mine", payload=payload)


def _cmd_infer(args: argparse.Namespace) -> Any:
    payload = {
        "limit_rules": args.limit_rules,
        "fixpoint": args.fixpoint,
        "max_iterations": args.max_iterations,
        "check_conflicts": not args.no_conflicts,
    }
    return _request_json(api_base=args.api_base, method="POST", path="/inference/run", payload=payload)


def _cmd_rules_list(args: argparse.Namespace) -> Any:
    query: dict[str, Any] = {"limit": args.limit}
    if args.status:
        query["status"] = args.status
    qs = parse.urlencode(query)
    return _request_json(api_base=args.api_base, method="GET", path=f"/rules?{qs}")


def _cmd_rules_adopt(args: argparse.Namespace) -> Any:
    return _request_json(
        api_base=args.api_base,
        method="POST",
        path=f"/rules/{parse.quote(args.rule_id, safe='')}/adopt",
    )


def _cmd_rules_reject(args: argparse.Namespace) -> Any:
    return _request_json(
        api_base=args.api_base,
        method="POST",
        path=f"/rules/{parse.quote(args.rule_id, safe='')}/reject",
    )


def _cmd_changes_append(args: argparse.Namespace) -> Any:
    added = [_parse_triplet(item) for item in args.add]
    removed = [_parse_triplet(item) for item in args.remove]
    if not added and not removed:
        raise CliError("at least one --add or --remove edge is required")
    payload: dict[str, Any] = {"added_edges": added, "removed_edges": removed}
    if args.batch_id:
        payload["batch_id"] = args.batch_id
    if args.idempotency_key:
        payload["idempotency_key"] = args.idempotency_key
    if args.context:
        payload["context"] = _parse_context(args.context)
    return _request_json(api_base=args.api_base, method="POST", path="/changes/append", payload=payload)


def _cmd_incremental_consume(args: argparse.Namespace) -> Any:
    payload: dict[str, Any] = {
        "body_length": args.body_length,
        "limit": args.limit,
        "change_limit": args.change_limit,
        "factual_only": not args.include_inferred,
        "confidence_ub_weight": args.confidence_ub_weight,
    }
    if args.min_support is not None:
        payload["min_support"] = args.min_support
    if args.min_pca_confidence is not None:
        payload["min_pca_confidence"] = args.min_pca_confidence
    if args.min_head_coverage is not None:
        payload["min_head_coverage"] = args.min_head_coverage
    if args.candidate_limit is not None:
        payload["candidate_limit"] = args.candidate_limit
    if args.beam_width is not None:
        payload["beam_width"] = args.beam_width
    if args.head_budget_per_relation is not None:
        payload["head_budget_per_relation"] = args.head_budget_per_relation
    if args.changed_relation:
        payload["changed_relations"] = args.changed_relation
    return _request_json(
        api_base=args.api_base,
        method="POST",
        path="/rules/mine/incremental/from-changelog",
        payload=payload,
    )


def _cmd_trigger_install(args: argparse.Namespace) -> Any:
    return _request_json(api_base=args.api_base, method="POST", path="/triggers/changelog/install")


def _cmd_trigger_drop(args: argparse.Namespace) -> Any:
    return _request_json(api_base=args.api_base, method="DELETE", path="/triggers/changelog")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="neo-infer", description="Lightweight client for neo-infer API.")
    parser.add_argument(
        "--api-base",
        default="http://127.0.0.1:8000",
        help="neo-infer API base URL (default: http://127.0.0.1:8000)",
    )
    sub = parser.add_subparsers(dest="command")

    p_health = sub.add_parser("health", help="check API health")
    p_health.set_defaults(func=_cmd_health)

    p_mine = sub.add_parser("mine", help="mine rules")
    p_mine.add_argument("--body-length", type=int, choices=[2, 3, 4, 5], default=2)
    p_mine.add_argument("--limit", type=int, default=200)
    p_mine.add_argument("--min-support", type=int)
    p_mine.add_argument("--min-pca-confidence", type=float)
    p_mine.add_argument("--min-head-coverage", type=float)
    p_mine.add_argument("--candidate-limit", type=int)
    p_mine.add_argument("--beam-width", type=int)
    p_mine.add_argument("--head-budget-per-relation", type=int)
    p_mine.add_argument("--confidence-ub-weight", type=float, default=0.0)
    p_mine.add_argument(
        "--include-inferred",
        action="store_true",
        help="include inferred edges in mining statistics (default: factual only)",
    )
    p_mine.set_defaults(func=_cmd_mine)

    p_infer = sub.add_parser("infer", help="run rule inference")
    p_infer.add_argument("--limit-rules", type=int, default=100)
    p_infer.add_argument("--fixpoint", action="store_true")
    p_infer.add_argument("--max-iterations", type=int, default=5)
    p_infer.add_argument("--no-conflicts", action="store_true", help="disable conflict checking")
    p_infer.set_defaults(func=_cmd_infer)

    p_rules = sub.add_parser("rules", help="rule operations")
    rules_sub = p_rules.add_subparsers(dest="rules_command")

    p_rules_list = rules_sub.add_parser("list", help="list rules")
    p_rules_list.add_argument("--status", choices=["discovered", "adopted", "applied", "rejected"])
    p_rules_list.add_argument("--limit", type=int, default=100)
    p_rules_list.set_defaults(func=_cmd_rules_list)

    p_rules_adopt = rules_sub.add_parser("adopt", help="adopt one rule")
    p_rules_adopt.add_argument("rule_id")
    p_rules_adopt.set_defaults(func=_cmd_rules_adopt)

    p_rules_reject = rules_sub.add_parser("reject", help="reject one rule")
    p_rules_reject.add_argument("rule_id")
    p_rules_reject.set_defaults(func=_cmd_rules_reject)

    p_changes = sub.add_parser("changes", help="append changelog edges")
    changes_sub = p_changes.add_subparsers(dest="changes_command")
    p_changes_append = changes_sub.add_parser("append", help="append added/removed edges")
    p_changes_append.add_argument(
        "--add",
        action="append",
        default=[],
        metavar="SRC,REL,DST",
        help="added edge triplet (repeatable)",
    )
    p_changes_append.add_argument(
        "--remove",
        action="append",
        default=[],
        metavar="SRC,REL,DST",
        help="removed edge triplet (repeatable)",
    )
    p_changes_append.add_argument("--batch-id")
    p_changes_append.add_argument("--idempotency-key")
    p_changes_append.add_argument(
        "--context",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="context key/value (repeatable)",
    )
    p_changes_append.set_defaults(func=_cmd_changes_append)

    p_inc = sub.add_parser("incremental", help="incremental mining operations")
    inc_sub = p_inc.add_subparsers(dest="incremental_command")
    p_consume = inc_sub.add_parser("consume", help="consume changelog and run incremental mining")
    p_consume.add_argument("--body-length", type=int, choices=[2, 3, 4, 5], default=2)
    p_consume.add_argument("--limit", type=int, default=200)
    p_consume.add_argument("--change-limit", type=int, default=1000)
    p_consume.add_argument("--min-support", type=int)
    p_consume.add_argument("--min-pca-confidence", type=float)
    p_consume.add_argument("--min-head-coverage", type=float)
    p_consume.add_argument("--candidate-limit", type=int)
    p_consume.add_argument("--beam-width", type=int)
    p_consume.add_argument("--head-budget-per-relation", type=int)
    p_consume.add_argument("--confidence-ub-weight", type=float, default=0.0)
    p_consume.add_argument(
        "--changed-relation",
        action="append",
        default=[],
        help="optional changed relation hint (repeatable)",
    )
    p_consume.add_argument(
        "--include-inferred",
        action="store_true",
        help="include inferred edges in mining statistics (default: factual only)",
    )
    p_consume.set_defaults(func=_cmd_incremental_consume)

    p_trigger = sub.add_parser("trigger", help="manage APOC changelog trigger")
    trigger_sub = p_trigger.add_subparsers(dest="trigger_command")
    p_trigger_install = trigger_sub.add_parser("install", help="install changelog trigger")
    p_trigger_install.set_defaults(func=_cmd_trigger_install)
    p_trigger_drop = trigger_sub.add_parser("drop", help="drop changelog trigger")
    p_trigger_drop.set_defaults(func=_cmd_trigger_drop)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help(sys.stderr)
        return 2
    try:
        payload = func(args)
    except CliError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    _print_result(payload)
    return 0


def run() -> None:
    raise SystemExit(main())

