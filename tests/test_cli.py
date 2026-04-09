from __future__ import annotations

import neo_infer.cli as cli


def test_cli_mine_builds_expected_payload(monkeypatch):
    captured: dict[str, object] = {}

    def fake_request_json(*, api_base, method, path, payload=None, timeout=30.0):
        captured["api_base"] = api_base
        captured["method"] = method
        captured["path"] = path
        captured["payload"] = payload
        captured["timeout"] = timeout
        return {"ok": True}

    monkeypatch.setattr(cli, "_request_json", fake_request_json)
    monkeypatch.setattr(cli, "_print_result", lambda payload: None)
    code = cli.main(
        [
            "--api-base",
            "http://127.0.0.1:8000",
            "mine",
            "--body-length",
            "3",
            "--limit",
            "123",
            "--min-support",
            "2",
            "--min-pca-confidence",
            "0.4",
            "--include-inferred",
        ]
    )
    assert code == 0
    assert captured["method"] == "POST"
    assert captured["path"] == "/rules/mine"
    assert captured["payload"] == {
        "body_length": 3,
        "limit": 123,
        "min_support": 2,
        "min_pca_confidence": 0.4,
        "factual_only": False,
        "confidence_ub_weight": 0.0,
    }


def test_cli_changes_append_parses_edges(monkeypatch):
    captured: dict[str, object] = {}

    def fake_request_json(*, api_base, method, path, payload=None, timeout=30.0):
        captured["api_base"] = api_base
        captured["method"] = method
        captured["path"] = path
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(cli, "_request_json", fake_request_json)
    monkeypatch.setattr(cli, "_print_result", lambda payload: None)
    code = cli.main(
        [
            "changes",
            "append",
            "--add",
            "u1,bornIn,u2",
            "--remove",
            "u2,locatedIn,u3",
            "--batch-id",
            "b1",
            "--idempotency-key",
            "i1",
            "--context",
            "actor=tester",
        ]
    )
    assert code == 0
    assert captured["method"] == "POST"
    assert captured["path"] == "/changes/append"
    assert captured["payload"] == {
        "added_edges": [{"src": "u1", "rel": "bornIn", "dst": "u2"}],
        "removed_edges": [{"src": "u2", "rel": "locatedIn", "dst": "u3"}],
        "batch_id": "b1",
        "idempotency_key": "i1",
        "context": {"actor": "tester"},
    }


def test_cli_incremental_consume_builds_expected_payload(monkeypatch):
    captured: dict[str, object] = {}

    def fake_request_json(*, api_base, method, path, payload=None, timeout=30.0):
        captured["method"] = method
        captured["path"] = path
        captured["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(cli, "_request_json", fake_request_json)
    monkeypatch.setattr(cli, "_print_result", lambda payload: None)
    code = cli.main(
        [
            "incremental",
            "consume",
            "--body-length",
            "2",
            "--limit",
            "80",
            "--change-limit",
            "500",
            "--min-support",
            "1",
            "--min-pca-confidence",
            "0.1",
            "--changed-relation",
            "bornIn",
            "--changed-relation",
            "locatedIn",
        ]
    )
    assert code == 0
    assert captured["method"] == "POST"
    assert captured["path"] == "/rules/mine/incremental/from-changelog"
    assert captured["payload"] == {
        "body_length": 2,
        "limit": 80,
        "change_limit": 500,
        "min_support": 1,
        "min_pca_confidence": 0.1,
        "factual_only": True,
        "confidence_ub_weight": 0.0,
        "changed_relations": ["bornIn", "locatedIn"],
    }


def test_cli_invalid_triplet_returns_error(monkeypatch):
    monkeypatch.setattr(cli, "_print_result", lambda payload: None)
    code = cli.main(["changes", "append", "--add", "badtriplet"])
    assert code == 1
