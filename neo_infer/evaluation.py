from __future__ import annotations

Edge = tuple[str, str, str]


def precision_recall(predicted: set[Edge], heldout: set[Edge]) -> dict[str, float]:
    """Score predicted head edges against a held-out edge set."""
    hits = predicted & heldout
    precision = len(hits) / len(predicted) if predicted else 0.0
    recall = len(hits) / len(heldout) if heldout else 0.0
    return {"precision": precision, "recall": recall, "hits": float(len(hits))}
