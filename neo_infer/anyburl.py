from __future__ import annotations


def rules_from_walks(walks: list[list[str]]) -> list[tuple[tuple[str, ...], str]]:
    """Turn a random-walk relation sequence into a path rule.

    Each walk is body relations followed by the closing head relation.
    Inverse hops use a leading ``^``.
    """
    found: list[tuple[tuple[str, ...], str]] = []
    seen: set[tuple[tuple[str, ...], str]] = set()
    for walk in walks:
        rels = [item.strip() for item in walk if item and item.strip()]
        if len(rels) < 2:
            continue
        rule = (tuple(rels[:-1]), rels[-1])
        if rule in seen:
            continue
        seen.add(rule)
        found.append(rule)
    return found
