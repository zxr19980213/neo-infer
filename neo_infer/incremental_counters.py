from __future__ import annotations

from collections import defaultdict

Edge = tuple[str, str, str]


def length2_stat_delta(
    *,
    r1: str,
    r2: str,
    head: str,
    present: set[Edge],
    added: list[Edge],
    removed: list[Edge],
) -> tuple[int, int, int]:
    """Return (support, pca_denominator, head_count) deltas for one length-2 rule.

    The graph is already in the post-batch state. ``present`` is the current
    neighborhood of the changed endpoints. ``added`` edges are subtracted out
    of that neighborhood and ``removed`` edges are put back, which rebuilds
    the pre-batch neighborhood. Pair-set sizes on the two neighborhoods differ
    only for bindings the batch created or destroyed, so the difference is the
    event contribution without a full-graph recount.

    Changelog identity is one logical edge per (src, rel, dst). ``head_count``
    therefore moves by one per distinct head edge in the batch, matching that
    identity rather than parallel-relationship multiplicity.
    """
    relevant = {r1, r2, head}
    present_rel = {edge for edge in present if edge[1] in relevant}
    added_rel = [edge for edge in added if edge[1] in relevant]
    removed_rel = [edge for edge in removed if edge[1] in relevant]

    removed_set = set(removed_rel)
    # A removed edge is absent from the current graph. Drop it if a caller
    # still included it, so the previous neighborhood can reintroduce it once.
    present_rel -= removed_set
    added_in_graph = {edge for edge in added_rel if edge in present_rel}
    previous = (present_rel - added_in_graph) | removed_set

    support_now, pca_now = _pair_counts(present_rel, r1, r2, head)
    support_prev, pca_prev = _pair_counts(previous, r1, r2, head)
    head_delta = sum(1 for edge in added_in_graph if edge[1] == head) - sum(
        1 for edge in removed_set if edge[1] == head
    )
    return (support_now - support_prev, pca_now - pca_prev, head_delta)


def _pair_counts(edges: set[Edge], r1: str, r2: str, head: str) -> tuple[int, int]:
    outgoing: dict[tuple[str, str], set[str]] = defaultdict(set)
    for src, rel, dst in edges:
        outgoing[(rel, src)].add(dst)

    body_pairs: set[tuple[str, str]] = set()
    for (rel, src), mids in outgoing.items():
        if rel != r1:
            continue
        for mid in mids:
            for dst in outgoing.get((r2, mid), ()):
                body_pairs.add((src, dst))

    head_pairs = {(src, dst) for (rel, src), dsts in outgoing.items() if rel == head for dst in dsts}
    head_sources = {src for rel, src in outgoing if rel == head}
    support = len(body_pairs & head_pairs)
    pca = sum(1 for src, _dst in body_pairs if src in head_sources)
    return support, pca
