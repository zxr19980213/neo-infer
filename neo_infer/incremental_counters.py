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
    present_rel, previous, added_in_graph, removed_set = _batch_states(
        relevant={r1, r2, head},
        present=present,
        added=added,
        removed=removed,
    )
    support_now, pca_now = _pair_counts(present_rel, r1, r2, head)
    support_prev, pca_prev = _pair_counts(previous, r1, r2, head)
    return (
        support_now - support_prev,
        pca_now - pca_prev,
        _head_delta(added_in_graph, removed_set, head),
    )


def length3_stat_delta(
    *,
    r1: str,
    r2: str,
    r3: str,
    head: str,
    present: set[Edge],
    added: list[Edge],
    removed: list[Edge],
) -> tuple[int, int, int]:
    """Return (support, pca_denominator, head_count) deltas for one length-3 rule.

    Body bindings are ``r1(X,A) ∧ r2(A,B) ∧ r3(B,Y)``. Support counts distinct
    ``(X,Y)`` that also have a head edge. PCA counts distinct ``(X,Y)`` whose
    ``X`` has any head edge. The batch arithmetic is the same set difference
    as length 2: the caller passes the post-batch neighborhood, and removed
    edges are restored into the previous neighborhood.

    A body path uses three distinct logical edges. ``head_count`` still moves
    by one per distinct head edge in the batch.
    """
    present_rel, previous, added_in_graph, removed_set = _batch_states(
        relevant={r1, r2, r3, head},
        present=present,
        added=added,
        removed=removed,
    )
    support_now, pca_now = _triple_counts(present_rel, r1, r2, r3, head)
    support_prev, pca_prev = _triple_counts(previous, r1, r2, r3, head)
    return (
        support_now - support_prev,
        pca_now - pca_prev,
        _head_delta(added_in_graph, removed_set, head),
    )


def _batch_states(
    *,
    relevant: set[str],
    present: set[Edge],
    added: list[Edge],
    removed: list[Edge],
) -> tuple[set[Edge], set[Edge], set[Edge], set[Edge]]:
    present_rel = {edge for edge in present if edge[1] in relevant}
    added_rel = [edge for edge in added if edge[1] in relevant]
    removed_set = {edge for edge in removed if edge[1] in relevant}
    # A removed edge is absent from the current graph. Drop it if a caller
    # still included it, so the previous neighborhood can reintroduce it once.
    present_rel -= removed_set
    added_in_graph = {edge for edge in added_rel if edge in present_rel}
    previous = (present_rel - added_in_graph) | removed_set
    return present_rel, previous, added_in_graph, removed_set


def _head_delta(added_in_graph: set[Edge], removed: set[Edge], head: str) -> int:
    return sum(1 for _src, rel, _dst in added_in_graph if rel == head) - sum(
        1 for _src, rel, _dst in removed if rel == head
    )


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


def _triple_counts(
    edges: set[Edge],
    r1: str,
    r2: str,
    r3: str,
    head: str,
) -> tuple[int, int]:
    by_src_rel: dict[tuple[str, str], list[Edge]] = defaultdict(list)
    for edge in edges:
        by_src_rel[(edge[1], edge[0])].append(edge)

    body_pairs: set[tuple[str, str]] = set()
    for first in edges:
        if first[1] != r1:
            continue
        source, _, mid = first
        for second in by_src_rel.get((r2, mid), ()):
            if second == first:
                continue
            for third in by_src_rel.get((r3, second[2]), ()):
                if third == first or third == second:
                    continue
                body_pairs.add((source, third[2]))

    head_pairs = {(src, dst) for src, rel, dst in edges if rel == head}
    head_sources = {src for src, rel, _dst in edges if rel == head}
    support = len(body_pairs & head_pairs)
    pca = sum(1 for src, _dst in body_pairs if src in head_sources)
    return support, pca


def length_n_stat_delta(
    *,
    body: list[str],
    head: str,
    present: set[Edge],
    added: list[Edge],
    removed: list[Edge],
) -> tuple[int, int, int]:
    """Event delta for a chain rule of any body length >= 2."""
    if len(body) == 2:
        return length2_stat_delta(
            r1=body[0], r2=body[1], head=head, present=present, added=added, removed=removed,
        )
    if len(body) == 3:
        return length3_stat_delta(
            r1=body[0], r2=body[1], r3=body[2], head=head,
            present=present, added=added, removed=removed,
        )
    present_rel, previous, added_in_graph, removed_set = _batch_states(
        relevant={*body, head},
        present=present,
        added=added,
        removed=removed,
    )
    support_now, pca_now = _chain_counts(present_rel, body, head)
    support_prev, pca_prev = _chain_counts(previous, body, head)
    return (
        support_now - support_prev,
        pca_now - pca_prev,
        _head_delta(added_in_graph, removed_set, head),
    )


def _chain_counts(edges: set[Edge], body: list[str], head: str) -> tuple[int, int]:
    by_src_rel: dict[tuple[str, str], list[Edge]] = defaultdict(list)
    for edge in edges:
        by_src_rel[(edge[1], edge[0])].append(edge)
    body_pairs: set[tuple[str, str]] = set()

    def walk(index: int, node: str, used: set[Edge], source: str) -> None:
        if index == len(body):
            body_pairs.add((source, node))
            return
        for edge in by_src_rel.get((body[index], node), ()):
            if edge in used:
                continue
            walk(index + 1, edge[2], used | {edge}, source)

    for edge in edges:
        if edge[1] != body[0]:
            continue
        walk(1, edge[2], {edge}, edge[0])

    head_pairs = {(src, dst) for src, rel, dst in edges if rel == head}
    head_sources = {src for src, rel, _dst in edges if rel == head}
    return len(body_pairs & head_pairs), sum(1 for src, _dst in body_pairs if src in head_sources)
