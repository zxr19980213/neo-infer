from neo_infer.anyburl import rules_from_walks
from neo_infer.evaluation import precision_recall
from neo_infer.models import Rule


def test_precision_recall_on_heldout_edges():
    predicted = {("a", "nationality", "china"), ("b", "nationality", "japan")}
    heldout = {("a", "nationality", "china"), ("c", "nationality", "china")}
    scores = precision_recall(predicted, heldout)
    assert scores["precision"] == 0.5
    assert scores["recall"] == 0.5
    assert scores["hits"] == 1.0


def test_anyburl_walk_becomes_a_path_rule():
    rules = rules_from_walks([
        ["bornIn", "locatedIn", "nationality"],
        ["^parent", "parent", "sibling"],
        ["bornIn"],
    ])
    assert rules == [
        (("bornIn", "locatedIn"), "nationality"),
        (("^parent", "parent"), "sibling"),
    ]


def test_inverse_atom_renders_swapped_arguments():
    rule = Rule(
        rule_id="r",
        body_relations=("^parent", "bornIn"),
        head_relation="nationality",
        support=1,
        pca_confidence=1.0,
        head_coverage=1.0,
        status="discovered",
        version=1,
    )
    assert "parent(Z1,X)" in rule.text
    assert "bornIn(Z1,Y)" in rule.text
