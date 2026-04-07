from __future__ import annotations

import re
from typing import Literal, Sequence

from pydantic import BaseModel, Field

RuleStatus = Literal["discovered", "adopted", "applied", "rejected"]


def build_rule_id(body_relations: Sequence[str], head_relation: str) -> str:
    body = "__".join(_normalize_token(rel) for rel in body_relations)
    head = _normalize_token(head_relation)
    return f"rule__{body}__to__{head}"


def _normalize_token(token: str) -> str:
    value = token.strip().lower()
    return re.sub(r"[^a-z0-9_]+", "_", value).strip("_")


def normalize_relation_token(token: str) -> str:
    return token.strip().replace("`", "")


class Rule(BaseModel):
    rule_id: str
    body_relations: tuple[str, ...]
    head_relation: str
    support: int = Field(ge=0)
    pca_confidence: float = Field(ge=0.0)
    head_coverage: float = Field(ge=0.0)
    status: RuleStatus = "discovered"
    version: int = Field(default=1, ge=1)

    @property
    def text(self) -> str:
        if len(self.body_relations) == 2:
            r1, r2 = self.body_relations
            return f"{r1}(X,Z) ∧ {r2}(Z,Y) -> {self.head_relation}(X,Y)"

        middle_nodes = [f"Z{i}" for i in range(1, len(self.body_relations))]
        vars_chain = ["X", *middle_nodes, "Y"]
        atoms: list[str] = []
        for idx, rel in enumerate(self.body_relations):
            atoms.append(f"{rel}({vars_chain[idx]},{vars_chain[idx + 1]})")
        body_text = " ∧ ".join(atoms)
        return f"{body_text} -> {self.head_relation}(X,Y)"


class MineRulesRequest(BaseModel):
    limit: int = Field(default=200, ge=1, le=10000)
    min_support: int | None = Field(default=None, ge=0)
    min_pca_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    min_head_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    candidate_limit: int | None = Field(default=None, ge=1, le=50000)
    body_length: int = Field(default=2, ge=2, le=3)
    changed_relations: list[str] | None = None


class MineRulesResponse(BaseModel):
    rules: list[Rule]


class RuleSetResponse(BaseModel):
    rules: list[Rule]


class ApplyRuleResult(BaseModel):
    rule_id: str
    created_triples: int = Field(ge=0)
    conflict_triples: int = Field(default=0, ge=0)
    iteration: int = Field(ge=1)


class InferenceRequest(BaseModel):
    limit_rules: int = Field(default=100, ge=1, le=10000)
    fixpoint: bool = False
    max_iterations: int = Field(default=5, ge=1, le=100)
    check_conflicts: bool = True
    # 可选：请求级冲突策略，优先级最高。
    # 例如 {"nationality": ["noNationality"]}。
    conflict_pairs: dict[str, list[str]] | None = None


class InferenceResponse(BaseModel):
    results: list[ApplyRuleResult]
    total_created: int = Field(ge=0)
    total_conflicts: int = Field(default=0, ge=0)
    iterations_run: int = Field(ge=0)


class ConflictPairsResponse(BaseModel):
    pairs: dict[str, list[str]]


class ConflictPairsUpdateRequest(BaseModel):
    pairs: dict[str, list[str]]


class IncrementalMineRequest(MineRulesRequest):
    affected_relations: list[str] = Field(min_length=1)


class ConflictCase(BaseModel):
    rule_id: str
    inferred_relation: str
    conflicting_relation: str
    source_x: str
    source_y: str
    detect_count: int = Field(ge=0)
    first_iteration: int = Field(ge=1)
    last_iteration: int = Field(ge=1)


class ConflictCaseListResponse(BaseModel):
    cases: list[ConflictCase]


# Backward-compatible alias.
ConflictCasesResponse = ConflictCaseListResponse

