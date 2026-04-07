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


class Rule(BaseModel):
    rule_id: str
    body_relations: tuple[str, str]
    head_relation: str
    support: int = Field(ge=0)
    pca_confidence: float = Field(ge=0.0)
    head_coverage: float = Field(ge=0.0)
    status: RuleStatus = "discovered"
    version: int = Field(default=1, ge=1)

    @property
    def text(self) -> str:
        r1, r2 = self.body_relations
        return f"{r1}(X,Z) ∧ {r2}(Z,Y) -> {self.head_relation}(X,Y)"


class MineRulesRequest(BaseModel):
    limit: int = Field(default=200, ge=1, le=10000)
    min_support: int | None = Field(default=None, ge=0)
    min_pca_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    min_head_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    candidate_limit: int | None = Field(default=None, ge=1, le=50000)


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


class InferenceResponse(BaseModel):
    results: list[ApplyRuleResult]
    total_created: int = Field(ge=0)
    iterations_run: int = Field(ge=0)

