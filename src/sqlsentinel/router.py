"""Risk-based routing: auto-execute or send to human review.

CLAUDE.md section 6. Two independent gates:

  * Risk overrides -- deterministic rules that force REVIEW regardless of
    confidence. Safety is not probabilistic: a DELETE does not become safe
    because the model felt sure about it.
  * Confidence threshold -- a reported tunable, not a magic constant. The
    deliverable is the routing curve (% routed vs % of errors caught) at every
    threshold, so the operating point is a choice the reader can see.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

import numpy as np

from sqlsentinel.executor import UnsafeQueryError, assert_read_only

DEFAULT_THRESHOLD = 0.5
LARGE_RESULT_ROWS = 1000


class Decision(StrEnum):
    AUTO = "AUTO"
    REVIEW = "REVIEW"


@dataclass
class RoutingDecision:
    decision: Decision
    confidence: float
    reasons: list[str] = field(default_factory=list)

    @property
    def auto(self) -> bool:
        return self.decision is Decision.AUTO


_NO_WHERE = re.compile(r"^\s*SELECT\b(?!.*\bWHERE\b).*\bFROM\b", re.I | re.S)


@dataclass
class Router:
    threshold: float = DEFAULT_THRESHOLD
    large_result_rows: int = LARGE_RESULT_ROWS
    enforce_risk_rules: bool = True

    def route(self, sql: str, confidence: float, *, row_count: int = 0,
              executed_ok: bool = True, n_tables: int = 0) -> RoutingDecision:
        reasons: list[str] = []

        if self.enforce_risk_rules:
            try:
                assert_read_only(sql)
            except UnsafeQueryError as e:
                # Non-SELECT is an unconditional override.
                return RoutingDecision(Decision.REVIEW, confidence, [f"unsafe statement: {e}"])

            if not sql.strip():
                return RoutingDecision(Decision.REVIEW, confidence, ["empty query"])
            if not executed_ok:
                reasons.append("query failed to execute")
            if row_count > self.large_result_rows:
                reasons.append(f"large result set ({row_count} rows)")
            if _NO_WHERE.match(sql) and n_tables and row_count > self.large_result_rows:
                reasons.append("unfiltered scan returning many rows")

        if confidence < self.threshold:
            reasons.append(f"confidence {confidence:.2f} below threshold {self.threshold:.2f}")

        decision = Decision.REVIEW if reasons else Decision.AUTO
        return RoutingDecision(decision, confidence, reasons)


def routing_curve(confidences, correct, thresholds=None) -> list[dict]:
    """The money metric (CLAUDE.md section 6).

    For each threshold: what fraction of queries go to review, and what
    fraction of all incorrect queries that catches. "Routed 20% and caught 60%
    of errors" is the sentence this table exists to support.
    """
    confidences = np.asarray(confidences, dtype=float)
    correct = np.asarray(correct, dtype=int)
    incorrect_total = int((correct == 0).sum())
    n = len(confidences) or 1

    if thresholds is None:
        thresholds = np.round(np.arange(0.0, 1.01, 0.05), 2)

    rows = []
    for t in thresholds:
        routed = confidences < t
        n_routed = int(routed.sum())
        caught = int(((correct == 0) & routed).sum())
        auto = ~routed
        rows.append({
            "threshold": float(t),
            "pct_routed": 100.0 * n_routed / n,
            "pct_errors_caught": (100.0 * caught / incorrect_total) if incorrect_total else 0.0,
            "n_routed": n_routed,
            "errors_caught": caught,
            # accuracy of what is still executed without a human
            "auto_accuracy": (100.0 * correct[auto].mean()) if auto.sum() else float("nan"),
            "n_auto": int(auto.sum()),
        })
    return rows


def best_operating_point(curve, max_pct_routed: float = 25.0) -> dict | None:
    """The threshold catching the most errors within a review budget."""
    feasible = [r for r in curve if r["pct_routed"] <= max_pct_routed and r["n_routed"] > 0]
    return max(feasible, key=lambda r: r["pct_errors_caught"]) if feasible else None
