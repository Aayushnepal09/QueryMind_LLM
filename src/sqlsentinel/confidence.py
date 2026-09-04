"""Calibrated confidence scoring (CLAUDE.md section 6).

Two versions, both shipped so the delta between them is reportable:

  v1  agreement rate alone. Generate k candidates at temperature 0.7, execute
      each, normalise the result sets, and take the fraction agreeing with the
      modal result. Nearly free and the single strongest signal.

  v2  a calibrated logistic model over agreement plus per-query features.

The calibration data comes from the `calib` split, which is disjoint from
`eval_500` by construction. CLAUDE.md calls training and reporting on the same
questions "the one mistake that would invalidate the whole project", so the
split enforces it structurally rather than by discipline: `fit()` refuses data
containing evaluation question ids.
"""

from __future__ import annotations

import itertools
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np

FEATURE_NAMES = [
    "agreement_rate",
    "n_tables_referenced",
    "has_aggregation",
    "has_subquery",
    "has_nested_select",
    "execution_errored",
    "n_correction_rounds",
    "result_row_count_log",
    "result_empty",
    "question_length",
    "evidence_provided",
    "schema_coverage",
    "sql_length",
    "n_joins",
]


@dataclass
class QueryFeatures:
    agreement_rate: float = 1.0
    n_tables_referenced: float = 0.0
    has_aggregation: float = 0.0
    has_subquery: float = 0.0
    has_nested_select: float = 0.0
    execution_errored: float = 0.0
    n_correction_rounds: float = 0.0
    result_row_count_log: float = 0.0
    result_empty: float = 0.0
    question_length: float = 0.0
    evidence_provided: float = 0.0
    schema_coverage: float = 0.0
    sql_length: float = 0.0
    n_joins: float = 0.0

    def vector(self) -> list[float]:
        d = asdict(self)
        return [d[n] for n in FEATURE_NAMES]


_AGG = re.compile(r"\b(COUNT|SUM|AVG|MAX|MIN|GROUP\s+BY|HAVING)\b", re.I)
_SUBQ = re.compile(r"\(\s*SELECT\b", re.I)
_JOIN = re.compile(r"\bJOIN\b", re.I)
_FROM_TABLES = re.compile(r"\b(?:FROM|JOIN)\s+[`\"\[]?(\w+)", re.I)


def extract_features(trace, question: str, evidence: str, n_schema_tables: int) -> QueryFeatures:
    """Per-query features from an AgentTrace. Pure function of the trace."""
    sql = trace.sql or ""
    tables = {t.lower() for t in _FROM_TABLES.findall(sql)}
    return QueryFeatures(
        agreement_rate=float(trace.agreement_rate),
        n_tables_referenced=float(len(tables)),
        has_aggregation=float(bool(_AGG.search(sql))),
        has_subquery=float(bool(_SUBQ.search(sql))),
        has_nested_select=float(sql.upper().count("SELECT") > 1),
        execution_errored=float(trace.execution_errored_pre_correction),
        n_correction_rounds=float(trace.n_correction_rounds),
        result_row_count_log=float(np.log1p(max(trace.result_row_count, 0))),
        result_empty=float(trace.executed_ok and trace.result_row_count == 0),
        question_length=float(len(question.split())),
        evidence_provided=float(bool(evidence and evidence.strip())),
        # under- or over-selection of tables relative to what was offered
        schema_coverage=(len(tables) / n_schema_tables) if n_schema_tables else 0.0,
        sql_length=float(len(sql.split())),
        n_joins=float(len(_JOIN.findall(sql))),
    )


def agreement_confidence(features: QueryFeatures) -> float:
    """v1: agreement rate, floored when execution failed outright."""
    if features.execution_errored and features.n_correction_rounds == 0:
        return 0.0
    return float(np.clip(features.agreement_rate, 0.0, 1.0))


class ConfidenceModel:
    """v2: calibrated logistic regression over the feature set."""

    def __init__(self) -> None:
        self.model = None
        self.feature_names = list(FEATURE_NAMES)
        self.trained_on: list[int] = []

    def fit(
        self,
        features: list[QueryFeatures],
        correct: list[int],
        question_ids: list[int],
        forbidden_ids: set[int] | None = None,
    ) -> ConfidenceModel:
        """Fit on calibration data.

        `forbidden_ids` is the evaluation split. Passing any of it raises --
        this is the leakage guard, enforced in code rather than by convention.
        """
        if forbidden_ids:
            overlap = set(question_ids) & forbidden_ids
            if overlap:
                raise ValueError(
                    f"LEAKAGE: {len(overlap)} training questions are in the "
                    f"evaluation split (e.g. {sorted(overlap)[:5]}). The confidence "
                    f"model must never be fitted on questions it is reported on."
                )

        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        X = np.array([f.vector() for f in features])
        y = np.array(correct)
        if len(set(y.tolist())) < 2:
            raise ValueError("need both correct and incorrect examples to calibrate")

        base = make_pipeline(
            StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced")
        )
        n_splits = max(2, min(5, int(np.bincount(y).min())))
        self.model = CalibratedClassifierCV(base, method="isotonic", cv=n_splits)
        self.model.fit(X, y)
        self.trained_on = sorted(question_ids)
        return self

    def predict(self, features: QueryFeatures | list[QueryFeatures]):
        if self.model is None:
            raise RuntimeError("model not fitted")
        single = isinstance(features, QueryFeatures)
        X = np.array([f.vector() for f in ([features] if single else features)])
        p = self.model.predict_proba(X)[:, 1]
        return float(p[0]) if single else p

    def save(self, path: str | Path) -> None:
        import pickle

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as fh:
            pickle.dump(
                {
                    "model": self.model,
                    "features": self.feature_names,
                    "trained_on": self.trained_on,
                },
                fh,
            )

    @classmethod
    def load(cls, path: str | Path) -> ConfidenceModel:
        import pickle

        with Path(path).open("rb") as fh:
            blob = pickle.load(fh)
        m = cls()
        m.model, m.feature_names, m.trained_on = (
            blob["model"],
            blob["features"],
            blob.get("trained_on", []),
        )
        return m


# ------------------------------------------------------------------ calibration


def brier_score(probs, correct) -> float:
    """Mean squared error of the probabilities. Lower is better; 0.25 is the
    score of always guessing 0.5."""
    return float(np.mean((np.asarray(probs) - np.asarray(correct)) ** 2))


def reliability_curve(probs, correct, n_bins: int = 10):
    """Observed accuracy per predicted-probability bin.

    A well-calibrated model puts points on the diagonal: of the queries it
    called 0.6, about 60% should be correct. CLAUDE.md section 6: a
    well-calibrated 0.6 is more useful than an overconfident 0.9.
    """
    probs, correct = np.asarray(probs), np.asarray(correct)
    edges = np.linspace(0, 1, n_bins + 1)
    out = []
    for lo, hi in itertools.pairwise(edges):
        m = (probs >= lo) & (probs < hi if hi < 1 else probs <= hi)
        if m.sum():
            out.append(
                {
                    "bin_lower": float(lo),
                    "bin_upper": float(hi),
                    "mean_predicted": float(probs[m].mean()),
                    "observed_accuracy": float(correct[m].mean()),
                    "count": int(m.sum()),
                }
            )
    return out


def expected_calibration_error(probs, correct, n_bins: int = 10) -> float:
    """Weighted mean gap between predicted and observed accuracy."""
    curve = reliability_curve(probs, correct, n_bins)
    n = len(probs) or 1
    return float(
        sum(b["count"] / n * abs(b["mean_predicted"] - b["observed_accuracy"]) for b in curve)
    )


def save_calibration_report(path, probs, correct, label: str) -> dict:
    report = {
        "label": label,
        "n": len(probs),
        "brier_score": brier_score(probs, correct),
        "expected_calibration_error": expected_calibration_error(probs, correct),
        "base_accuracy": float(np.mean(correct)),
        "reliability_curve": reliability_curve(probs, correct),
    }
    Path(path).write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
