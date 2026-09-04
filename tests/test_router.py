import numpy as np
import pytest

from sqlsentinel.router import (
    Decision,
    Router,
    best_operating_point,
    routing_curve,
)


def test_high_confidence_select_auto_executes():
    d = Router(threshold=0.5).route("SELECT a FROM t WHERE b=1", 0.9, row_count=10)
    assert d.decision is Decision.AUTO and d.auto


def test_low_confidence_routes_to_review():
    d = Router(threshold=0.5).route("SELECT a FROM t WHERE b=1", 0.2, row_count=10)
    assert d.decision is Decision.REVIEW
    assert "below threshold" in d.reasons[0]


@pytest.mark.parametrize(
    "sql", ["DELETE FROM t", "DROP TABLE t", "UPDATE t SET a=1", "SELECT 1; DROP TABLE t"]
)
def test_non_select_always_reviewed_even_at_full_confidence(sql):
    """Safety rules are not probabilistic (spec §6)."""
    d = Router(threshold=0.5).route(sql, 1.0)
    assert d.decision is Decision.REVIEW


def test_large_result_reviewed_despite_confidence():
    d = Router(threshold=0.5, large_result_rows=100).route(
        "SELECT * FROM t WHERE x=1", 0.99, row_count=5000
    )
    assert d.decision is Decision.REVIEW
    assert any("large result" in r for r in d.reasons)


def test_failed_execution_reviewed():
    d = Router().route("SELECT a FROM t", 0.99, executed_ok=False)
    assert d.decision is Decision.REVIEW


def test_empty_query_reviewed():
    assert Router().route("", 1.0).decision is Decision.REVIEW


def test_risk_rules_can_be_disabled_for_ablation():
    d = Router(threshold=0.5, enforce_risk_rules=False).route(
        "SELECT * FROM t", 0.99, row_count=10**6
    )
    assert d.decision is Decision.AUTO


# ------------------------------------------------------------------ routing curve


def _population(n=200, seed=1):
    rng = np.random.default_rng(seed)
    conf = rng.uniform(0, 1, n)
    correct = (rng.uniform(size=n) < conf).astype(int)
    return conf, correct


def test_curve_is_monotonic_in_routing_volume():
    conf, correct = _population()
    curve = routing_curve(conf, correct)
    routed = [r["pct_routed"] for r in curve]
    assert routed == sorted(routed)


def test_threshold_zero_routes_nothing_and_one_routes_all():
    conf, correct = _population()
    curve = routing_curve(conf, correct, thresholds=[0.0, 1.0])
    assert curve[0]["pct_routed"] == 0.0
    assert curve[-1]["pct_routed"] == 100.0
    assert curve[-1]["pct_errors_caught"] == 100.0


def test_useful_signal_catches_errors_faster_than_it_routes():
    """With a real signal, routing X% should catch more than X% of errors."""
    conf, correct = _population(500)
    curve = routing_curve(conf, correct)
    mid = [r for r in curve if 15 <= r["pct_routed"] <= 40]
    assert mid, "no thresholds in the useful band"
    assert any(r["pct_errors_caught"] > r["pct_routed"] for r in mid)


def test_auto_accuracy_rises_as_more_is_routed():
    conf, correct = _population(500)
    curve = routing_curve(conf, correct)
    lo = next(r for r in curve if r["threshold"] == 0.1)
    hi = next(r for r in curve if r["threshold"] == 0.6)
    assert hi["auto_accuracy"] > lo["auto_accuracy"]


def test_best_operating_point_respects_budget():
    conf, correct = _population(500)
    pt = best_operating_point(routing_curve(conf, correct), max_pct_routed=25.0)
    assert pt is not None and pt["pct_routed"] <= 25.0


def test_best_operating_point_none_when_budget_impossible():
    conf, correct = _population(100)
    assert best_operating_point(routing_curve(conf, correct), max_pct_routed=0.0) is None
