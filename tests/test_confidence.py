import numpy as np
import pytest

from sqlsentinel.confidence import (
    ConfidenceModel,
    QueryFeatures,
    agreement_confidence,
    brier_score,
    expected_calibration_error,
    extract_features,
    reliability_curve,
)


class FakeTrace:
    def __init__(self, **kw):
        self.sql = kw.get("sql", "SELECT a FROM t")
        self.agreement_rate = kw.get("agreement_rate", 1.0)
        self.execution_errored_pre_correction = kw.get("errored", False)
        self.n_correction_rounds = kw.get("rounds", 0)
        self.result_row_count = kw.get("rows", 5)
        self.executed_ok = kw.get("ok", True)


def test_features_detect_sql_structure():
    t = FakeTrace(
        sql="SELECT COUNT(*) FROM a JOIN b ON a.id=b.id WHERE x IN (SELECT y FROM c)"
    )
    f = extract_features(t, "how many?", "hint", n_schema_tables=5)
    assert f.has_aggregation == 1.0
    assert f.has_subquery == 1.0
    assert f.has_nested_select == 1.0
    assert f.n_joins == 1.0
    assert f.n_tables_referenced == 3.0
    assert f.evidence_provided == 1.0


def test_empty_result_flagged():
    f = extract_features(FakeTrace(rows=0, ok=True), "q", "", 3)
    assert f.result_empty == 1.0


def test_schema_coverage_is_a_ratio():
    f = extract_features(FakeTrace(sql="SELECT a FROM t"), "q", "", n_schema_tables=4)
    assert f.schema_coverage == pytest.approx(0.25)


def test_v1_confidence_is_agreement():
    assert agreement_confidence(QueryFeatures(agreement_rate=0.6)) == pytest.approx(0.6)


def test_v1_confidence_zero_when_execution_failed():
    f = QueryFeatures(agreement_rate=0.9, execution_errored=1.0, n_correction_rounds=0)
    assert agreement_confidence(f) == 0.0


def _synthetic(n=200, seed=0):
    """Agreement correlates with correctness, so a fitted model should learn it."""
    rng = np.random.default_rng(seed)
    feats, correct = [], []
    for _ in range(n):
        agree = rng.uniform(0, 1)
        ok = int(rng.uniform() < agree)
        feats.append(QueryFeatures(agreement_rate=agree, n_tables_referenced=rng.integers(1, 4)))
        correct.append(ok)
    return feats, correct


def test_model_learns_and_predicts_in_range():
    feats, correct = _synthetic()
    m = ConfidenceModel().fit(feats, correct, question_ids=list(range(len(feats))))
    p = m.predict(feats)
    assert len(p) == len(feats)
    assert p.min() >= 0.0 and p.max() <= 1.0


def test_model_beats_a_constant_predictor():
    feats, correct = _synthetic()
    m = ConfidenceModel().fit(feats, correct, question_ids=list(range(len(feats))))
    assert brier_score(m.predict(feats), correct) < brier_score(
        [np.mean(correct)] * len(correct), correct
    )


def test_leakage_guard_rejects_evaluation_ids():
    """The guard CLAUDE.md calls project-invalidating if missed."""
    feats, correct = _synthetic(50)
    with pytest.raises(ValueError, match="LEAKAGE"):
        ConfidenceModel().fit(
            feats, correct, question_ids=list(range(50)), forbidden_ids={7, 99}
        )


def test_leakage_guard_allows_disjoint_ids():
    feats, correct = _synthetic(50)
    ConfidenceModel().fit(
        feats, correct, question_ids=list(range(50)), forbidden_ids={500, 501}
    )


def test_fit_requires_both_classes():
    feats, _ = _synthetic(20)
    with pytest.raises(ValueError, match="both correct and incorrect"):
        ConfidenceModel().fit(feats, [1] * 20, question_ids=list(range(20)))


def test_roundtrip_save_load(tmp_path):
    feats, correct = _synthetic(100)
    m = ConfidenceModel().fit(feats, correct, question_ids=list(range(100)))
    p1 = m.predict(feats)
    m.save(tmp_path / "m.pkl")
    p2 = ConfidenceModel.load(tmp_path / "m.pkl").predict(feats)
    assert np.allclose(p1, p2)


def test_brier_bounds():
    assert brier_score([1.0, 0.0], [1, 0]) == 0.0
    assert brier_score([0.0, 1.0], [1, 0]) == 1.0


def test_perfect_calibration_has_zero_ece():
    probs = [0.0] * 50 + [1.0] * 50
    correct = [0] * 50 + [1] * 50
    assert expected_calibration_error(probs, correct) == pytest.approx(0.0)


def test_reliability_curve_bins_sum_to_n():
    probs = np.linspace(0, 1, 100)
    correct = (probs > 0.5).astype(int)
    assert sum(b["count"] for b in reliability_curve(probs, correct)) == 100
