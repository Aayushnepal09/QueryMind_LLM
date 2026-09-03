from pathlib import Path

import pytest

from sqlsentinel.eval.harness import BirdHarness, EvalResult

BIRD = Path("data/bird/dev_20240627")
pytestmark = pytest.mark.skipif(not (BIRD / "dev.json").exists(), reason="BIRD dev set not present")


@pytest.fixture(scope="module")
def harness():
    return BirdHarness(BIRD)


def test_gold_sql_scores_100_percent(harness):
    """The strongest harness test: feeding BIRD's own gold SQL back must score 100%.

    If this drops below 100 the wrapper is misaligning predictions, gold, or
    difficulty labels -- which would silently corrupt every number downstream.
    """
    qids = [r["question_id"] for r in harness.records[:40]]
    gold = {q: harness.gold_lines[q].split("\t")[0] for q in qids}
    result = harness.evaluate(gold, qids)
    assert result.accuracy == 100.0


def test_stub_scores_near_floor(harness):
    qids = [r["question_id"] for r in harness.records[:40]]
    result = harness.evaluate({q: "SELECT 1" for q in qids}, qids)
    assert result.accuracy < 15.0


def test_unparseable_sql_scores_zero_not_crash(harness):
    qids = [r["question_id"] for r in harness.records[:60]]
    result = harness.evaluate({q: "this is not sql at all" for q in qids}, qids)
    assert result.accuracy == 0.0


def test_missing_prediction_raises(harness):
    qids = [r["question_id"] for r in harness.records[:5]]
    with pytest.raises(ValueError, match="missing"):
        harness.evaluate({qids[0]: "SELECT 1"}, qids)


def test_ci95_shrinks_with_n():
    assert EvalResult(50, 50.0, 0, 0, 0).ci95 > EvalResult(500, 50.0, 0, 0, 0).ci95
    assert abs(EvalResult(500, 50.0, 0, 0, 0).ci95 - 4.4) < 0.5
