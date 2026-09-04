import json
from pathlib import Path

import pytest

from sqlsentinel.eval.subsets import DEV_N, EVAL_N, build_splits

BIRD = Path("data/bird/dev_20240627")
pytestmark = pytest.mark.skipif(not (BIRD / "dev.json").exists(), reason="BIRD dev set not present")


@pytest.fixture(scope="module")
def splits():
    return build_splits(BIRD / "dev.json")


def test_sizes(splits):
    assert len(splits["eval_500"]) == EVAL_N
    assert len(splits["dev_50"]) == DEV_N
    assert len(splits["calib"]) == 1534 - EVAL_N


def test_no_leakage_between_calib_and_eval(splits):
    """The invariant CLAUDE.md calls out as project-invalidating if broken."""
    assert not set(splits["calib"]) & set(splits["eval_500"])


def test_dev50_is_subset_of_eval500(splits):
    assert set(splits["dev_50"]) <= set(splits["eval_500"])


def test_deterministic(splits):
    assert build_splits(BIRD / "dev.json") == splits


def test_committed_splits_match_generated(splits):
    """results/splits.json is the record of truth; regenerating must not drift."""
    committed = json.loads(Path("results/splits.json").read_text(encoding="utf-8"))
    for name in ("eval_500", "dev_50", "calib"):
        assert committed[name] == splits[name], f"{name} drifted from committed split"


def test_stratification_matches_population(splits):
    recs = {
        r["question_id"]: r for r in json.loads((BIRD / "dev.json").read_text(encoding="utf-8"))
    }
    pop = {
        d: sum(r["difficulty"] == d for r in recs.values()) / len(recs)
        for d in ("simple", "moderate", "challenging")
    }
    ids = splits["eval_500"]
    for d, want in pop.items():
        got = sum(recs[i]["difficulty"] == d for i in ids) / len(ids)
        assert abs(got - want) < 0.02, f"{d}: {got:.3f} vs population {want:.3f}"


def test_all_databases_represented(splits):
    recs = {
        r["question_id"]: r for r in json.loads((BIRD / "dev.json").read_text(encoding="utf-8"))
    }
    assert len({recs[i]["db_id"] for i in splits["eval_500"]}) == 11
