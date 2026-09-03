import pytest

from sqlsentinel.eval.stats import mcnemar, wilson_interval


def make(a_bits: str, b_bits: str) -> tuple[dict, dict]:
    return (
        {i: int(c) for i, c in enumerate(a_bits)},
        {i: int(c) for i, c in enumerate(b_bits)},
    )


def test_identical_runs_are_not_significant():
    a, b = make("11001", "11001")
    r = mcnemar(a, b)
    assert r.discordant == 0
    assert r.p_value == 1.0
    assert not r.significant
    assert "identical" in r.verdict()


def test_counts_partition_the_questions():
    a, b = make("1100", "1010")
    r = mcnemar(a, b)
    assert r.a_only + r.b_only + r.both + r.neither == r.n == 4
    assert r.both == 1 and r.neither == 1
    assert r.a_only == 1 and r.b_only == 1


def test_delta_sign_follows_b():
    a, b = make("1000", "1110")  # B wins two extra
    r = mcnemar(a, b)
    assert r.delta_points == pytest.approx(50.0)
    a, b = make("1110", "1000")
    assert mcnemar(a, b).delta_points == pytest.approx(-50.0)


def test_large_consistent_difference_is_significant():
    a = dict.fromkeys(range(20), 1)
    b = dict.fromkeys(range(20), 0)
    r = mcnemar(a, b)
    assert r.significant and r.p_value < 0.001


def test_small_difference_is_not_significant():
    a, b = make("10" + "1" * 20, "01" + "1" * 20)
    assert not mcnemar(a, b).significant


def test_evidence_ablation_shape_is_significant():
    """The observed dev_50 result: 14 helped, 4 hurt, 32 concordant."""
    a = dict.fromkeys(range(14), 1)          # evidence correct only
    b = dict.fromkeys(range(14), 0)
    for i in range(14, 18):                # no-evidence correct only
        a[i], b[i] = 0, 1
    for i in range(18, 50):
        a[i], b[i] = 1, 1
    r = mcnemar(a, b)
    assert r.a_only == 14 and r.b_only == 4
    assert r.p_value == pytest.approx(0.0309, abs=1e-3)
    assert r.significant


def test_paired_test_resolves_what_independent_intervals_cannot():
    """The reason this module exists, using the real dev_50 evidence ablation.

    31/50 vs 21/50: the Wilson intervals overlap, so comparing them
    independently would call the effect inconclusive. The paired test, looking
    only at the 18 questions where the runs disagree, finds it significant.
    """
    a, b = {}, {}
    for i in range(14):            # evidence correct only
        a[i], b[i] = 1, 0
    for i in range(14, 18):        # no-evidence correct only
        a[i], b[i] = 0, 1
    for i in range(18, 35):        # both correct
        a[i], b[i] = 1, 1
    for i in range(35, 50):        # both wrong
        a[i], b[i] = 0, 0

    assert sum(a.values()) == 31 and sum(b.values()) == 21

    lo_a, _ = wilson_interval(31, 50)
    _, hi_b = wilson_interval(21, 50)
    assert lo_a < hi_b, "unpaired intervals overlap — the effect looks inconclusive"
    assert mcnemar(a, b).significant, "paired test resolves it"


def test_disjoint_question_sets_raise():
    with pytest.raises(ValueError, match="no overlapping"):
        mcnemar({1: 1}, {2: 1})


def test_only_shared_questions_are_compared():
    a = {1: 1, 2: 1, 3: 0}
    b = {2: 0, 3: 1, 9: 1}
    assert mcnemar(a, b).n == 2


def test_wilson_stays_in_range():
    assert wilson_interval(0, 10)[0] >= 0.0
    assert wilson_interval(10, 10)[1] <= 100.0


def test_wilson_narrows_with_n():
    lo1, hi1 = wilson_interval(25, 50)
    lo2, hi2 = wilson_interval(250, 500)
    assert (hi2 - lo2) < (hi1 - lo1)


def test_wilson_empty():
    assert wilson_interval(0, 0) == (0.0, 0.0)
