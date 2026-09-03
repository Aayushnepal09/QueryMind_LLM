"""Deterministic, stratified BIRD dev-set splits.

Design rationale (see PROGRESS.md "Locked decisions"):

BIRD dev has 1,534 questions. We do not report on all of them -- a full run at
k=5 is an overnight job on the target hardware. Instead we fix three splits,
seeded and committed, so every run in the project is comparable:

    eval_500  -- the REPORTED evaluation set. Stratified by difficulty x db_id.
                 95% CI on an accuracy near 50% is +/-4.4%. Nothing is ever
                 fitted on these questions.
    dev_50    -- a subset of eval_500 for the fast development loop.
    calib     -- the remaining 1,034 questions, used ONLY to fit the confidence
                 classifier (CLAUDE.md section 6).

The calib/eval_500 split is disjoint by construction. This is what enforces
CLAUDE.md's "never train and evaluate the confidence model on the same
questions" -- the one mistake that would invalidate the project.
"""

from __future__ import annotations

import json
import random
from collections import defaultdict
from pathlib import Path

SEED = 20240627  # matches the pinned BIRD release date; arbitrary but fixed
EVAL_N = 500
DEV_N = 50


def _stratified_sample(
    records: list[dict], n: int, rng: random.Random
) -> list[int]:
    """Sample n question_ids, preserving the difficulty x db_id distribution.

    Uses largest-remainder allocation so the strata proportions are matched as
    closely as integer counts allow, rather than drifting with rounding.
    """
    strata: dict[tuple[str, str], list[int]] = defaultdict(list)
    for r in records:
        strata[(r["difficulty"], r["db_id"])].append(r["question_id"])

    total = len(records)
    exact = {k: len(v) * n / total for k, v in strata.items()}
    alloc = {k: int(v) for k, v in exact.items()}

    # distribute the remainder to the strata with the largest fractional parts
    short = n - sum(alloc.values())
    by_frac = sorted(strata, key=lambda k: exact[k] - alloc[k], reverse=True)
    for k in by_frac[:short]:
        alloc[k] += 1

    picked: list[int] = []
    for k in sorted(strata):  # sorted for determinism
        ids = sorted(strata[k])
        rng.shuffle(ids)
        picked.extend(ids[: alloc[k]])
    return sorted(picked)


def build_splits(dev_json: Path) -> dict[str, list[int]]:
    records = json.loads(dev_json.read_text(encoding="utf-8"))

    rng = random.Random(SEED)
    eval_ids = _stratified_sample(records, EVAL_N, rng)

    eval_set = set(eval_ids)
    eval_records = [r for r in records if r["question_id"] in eval_set]

    rng_dev = random.Random(SEED + 1)
    dev_ids = _stratified_sample(eval_records, DEV_N, rng_dev)

    calib_ids = sorted(r["question_id"] for r in records if r["question_id"] not in eval_set)

    assert not (set(calib_ids) & eval_set), "calib/eval leakage"
    assert set(dev_ids) <= eval_set, "dev_50 must be a subset of eval_500"
    assert len(eval_ids) == EVAL_N and len(dev_ids) == DEV_N

    return {"eval_500": eval_ids, "dev_50": dev_ids, "calib": calib_ids}


def stratified_subsample(records: list[dict], n: int, seed: int = SEED) -> list[int]:
    """Take n question_ids from `records`, preserving difficulty x db_id shape.

    Used for --subset. Truncating a split with [:n] would both bias the sample
    toward whichever databases sort first and risk producing a slice with an
    empty difficulty bucket, which crashes the official BIRD scorer.
    """
    if n >= len(records):
        return sorted(r["question_id"] for r in records)
    return _stratified_sample(records, n, random.Random(seed))
