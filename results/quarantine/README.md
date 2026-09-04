# Quarantined results — do not cite

These artifacts were produced before a leakage bug was found and fixed. They are
kept rather than deleted because the size of the contamination is itself a
finding, and because deleting inconvenient results is how evaluations become
untrustworthy.

## The bug

`ExemplarStore.retrieve()` had no guard against returning the question being
answered. Few-shot exemplars are drawn from the `calib` split — so when the
agent was *evaluated on* `calib`, every question retrieved **itself** at
cosine similarity 1.000, and its gold SQL was pasted into the prompt as a
worked example.

The agent was being shown the answer.

## How it surfaced

Two runs with an identical configuration (`--k 3 --few-shot 3
--max-corrections 2`) on two stratified splits of the same population:

| Run | Split | EX |
|---|---|---:|
| `k3-calib200` | `calib` (in the exemplar pool) | **78.0% ± 5.7** |
| `k3-eval200` | `eval_500` (not in the pool) | **54.5% ± 6.8** |

A 23.5-point gap between two similarly-stratified samples of the same benchmark
is not a difficulty difference. That discrepancy is what prompted the check.

## Scope of the contamination

Only runs **evaluated on `calib` with few-shot enabled** are affected — that is
`k3-calib200` alone.

Unaffected, and safe to cite:
- every `dev_50` run — `dev_50 ⊂ eval_500`, which is disjoint from `calib`
- `k3-eval200` and both `eval_500` runs — same reason
- every run with few-shot disabled — no retrieval, no pool

## The fix

`retrieve(..., exclude_question_id=...)`, passed by the agent on every call, with
regression tests in `tests/test_retrieval.py` covering both the leak and the
guard. `k3-calib200` was re-run clean; its replacement lives in
`results/traces/`.

## Why this happened

The project's stated leakage guard (`ConfidenceModel.fit(forbidden_ids=...)`)
defends the *confidence model* against training on evaluation questions. It does
not defend the *generator* against retrieving a question's own answer. Those are
two different leakage paths through the same split, and only one had a guard.
