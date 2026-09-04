# Stub-predictor floor

BIRD version `dev_20240627`. Predictor always emits `SELECT 1`.

| Split | n | EX | 95% CI |
|---|---|---|---|
| dev_50 | 50 | 4.0% | ±6.2 |
| eval_500 | 500 | **3.0%** | ±1.5 |

**The floor is not zero.** spec §8 predicted "~0%". It is ~3%.

BIRD's execution-accuracy metric compares result *sets*: a prediction is correct
when `set(predicted_rows) == set(gold_rows)`. `SELECT 1` returns `{(1,)}`, and a
non-trivial number of gold queries also return a single scalar `1` — a `COUNT(*)`
that happens to be 1, a `MAX` of a boolean-ish column, and so on. Those match by
coincidence, not by reasoning.

Consequence for reporting: **every accuracy number in this project should be read
against a ~3% chance floor, not 0%.** A model scoring 50% is 47 points above
chance, not 50. This matters most for the `challenging` bucket, where the floor is
lower (2.1%) but sample sizes are smallest.

Scoring cost: 42 s wall-clock for all 500 questions at `--num-cpus 4`. Scoring is
not a bottleneck; generation is.
