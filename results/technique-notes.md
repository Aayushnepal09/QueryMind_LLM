# Technique notes

Diagnostics that explain *why* a technique moved the number, recorded alongside
the number itself. Measured on `dev_50` unless stated.

## Summary: the Phase 2 ablations

All comparisons are **paired** (McNemar exact, same 50 questions) rather than
comparisons of independent confidence intervals. The runs share their questions,
and discarding that pairing costs enough power to hide real effects — on
`dev_50` the unpaired interval is ±13 points, which would call almost anything
inconclusive.

| Run | EX | delta | helped | hurt | p | Verdict |
|---|---:|---:|---:|---:|---:|---|
| baseline | 62.0% | — | — | — | — | reference |
| combined (few-shot + self-correct) | 64.0% | +2.0 | 6 | 5 | 1.000 | not distinguishable |
| schema pruning | 62.0% | 0.0 | 0 | 0 | 1.000 | identical predictions |
| self-correction | 62.0% | 0.0 | 0 | 0 | 1.000 | identical *scores*, different failures |
| few-shot (k=3) | 58.0% | −4.0 | 6 | 8 | 0.791 | not distinguishable |
| **− evidence field** | **42.0%** | **−20.0** | **4** | **14** | **0.031** | **significant** |

**The headline: none of the techniques I added produced a measurable
improvement, and the single largest effect in the entire sweep is a field BIRD
ships with the dataset.** Removing the `evidence` hint costs 20 points — more
than any technique gained. That is the honest result, and it is more useful than
a tuned number would have been: on this benchmark, with this model, the binding
constraint is *domain knowledge supplied with the question*, not prompting
strategy.

The few-shot row is worth reading carefully. Its −4.0 points come from 6
questions helped and 8 hurt (p=0.79) — near-symmetric churn, not a systematic
harm. It moves predictions around without moving accuracy.

## Few-shot retrieval (k=3 exemplars)

**Result: 58.0% vs 62.0% baseline — 4 points worse.**

At n=50 the 95% CI is ±13 points, so this is **not a significant decline**. It
is reported as "no measurable benefit", not as "few-shot hurts".

Diagnostics run to check for an implementation fault rather than a real null:

| Check | Value | Reading |
|---|---|---|
| Exemplars drawn from the same database as the question | 140/150 (93%) | Retrieval is not mostly pulling in foreign schemas |
| Exemplars from a *different* database | 10/150 (7%) | Small, but these do show table names absent from the target schema |
| Mean TF-IDF cosine similarity | 0.338 | Weak. Retrieved questions are only loosely related |
| Prompt size, baseline | ~1,360 tokens | |
| Prompt size, with 3 exemplars | ~3,400 tokens | **2.5x** |

Two plausible mechanisms, not separated by this data:

1. **Weak retrieval.** A mean similarity of 0.34 means the exemplars often are
   not analogous. BIRD's calibration pool has ~94 questions per database on
   average, which is thin for nearest-neighbour retrieval.
2. **Context dilution.** Tripling the prompt gives a 7B model more to attend to
   without adding proportionally more signal.

Worth testing before drawing a conclusion: restricting retrieval to the same
database (`ExemplarStore(same_db_only=True)`), and k=1 to isolate prompt length
from exemplar quality. Both are cheap; neither has been run.

**Not done:** tuning the retriever until the number improves. With a ±13 margin
that would be fitting to noise, and `dev_50` is the development loop, not
evidence.

## Schema pruning

**Result: 62.0% — identical to the 62.0% baseline, to the question.**

Predicted before the run from a structural diagnostic, and confirmed exactly:

| Measurement | Value |
|---|---|
| Tables retained across `dev_50` | 351/356 (99%) |
| Mean prompt reduction | 1.3% |
| BIRD dev database sizes | 3–13 tables |

The pruner scores tables on lexical overlap with the question, then re-adds
anything reachable by a foreign key from a kept table. On BIRD's dev databases —
small and densely FK-connected — that closure pulls back essentially everything
the lexical scoring dropped. The technique is a no-op here, and the identical
accuracy is the expected consequence, not a coincidence.

**Why the FK closure stays.** Removing it would prune more aggressively and make
the technique *look* like it does something. It would also drop bridge tables
that joins depend on, and a missing bridge table does not produce an error — it
produces confidently wrong SQL. A silent accuracy loss traded for a visible
token saving is a bad trade on a benchmark scored by exact result sets.

**Answering CLAUDE.md §13** ("Is schema linking worth it on BIRD's larger
databases, or does full-schema context win?"): on BIRD *dev*, full-schema context
wins by default because there is nothing meaningful to prune. Mean schema is
~1,230 tokens and the largest is 3,128, so there is no token pressure to relieve
either. Schema linking is a technique for wide schemas; BIRD dev does not have
them. It would need BIRD *train* (or a genuinely wide warehouse schema) to be
tested properly.

## Self-correction (max 2 rounds)

**Result: 62.0% — identical to baseline. But it did not do nothing.**

| Measurement | Baseline | Self-correction |
|---|---:|---:|
| Queries that failed to execute (pre-correction) | 4 | 4 |
| Correction attempted | 0 | 4 |
| Queries that ultimately executed successfully | 46/50 | **48/50** |
| Execution accuracy | 62.0% | **62.0%** |

The loop worked exactly as designed: it caught all 4 execution errors, and fixed
2 of them into queries that run. Accuracy did not move, which means **both
repaired queries now execute and return the wrong answer.**

### This is a worse outcome than it looks

Self-correction converted two *loud* failures into two *silent* ones. An
execution error announces itself — the caller gets an exception and knows not to
trust the result. A syntactically valid query returning wrong rows looks exactly
like success.

It also degrades the confidence signal. `execution_errored` is one of the
strongest negative features in the scorer (CLAUDE.md §6 lists it as such), and
self-correction is precisely a mechanism for destroying that feature's
information while leaving the underlying error in place.

### What follows from it

1. **Self-correction should not be enabled without the routing layer.** On its
   own it trades detectability for a cosmetic improvement in execution rate.
2. `n_correction_rounds` is retained as a confidence feature specifically
   because a query that needed repairing is less trustworthy — the trace keeps
   `execution_errored_pre_correction` separately from the post-correction state
   so the signal survives the repair.
3. The two queries that failed both rounds (`1262`, `1350`) are genuinely hard,
   not transient: two attempts with the error message in context did not help.

**Caveat:** n=4 corrected queries. This is a mechanism observation, not a
statistically supported claim about correction rates. The direction of the
effect is what matters, and it is visible in the trace regardless of sample size.

## Self-consistency (k=3) — the technique that worked

> ⚠️ **SUPERSEDED — the numbers in this section came from a contaminated run.**
> `k3-calib200` was evaluated on the same split the few-shot exemplars are drawn
> from, so every question retrieved itself and its gold SQL. Worth 23.5 points.
> See `results/quarantine/README.md`. The run has been repeated with the leakage
> guard in place; this section is retained to show what the contaminated numbers
> looked like, and is replaced below once the clean run lands.
>
> The *structure* of the finding — that agreement rate is monotonic and that
> routing catches errors at a useful rate — is expected to survive, because the
> agreement signal does not depend on the leak. The *magnitudes* are not to be
> cited.

**`k3-calib200`: EX 78.0% ± 5.7 (n=200)** — **contaminated**, with few-shot and
self-correction enabled. Not directly comparable to the 62% `dev_50` baseline — different
questions — but the confidence behaviour is the point, and it is measured on 200
questions rather than 50.

### Agreement is a real signal, not a degenerate one

The first thing to check with self-consistency is whether the samples actually
diverge. With a cache keyed on `sample_index` they do:

| Agreement rate | Questions | Observed accuracy |
|---:|---:|---:|
| 1.00 (unanimous) | 144 | 94% |
| 0.67 | 37 | 51% |
| 0.33 | 13 | 15% |
| 0.00 | 6 | 0% |

Monotonic across every bucket. Brier score **0.101**, down from 0.300 at k=1
(where agreement is constant at 1.0 and the signal does not exist). ECE 0.080.

### The money metric

| Threshold | Routed to review | Errors caught | Auto-executed accuracy | Lift over random |
|---:|---:|---:|---:|---:|
| 0.35 | 10% | 40% | 85.6% | 4.2× |
| **0.70** | **28%** | **81%** | **94.4%** | **2.9×** |

**At threshold 0.70: routing 28% of queries to human review catches 81% of all
incorrect queries, and everything auto-executed is 94.4% correct — up from 78%
unrouted.**

### Two honest caveats

**v1 is consistently overconfident.** The reliability curve is monotonic but
sits below the diagonal everywhere: predicted 0.67 → observed 0.51, predicted
0.33 → observed 0.15. Useful for *ranking* queries by risk, but the number
should not be read as a probability. This is precisely what the v2 calibrated
model exists to correct.

**k=3 gives only four possible confidence values** (0, 0.33, 0.67, 1.0), which
is why the routing curve is a step function with wide flat regions. Any
threshold in 0.35–0.65 behaves identically. Finer control needs larger k — the
cost/granularity trade-off CLAUDE.md §13 asks about, now with a concrete answer:
k=3 is enough to separate risk into four useful bands, and not enough to tune a
threshold precisely.

**Split note:** these numbers are from `calib`. That is legitimate for v1, which
has no fitted parameters — it is a measurement of the raw agreement signal, not
a trained model. The v2 calibrated scorer is fitted here and reported only on
`eval_500`, which `calib` is disjoint from.

---

# Final numbers (clean, post-leakage-fix)

## The headline

| Configuration | Split | n | EX | 95% CI |
|---|---|---:|---:|---:|
| Stub (`SELECT 1`) — chance floor | eval_500 | 500 | 3.0% | ±1.5 |
| **Baseline** (ported QueryMind prompt, single-shot) | eval_500 | 500 | **45.6%** | ±4.3 |
| **Final** (k=3 self-consistency + few-shot + self-correction) | eval_500 subset | 200 | **55.0%** | ±6.8 |

**Paired comparison on the 200 shared questions: +9.0 points, 30 helped, 12
hurt, McNemar exact p = 0.0079 — significant.**

This is the project's real improvement, and it comes almost entirely from
self-consistency. The individual techniques measured alone on `dev_50` were all
null; sampling k=3 and voting on executed results is what moved the number.

## The small-sample lesson, measured

The 50-question development loop and the 500-question evaluation split were run
with the identical configuration. `dev_50` is a stratified subset of `eval_500`,
so this is the same agent on the same benchmark:

| Slice | n | EX | 95% CI |
|---|---:|---:|---:|
| `dev_50` | 50 | 62.0% | [48.2, 74.1] |
| the other 450 | 450 | 43.1% | [38.6, 47.7] |
| **full `eval_500`** | **500** | **45.0%** | **[40.7, 49.4]** |

**The development loop overestimated accuracy by 17 points, and its 95%
confidence interval did not contain the true value.** Stratification by
difficulty × database did not save it: at n=50 there are only ~4 questions per
database, and that is not enough.

Had this project reported `dev_50` numbers — which is the natural thing to do,
because it is the loop you iterate against — it would have claimed 62% for a
system that scores 45.6%. That is the same class of error as QueryMind's
unverifiable "92%", arrived at honestly.

It is also why every technique delta in this repository is reported with its
confidence interval and a paired significance test, and why the reported split
is fixed and committed before any technique is tried.

## Failure taxonomy (baseline, n=500, 273 failures)

| Category | Count | Share |
|---|---:|---:|
| `execution_error` | 82 | 30% |
| `join_path_error` | 73 | 27% |
| `other` | 60 | 22% |
| `wrong_columns` | 27 | 10% |
| `aggregation_error` | 16 | 6% |
| `value_matching` | 9 | 3% |
| `ordering_or_limit` | 5 | 2% |
| `empty_result` | 1 | 0% |

Nearly a third of baseline failures never execute at all — which looks like a
strong argument for self-correction, until you read the self-correction result
above: it repairs executability without repairing correctness, converting loud
failures into silent ones. The two findings only make sense together, and
together they are the argument for routing rather than for repair.
