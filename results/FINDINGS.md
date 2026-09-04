# Findings

Secondary analyses over this project's own runs. Every number here is computed
by `scripts/research.py` from the trace files in `results/traces/`, and can be
regenerated. Nothing is quoted from published work; where a result agrees with
the literature that is noted as agreement, not as a source.

All results are from `qwen2.5-coder:7b` running locally against BIRD dev
`dev_20240627`, evaluated on the fixed `eval_500` split.

---

## 1. Most wrong answers do not look wrong

**59% of the baseline's incorrect queries execute cleanly and return
non-empty results.** They are indistinguishable from correct answers without
the gold query.

| | wrong | **silent** (runs, returns rows, still wrong) | loud (execution error) | empty result |
|---|---:|---:|---:|---:|
| Baseline | 273/500 | **162 (59%)** | 82 | 29 |
| Final (few-shot + self-correction) | 248/500 | **176 (71%)** | 44 | 28 |

Read as a share of *all* queries: **32% of everything the baseline produces is
wrong data that looks right**, rising to 35% for the improved system.

### The improvement made this worse

Self-correction cut loud execution errors almost in half (82 → 44) while the
silent share of errors *rose* from 59% to 71%. The technique converts failures
that announce themselves into failures that do not.

That is a genuine safety regression hiding inside a 5-point accuracy
improvement, and it is invisible in any single-number report. It is also the
clearest argument for this project's central claim: **an accuracy number tells
you how often a text-to-SQL system is wrong; it does not tell you whether you
would notice.** A system whose errors are mostly silent needs a confidence layer
more than it needs another accuracy point.

*Method:* an incorrect prediction is "silent" when `executed_ok` is true and
`result_row_count > 0`. Correctness is the official BIRD result-set comparison.

---

## 2. Per-database accuracy varies by 65 points, and schema size does not explain it

BIRD results are conventionally reported as one number. That number conceals an
enormous spread.

| Database | n | EX | 95% CI | tables | columns | FKs |
|---|---:|---:|---|---:|---:|---:|
| `thrombosis_prediction` | 53 | **22.6%** | [13.5, 35.5] | 3 | 64 | 2 |
| `california_schools` | 30 | 23.3% | [11.8, 40.9] | 3 | 89 | 2 |
| `european_football_2` | 42 | 31.0% | [19.1, 46.0] | 7 | 199 | 31 |
| `financial` | 34 | 35.3% | [21.5, 52.1] | 8 | 55 | 8 |
| `toxicology` | 47 | 40.4% | [27.6, 54.7] | 4 | 11 | 5 |
| `formula_1` | 57 | 42.1% | [30.2, 55.0] | 13 | 94 | 19 |
| `debit_card_specializing` | 20 | 45.0% | [25.8, 65.8] | 5 | 21 | 2 |
| `card_games` | 62 | 48.4% | [36.4, 60.6] | 6 | 115 | 4 |
| `codebase_community` | 61 | 52.5% | [40.2, 64.5] | 8 | 71 | 13 |
| `student_club` | 52 | 61.5% | [48.0, 73.5] | 8 | 48 | 8 |
| `superhero` | 42 | **88.1%** | [75.0, 94.8] | 10 | 31 | 11 |

The best and worst databases differ by **65 points** with non-overlapping
confidence intervals. The headline 45.6% describes no database in particular.

### Schema size does not predict difficulty

| Predictor | Pearson r | p | |
|---|---:|---:|---|
| number of tables | **+0.546** | 0.083 | not significant, and *positive* |
| number of columns | −0.368 | 0.265 | not significant |
| number of foreign keys | +0.050 | 0.884 | not significant |

None of the three reaches significance at n=11 databases, and the table count
trends the *wrong way* — the agent does slightly better on schemas with more
tables, not worse.

The concrete counterexamples are stark. `superhero` has 10 tables and 11 foreign
keys and scores 88.1%. `thrombosis_prediction` has 3 tables and scores 22.6%.
`european_football_2` has 199 columns — by far the widest — and sits mid-table at
31.0%.

**This is why schema pruning did nothing here** (see `technique-notes.md`).
Pruning attacks schema *size*, and size is not what makes these databases hard.
What separates `superhero` from `thrombosis_prediction` is domain
transparency: superhero columns are named `superhero_name`, `attribute_value`,
`power_name`, while thrombosis columns are clinical abbreviations whose meaning
is not recoverable from the identifier. The failure is semantic, not structural.

*Caveat:* n=11 databases is a small sample for correlation, and these are
confidence intervals on per-database subsets ranging from 20 to 62 questions.
The 65-point spread is robust; the correlation analysis is suggestive at best,
and is reported as a null rather than a finding.

---

## 3. Domain knowledge helps most where the question is hardest

Ablating BIRD's `evidence` field costs 20 points overall (p=0.031). Broken down:

| Difficulty | n | helped | hurt | net |
|---|---:|---:|---:|---:|
| simple | 31 | 7 | 3 | +12.9 points |
| **moderate** | 15 | 6 | 1 | **+33.3 points** |
| challenging | 4 | 1 | 0 | +25.0 points |

The evidence field is worth roughly **2.5× more on moderate questions than on
simple ones**. This is consistent with §2: what limits the agent is not parsing
the question or navigating the schema, but knowing what the domain terms mean.
Simple questions can often be answered from column names alone; harder ones
cannot.

*Caveat:* only 4 challenging questions in this slice. The simple/moderate
contrast is the supportable part.

---

## 4. A +5 point improvement moved 97 questions

Comparing baseline to final on the same 500 questions:

| | count |
|---|---:|
| Questions fixed | **61** |
| Questions broken | **36** |
| Net | **+25** (+5.0 points, p = 0.014) |
| **Churn** | **3.9× the net effect** |

Nearly four questions change answer for every one net gain. By difficulty:

| | simple | moderate | challenging |
|---|---:|---:|---:|
| fixed | 39 | 18 | 4 |
| broken | 23 | 7 | **6** |
| net | +16 | +11 | **−2** |

**The techniques made challenging questions net worse.** All of the gain comes
from simple and moderate questions; on the hardest tier the agent broke more
than it fixed.

This is invisible in the headline number, and it matters for deployment: a
system that improves on easy cases while regressing on hard ones has become
*more* dangerous in exactly the situations where a human would most want to
trust it. It also compounds §1 — the extra silent failures are concentrated
where the stakes are highest.

---

## 5. Question length predicts failure, strongly

| Question length | n | EX |
|---|---:|---:|
| 1–10 words | 133 | **60.9%** |
| 11–20 words | 306 | 41.5% |
| 21–30 words | 54 | 33.3% |
| 31+ words | 7 | **14.3%** |

Pearson **r = −0.224, p < 0.0001** — one of the most statistically solid results
in this project, on n=500.

Accuracy falls by roughly **47 points** from the shortest to the longest bucket.
Longer questions carry more clauses, more implicit joins, and more conditions
that must all be satisfied simultaneously for the result set to match exactly.

This is directly actionable: question length is known *before* any generation
happens, costs nothing to compute, and is already one of the 14 features in the
confidence model. It is the cheapest available prior on whether an answer will
be trustworthy.

---

## 6. The model's uncertainty does not track the benchmark's difficulty labels

| Difficulty | n | mean agreement (k=3) |
|---|---:|---:|
| simple | 120 | 0.778 |
| moderate | 60 | 0.683 |
| **challenging** | 20 | **0.717** |

Agreement drops from simple to moderate, then *rises* again on challenging
questions. It is not monotonic.

This is a useful negative result. Self-consistency agreement is a strong
predictor of correctness (§7 below, and the reliability curve in
`RESULTS.md`) — but it is **not** measuring the same thing BIRD's human
difficulty annotation measures. The model can be confidently wrong on questions
annotators called hard, and uncertain on ones they called easy.

The practical implication is that the two signals are complementary rather than
redundant: a router using both a difficulty prior and agreement has more
information than one using either alone.

*Caveat:* n=20 challenging questions. The simple→moderate drop is the reliable
part; the challenging bucket is too small to be confident about.

---

## 7. Value-matching failures are not spelling problems

A recurring hypothesis about BIRD's "dirty data" difficulty is that queries fail
because they filter on `'CA'` when the column stores `'California'`, or match
case incorrectly. That is testable.

Of the baseline's incorrect queries, 24 executed successfully, returned zero
rows, and filtered on a string literal. Rewriting each `= 'value'` as
`LIKE 'value'` — which relaxes case sensitivity in SQLite:

| | count |
|---|---:|
| Zero-row failures filtering on a string | 24 |
| Would return rows with a relaxed match | **2** |
| Share recoverable | **8%** |

**92% of these failures are not case or matching problems.** The query is
selecting genuinely absent data — the wrong column, the wrong join path, or a
condition that legitimately matches nothing. Fuzzy value matching, a commonly
proposed fix, would address 2 of 500 questions here.

---

## 8. Cost per correct answer

CLAUDE.md §13 asks for provider choice on cost-per-correct-answer rather than
raw accuracy. Both providers are free in dollars, so the real currency is time.

| Configuration | EX | seconds per correct answer | mean prompt tokens | nominal USD |
|---|---:|---:|---:|---:|
| Baseline (k=1) | 45.4% | **20.2 s** | 2,313 | $0 |
| Final (few-shot + correction) | 50.4% | 39.1 s | 2,318 | $0 |
| k=3 self-consistency | 55.0% | **55.5 s** | 6,955 | $0 |

Accuracy rises from 45.4% → 55.0%, a relative improvement of 21%. The compute
cost per correct answer rises from 20.2 s → 55.5 s, a **175% increase**.

**Each additional point of accuracy costs progressively more.** Going from
baseline to final buys 5 points for 1.9× the compute; going on to k=3 buys
another 4.6 points for 2.7× the baseline compute. On free-tier hosted inference
the same curve appears as rate-limit pressure rather than latency.

---

## What these add up to

Three of these findings point the same way, and together they are the argument
for the shape of this system:

1. Most errors are **silent** (§1) — you cannot tell by looking.
2. The techniques that raise accuracy **increase the silent share** and make the
   hardest questions **net worse** (§1, §4).
3. But cheap, available signals — **question length** (§5) and **agreement**
   (§6, §7 of `RESULTS.md`) — do predict failure.

An accuracy number alone would have shown a clean 45.6% → 50.4% improvement and
concealed all three. That gap between "the number went up" and "the system got
safer" is what an evaluation-driven build is for.

---

## Reproducing

```bash
uv run python scripts/research.py
```

Writes `results/findings.json` and prints this report's underlying numbers.
Per-question labels are cached in `results/.labels_cache.json`; delete it to
recompute from scratch.

### Limitations that apply throughout

- **One model.** Everything here is `qwen2.5-coder:7b`. Whether these patterns
  hold for a frontier model is untested, and the silent-failure ratio in
  particular could differ substantially.
- **One benchmark, one version.** BIRD dev `dev_20240627`, whose annotations
  contain known errors. Some scored failures are defensible predictions.
- **Failure categories are heuristic.** See `docs/failure-taxonomy.md` for what
  the classifier can and cannot see.
- **Per-database and per-difficulty slices are small.** Where a bucket is under
  ~30 questions it is flagged inline. The `n=500` results (§1, §4, §5) are the
  ones to lean on.

---

## 9. Most of the confidence model's features do nothing, and one is harmful

The scorer uses 14 features. Fitting on `calib` (n=200) and scoring on the
disjoint `eval_500` slice (n=200), leave-one-out tells a blunt story.

**Removing these hurts** (positive = model gets worse without it):

| Feature | ΔBrier when removed |
|---|---:|
| `agreement_rate` | **+0.0224** |
| `result_empty` | **+0.0170** |
| `has_aggregation` | +0.0080 |
| `sql_length` | +0.0036 |

**Removing these helps** (negative = the feature was making the model worse):

| Feature | ΔBrier when removed |
|---|---:|
| `question_length` | **−0.0061** |
| `n_correction_rounds` | −0.0029 |
| `n_joins` | −0.0021 |
| `evidence_provided` | −0.0021 |
| `execution_errored` | −0.0008 |
| `schema_coverage` | −0.0004 |

### The one that surprised me: question length

§5 of this document reports question length as one of the most statistically
solid results here — r = −0.224, p < 0.0001, accuracy falling 61% → 14% across
buckets. I described it as "the cheapest available prior on whether an answer
will be trustworthy."

**It is the single most harmful feature in the multivariate model.** Dropping it
improves Brier from 0.1943 to 0.1882.

Both results are correct, and the tension between them is the point. Question
length genuinely predicts failure *on its own*. But whatever it captures —
question complexity — is already captured better by the agreement rate, because
a complex question is one the model samples inconsistently on. Once agreement is
in the model, length contributes variance rather than signal. **A strong
univariate predictor is not automatically a useful feature.**

### A redundancy bug in my own feature set

`has_subquery` and `has_nested_select` correlated at **r = 1.0000** on this data —
they were the same feature computed two ways (`(SELECT` versus counting the
occurrences of `SELECT`). Their single-feature Brier and ECE were identical to
four decimal places. One was dead weight, and it went unnoticed until this
ablation because nothing in the pipeline checked for collinearity.

**Fixed:** `has_nested_select` now means a second SELECT that is *not* a
parenthesised subquery — a set operation or CTE — so the two flags describe
different structures. A regression test asserts no pair of features exceeds
r = 0.999.

Worth stating plainly: **the fix did not improve the model.** Brier stayed at
0.1943 and ECE moved from 0.0507 to 0.0499, which is noise. That is the expected
result — a perfectly redundant feature contributes nothing, so removing the
redundancy cannot recover anything. What the fix buys is that the feature now
measures something real, so future ablations are interpretable. Reporting it as
a performance win would be inventing a result.

### What a smaller model does

| Feature set | Brier ↓ | ECE ↓ | n features |
|---|---:|---:|---:|
| agreement only | 0.2186 | 0.0756 | 1 |
| agreement + `result_empty` | 0.1939 | 0.0707 | 2 |
| + `has_aggregation` | 0.1843 | 0.0734 | 3 |
| **+ `sql_length`** | **0.1770** | 0.0643 | **4** |
| all 14 (shipped) | 0.1943 | **0.0507** | 14 |

Agreement alone is meaningfully worse than the full model, so the query-structure
features are earning their place — but four of them appear to do the work of
fourteen.

### ⚠️ Why the 4-feature model was NOT shipped

**That table was produced by choosing feature sets and reading their scores on
the evaluation split.** Selecting a model that way is a form of fitting to the
test set, and the 0.1770 is therefore optimistic by an unknown amount. It is
exactly the class of mistake this project exists to avoid, and reporting it as
"the better model" would undercut everything else here.

Confirming it honestly needs a third split held out from both fitting and
selection. That has not been done, so **the shipped scorer remains the 14-feature
model**, whose 0.1943 / 0.0507 were measured on data untouched by any selection
decision.

This section is therefore a *diagnostic*, not a result: it says the feature set
is probably larger than it needs to be, and names the collinear pair and the
harmful feature as concrete leads. Acting on it is future work with a proper
split, not a number to quote.
