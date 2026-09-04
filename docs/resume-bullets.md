# Resume bullets

Drafted from measured results. Every number is traceable to
`results/RESULTS.md`, `results/FINDINGS.md`, or `results/findings.json`.

Pick two or three. Do not use all of them — a reader skims, and the strongest
version of this project is the honesty of the evaluation, not the length of the
list.

---

## Recommended set (three bullets)

> **SQLSentinel — evaluation-driven text-to-SQL agent** · Python, FastAPI,
> scikit-learn, MLflow, Docker
>
> - Rebuilt a text-to-SQL tool whose accuracy claim was unverifiable into a
>   benchmarked system, measuring it on the public BIRD-SQL dev set with the
>   official execution-accuracy harness: **45.6% → 50.4% (n=500, +5.0 paired, p=0.014)**
>   single-shot to final, and **55.0%** with k=3 self-consistency
>   (**+9.0 points, p=0.008**), all comparisons paired via exact McNemar rather
>   than overlapping confidence intervals.
> - Built a calibrated confidence scorer (self-consistency agreement plus 14
>   query features, isotonic-calibrated logistic regression) that **routes 27%
>   of queries to human review while catching 47% of all incorrect answers**,
>   raising auto-executed accuracy from **54.5% to 67.1%** — motivated by the
>   finding that **59% of the agent's wrong answers execute cleanly and return
>   plausible data**, and are therefore invisible without it.
> - Instrumented the system end to end — OpenTelemetry tracing, per-query cost
>   and latency, MLflow experiment tracking, a Dockerised FastAPI service, and a
>   CI accuracy-regression gate — and found a retrieval leakage bug worth **24.5
>   points** of inflated accuracy by cross-checking two splits, documenting it
>   rather than deleting the result.

---

## Alternatives, by what you want to emphasise

### Rigour / evaluation

- Designed a fixed, seeded, difficulty-stratified 500-question evaluation split
  disjoint by construction from the confidence model's training data, enforced
  in code (`fit()` raises on any overlap); demonstrated that the 50-question
  development loop **overestimated accuracy by 17 points with a confidence
  interval that excluded the true value**, and reported every result with its
  interval and a paired significance test.

### Safety / production judgement

- Quantified a class of failure that accuracy alone conceals: **59% of incorrect
  queries execute successfully and return non-empty results**, and self-correction
  *raised* that share to 71% while halving visible errors — a safety regression
  hidden inside a 5-point accuracy gain — then built confidence-based routing
  and a non-expert review interface as the response.

### Analysis depth

- Produced eight secondary findings from the trace data, including a **65-point
  accuracy spread across the benchmark's 11 databases** that schema size fails
  to predict (no correlation significant; table count trends *positive*),
  explaining why a schema-pruning optimisation measured as a no-op, and a
  **question-length predictor of failure at r = −0.224, p < 0.0001**.

### Engineering breadth

- Shipped the full loop on zero budget: local `qwen2.5-coder:7b` via Ollama plus
  a free-tier hosted model behind one client interface, with a SQLite response
  cache keyed on sample index (without which k-sample self-consistency silently
  collapses to a constant), three-layer read-only query enforcement, and a
  reproducible Docker deployment verified end to end.

---

## Interview talking points

Prepared answers for the questions these bullets invite.

**"Why is the accuracy only 50%?"**
Because it is measured. BIRD is deliberately hard — real, dirty databases with
external-knowledge requirements — and published single-shot results for
frontier models sit in the 45–55% range. This runs a 7B model on a consumer GPU
for $0. The predecessor claimed 92% against a private test set that does not
exist in the repository; that number was worth nothing and this one is
falsifiable.

**"What was the hardest bug?"**
A leakage bug I found by noticing that two runs of the identical configuration
scored 78% and 54.5% on differently-drawn samples of the same benchmark.
Few-shot exemplars were retrieved from the same split being evaluated, so every
question retrieved *itself* at similarity 1.0 and its gold SQL went into the
prompt. Worth 24.5 points, confirmed by re-running clean: 78.0% -> 53.5%. The project already had a leakage guard — on the
*confidence model* — and it did not cover the *generator*. Two paths through the
same split, one guard. The fix, regression tests, and a written postmortem are
in `results/quarantine/`.

**"What would you do differently?"**
Report on the large split from day one. I developed against 50 questions because
it was fast, and it overestimated accuracy by 17 points with an interval that
excluded the true value — despite stratification. At n=50 there are about four
questions per database, and that is not enough to resolve anything.

**"What did not work?"**
Schema pruning and few-shot retrieval both measured as null, and I kept the rows.
Pruning failed for a reason worth knowing: it attacks schema size, and I later
measured that size does not predict difficulty on this benchmark — `superhero`
has 10 tables and scores 88%, `thrombosis_prediction` has 3 and scores 23%. The
difference is whether column names carry recoverable meaning.

**"What is the single most useful thing you learned?"**
That an accuracy number does not tell you whether you would notice being wrong.
Most of this system's errors are silent, and the technique that most improved
accuracy made the silent share worse. That gap is the whole reason the
confidence layer exists, and I would not have seen it without per-question
traces.

---

## Numbers, with sources

| Claim | Value | Source |
|---|---|---|
| Baseline accuracy | 45.6% ± 4.3 (n=500) | `results/RESULTS.md` |
| Final accuracy | 50.4% ± 4.4 (n=500) | `results/RESULTS.md` |
| Final delta (paired) | +5.0 points, 61 fixed / 36 broken, p = 0.014 | `results/comparisons.json` |
| k=3 self-consistency | 55.0% (n=200), +9.0 points, p = 0.008 | `results/comparisons.json` |
| Chance floor | 3.0% | `results/baseline-floor.md` |
| Routing | 27% routed, 47% of errors caught | `results/routing-k3-eval200.json` |
| Auto-executed accuracy | 54.5% → 67.1% | `results/routing-k3-eval200.json` |
| Calibration | Brier 0.262 → 0.194, ECE 0.203 → 0.051 | `results/calibration-k3-eval200-v2.json` |
| Silent failure rate | 59% of errors (71% after self-correction) | `results/findings.json` |
| Leakage bug | 24.5 points (78.0% → 53.5% on re-run) | `results/quarantine/README.md` |
| dev_50 overestimate | 17 points, CI excluded truth | `results/technique-notes.md` |
| Per-database spread | 22.6% – 88.1% | `results/findings.json` |
| Question length | r = −0.224, p < 0.0001 | `results/findings.json` |
| Evidence field | +20 points, p = 0.031 | `results/comparisons.json` |
| Benchmark | BIRD dev `dev_20240627`, 1,534 questions, 11 databases | `docs/reproducing.md` |
| Before/after diff | `git diff v0-querymind main` | tag `v0-querymind` |
| Tests | 264 passing, 92% coverage | `uv run pytest --cov=sqlsentinel` |

**Do not round these upward.** The credibility of the whole project rests on the
numbers being exactly what was measured, and anyone who asks can run
`scripts/research.py` and check.
