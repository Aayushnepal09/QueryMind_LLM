# SQLSentinel

**A text-to-SQL agent that knows when it's wrong.**

Natural language → SQL, benchmarked on BIRD-SQL, with a calibrated confidence
score that routes uncertain queries to a human instead of executing them blind.

Rebuilt from [QueryMind](#what-changed-from-querymind), which worked but claimed
an accuracy nobody could verify. The headline artifact here is not the agent —
it is **[the evaluation](results/RESULTS.md)**: a measured baseline, a measured
delta, a calibration curve, a routing curve, and an honest account of what still
fails.

```
NL question ──► Schema Linker ──► SQL Generator ──► Self-Correction ──► Confidence Scorer
                (introspection)   (k samples,       (execute, re-prompt   (agreement +
                                   few-shot)         on error, ≤2)         14 features)
                                                                                │
                                            ┌───────────────────────────────────┴───┐
                                            ▼                                       ▼
                                    high confidence                    low confidence / risk rule
                                    auto-execute                       human review queue
```

Every step emits OpenTelemetry spans. Every eval run logs to MLflow. A CI gate
blocks merges on accuracy regressions.

---

## Results

| | n | EX | 95% CI |
|---|---:|---:|---:|
| Chance floor (`SELECT 1`) | 500 | 3.0% | ±1.5 |
| **Baseline** — ported QueryMind prompt, single-shot | 500 | **45.6%** | ±4.3 |
| **Final** — few-shot + self-correction | 500 | **50.4%** | ±4.4 |
| k=3 self-consistency | 200 | **55.0%** | ±6.8 |

Paired on the same questions: **+4.8 points** for the final configuration
(p = 0.014), **+9.0 points** for k=3 self-consistency (p = 0.008). Both
significant under an exact McNemar test.

**→ [results/RESULTS.md](results/RESULTS.md)** — generated from MLflow and the
analysis artifacts, never hand-edited, so the reported numbers cannot drift from
the measured ones.
**→ [results/FINDINGS.md](results/FINDINGS.md)** — eight secondary findings the
headline number conceals.
**→ [results/technique-notes.md](results/technique-notes.md)** — why each
technique moved the number, or didn't.

Three things about how numbers are reported here:

1. **Every accuracy carries its confidence interval and n.** On the 500-question
   evaluation split the 95% CI is ±4.4 points; on the 50-question development
   loop it is ±13. A delta smaller than the interval is labelled as
   indistinguishable from noise rather than claimed as an improvement.
2. **The chance floor is ~3%, not 0%.** A predictor emitting `SELECT 1` scores
   3.0% because BIRD compares result *sets* and some gold queries return a
   scalar `1`. Read every number against that floor
   ([why](results/baseline-floor.md)).
3. **Negative results are kept.** Techniques that did not help have rows in the
   table with their real numbers, and a leakage bug that inflated an early
   result by 23.5 points is documented in
   [results/quarantine/](results/quarantine/README.md) rather than deleted.

### The finding that matters most

**59% of the baseline's wrong answers execute cleanly and return non-empty
results** — they are indistinguishable from correct answers without the gold
query. Self-correction *raises* that share to 71% while halving loud execution
errors: a safety regression concealed inside a 4.8-point accuracy improvement.

That gap between "the number went up" and "the system got safer" is the entire
argument for building the confidence layer rather than chasing another accuracy
point. Details, and seven more findings, in
[results/FINDINGS.md](results/FINDINGS.md).

---

## What the evaluation found

Beyond the headline delta, eight secondary analyses
([results/FINDINGS.md](results/FINDINGS.md)):

| Finding | Measured |
|---|---|
| Most errors are **silent** — they run and return plausible rows | 59% of baseline errors; 71% after self-correction |
| Per-database accuracy spread | **65 points** (22.6% → 88.1%), non-overlapping CIs |
| Schema size predicts difficulty | **No** — no correlation significant; table count trends positive |
| The +4.8 point gain churned | fixed 61, broke 36, **net negative on challenging questions** |
| Question length predicts failure | r = −0.224, **p < 0.0001**; 61% → 14% across buckets |
| BIRD's evidence field is worth | +20 points overall, **2.5× more on moderate than simple** |
| Agreement tracks BIRD's difficulty labels | **No** — the signals are complementary, not redundant |
| Zero-row string-filter failures are case problems | **No** — only 8% recoverable by relaxing the match |

---

## Why this exists

QueryMind's README claimed *"92% accuracy on complex multi-table schemas."* That
number came from a private test set that does not exist in the repository — no
fixtures, no gold queries, no harness. It was unfalsifiable, and therefore worth
nothing to a reader.

SQLSentinel replaces it with three properties a demo does not have:

| Property | How |
|---|---|
| **Benchmarked** | Public BIRD-SQL dev set, scored by BIRD's own official execution-accuracy script (wrapped, not reimplemented) |
| **Calibrated** | Every query gets a confidence score; the reliability diagram shows whether that score means anything |
| **Observable** | OpenTelemetry traces, per-query cost and latency, and a CI gate that fails on regressions |

---

## Quick start

```bash
uv sync --extra dev
```

Get the BIRD dev set from <https://bird-bench.github.io/> and unpack it to
`data/bird/dev_20240627/` (full steps: [docs/reproducing.md](docs/reproducing.md)).

Verify the harness before trusting any number:

```bash
uv run python -m sqlsentinel.eval --split dev_50 --predictor stub
```

That should report ~3–4% and not crash. Then run the agent:

```bash
uv run python -m sqlsentinel.eval --split dev_50 --predictor agent
```

Or bring up the whole system:

```bash
docker compose up
```

API on `:8000`, review UI on `:8501`, trace viewer on `:6006`.

---

## Running it for free

This project has a hard constraint: **zero budget**. Both providers are free,
and the choice between them is a real engineering trade-off rather than a
formality.

| | Local `qwen2.5-coder:7b` | `gemini-2.5-flash` free tier |
|---|---|---|
| Cost | $0 | $0 |
| Latency | ~20 s/query (GTX 1660 Ti, 6 GB) | ~3 s/query |
| Throughput | unlimited | **~0.8 req/min sustained** after quota and backoff |
| 500 questions | ~3 hours | ~10 hours |

The measured sustained rate is the finding that decided the architecture: the
hosted model is six times faster per call but its free-tier quota makes it
*slower* for bulk evaluation. The local model became the workhorse; Gemini is
the comparison point.

**Response caching is load-bearing, not an optimisation.** Every call is cached
on `(provider, model, system, user, temperature, sample_index)`. Re-running an
unchanged configuration is instant. `sample_index` is in that key deliberately —
without it, all *k* samples of a self-consistency draw collapse to one cached
response, agreement rate becomes a constant 1.0, and the entire confidence
signal dies silently. There is a test pinning it.

---

## How the evaluation is set up

BIRD dev is 1,534 questions across 11 real databases (75 tables, 798 columns).
They are split once, seeded, and committed to
[`results/splits.json`](results/splits.json):

| Split | n | Purpose |
|---|---:|---|
| `eval_500` | 500 | **Reported.** Nothing is ever fitted on these. |
| `dev_50` | 50 | Fast development loop. A strict subset of `eval_500`. |
| `calib` | 1,034 | Fits the confidence model. **Disjoint from `eval_500`.** |

Stratified on difficulty × database — `eval_500` matches the population
distribution to within 0.1 points and covers all 11 databases.

The `calib`/`eval_500` disjointness is the project's most important invariant.
CLAUDE.md calls training and reporting on the same questions *"the one mistake
that would invalidate the whole project"*, so it is enforced in code:
`ConfidenceModel.fit()` takes the evaluation ids as `forbidden_ids` and raises
on any overlap. It is a guard, not a convention.

Why 500 and not all 1,534: at n=500 the 95% CI is ±4.4 points, enough to support
the size of delta these techniques actually produce, and a full k=5 run on local
hardware is an overnight job. The choice and its cost are recorded in
[PROGRESS.md](PROGRESS.md).

---

## The confidence scorer

The differentiating piece. Two versions, both reported so the delta between them
is visible:

**v1 — agreement rate.** Generate *k* candidates at temperature 0.7, execute
each, normalise the result sets (order-insensitive, values stringified), and
take the fraction agreeing with the modal result. Agreement is computed over
*executed results*, not SQL text — two differently-written queries returning the
same rows are the same answer, which is exactly the equivalence BIRD scores on.

**v2 — calibrated classifier.** Logistic regression with isotonic calibration
over 14 features: agreement rate, tables referenced, aggregation/subquery/nesting
flags, pre-correction execution error, correction rounds, result size, empty
result, question length, evidence presence, schema coverage, SQL length, joins.

Reported with a **reliability diagram, Brier score and ECE**, because a
well-calibrated 0.6 is more useful than an overconfident 0.9.

**Risk overrides are not probabilistic.** Independent of confidence, these force
review: any non-`SELECT` statement, a failed execution, an oversized result set,
an unfiltered scan returning many rows. A `DELETE` does not become safe because
the model felt sure about it.

### Does the confidence score mean anything?

<p align="center">
  <img src="results/calibration-k3-eval200.png" alt="Reliability diagram" width="46%">
  <img src="results/routing-k3-eval200.png" alt="Routing curve" width="52%">
</p>

**Left — reliability.** Observed accuracy rises monotonically with predicted
confidence across all four buckets (0.08 / 0.23 / 0.58 / 0.69), so the score
ranks risk correctly. It sits below the diagonal throughout, meaning the raw
agreement rate is systematically *overconfident* — which is what the v2
calibration layer exists to correct. Marker area is bin population.

**Right — routing.** Every point above the dashed line beats routing at random.
At the marked operating point, **sending 22% of queries to review catches 39% of
all incorrect answers**, and what still auto-executes is 65.0% correct rather
than 54.5%.

The curve is a step function because k=3 admits only four possible confidence
values — a concrete answer to "what is the right k?": three separates risk into
four useful bands, and is not enough to tune a threshold finely.

---

## Safety

The original executed generated SQL directly against a production PostgreSQL
database with no guard. SQLSentinel enforces read-only in three independent
layers, because any single one can be defeated:

1. **Statement inspection** — reject anything that is not a single `SELECT`/`WITH`.
   Comments are stripped first, so they cannot mask a write; stacked statements
   are rejected outright.
2. **Connection mode** — SQLite opens `?mode=ro`; Postgres sets the transaction
   read-only. Enforced by the database, not by our parser.
3. **Hard timeout** — wall-clock cap, so a pathological query cannot hang a run.

---

## What changed from QueryMind

Full audit: [docs/migration-notes.md](docs/migration-notes.md). `main` still
points at the original — the before state is evidence, not clutter.

**Kept** (all three from `streamlit_app.py`, the only real assets in 503 lines):
the schema-context *shape*, the generation prompt, and the SQL extractor. Its
Postgres DSN builder became `executor.postgres_dsn_from_env()`.

The original application itself is **not carried on this branch** — it lives on
`main`, untouched, which is what makes `git diff main sqlsentinel` the complete
before/after.

**Replaced:** the hardcoded `DATABASE_SCHEMA` string became live introspection
across 11 databases. The extractor was hardened — the original stripped
` ```sql ` fences and nothing else, so a response with a prose preamble and a
trailing explanation was handed to the executor verbatim.

**Dropped:** QueryMind's `default LIMIT 100`. Under BIRD's exact-result-set
scoring, an unrequested `LIMIT` turns correct answers into failures.

**Also found, and fixed or documented:**
- An SSH keypair committed in the repository's history (purged; the key should
  be treated as compromised and rotated).
- The Gemini key read from a secret named `OPENAI_API_KEY`.
- `test_render_database.py` opened a production connection *on import* — anything
  that auto-discovered it would have hit prod.

The spec expected more prior art than existed: there was no FastAPI layer, no
chain-of-thought, no few-shot exemplars, no schema introspection. The Phase 1
baseline is therefore a genuinely naive zero-shot prompt, which makes the
subsequent deltas measure real technique rather than re-tuning.

---

## Known limitations

- **`dev_50` is underpowered, and this was measured rather than assumed.** Run
  with the identical configuration, the 50-question development loop scored
  62.0% while the full 500-question split scored 45.0% — **a 17-point
  overestimate whose 95% CI [48.2, 74.1] did not contain the true value**,
  despite stratification. Reporting the development-loop number would have
  claimed 62% for a system that scores 45.6%: the same class of error as
  QueryMind's unverifiable "92%", arrived at honestly. All reported numbers come
  from `eval_500`.
- **The failure taxonomy is heuristic.** It compares SQL text, not semantics, so
  it cannot tell a genuinely wrong join path from an equivalent one written
  differently. Limits stated in [docs/failure-taxonomy.md](docs/failure-taxonomy.md).
- **BIRD's own annotations contain known errors.** Some scored failures are cases
  where the prediction is defensible. This is why the benchmark is described as
  realistic and deliberately hard rather than as a ranking.
- **The CI gate is a smoke alarm, not a measurement.** On 50 questions a 3-point
  threshold sits well inside the noise; it catches breakage, not regression.
- **Schema pruning is close to a no-op on BIRD dev** (99% of tables retained,
  1.3% prompt reduction, identical accuracy). The measured reason is in
  [FINDINGS §2](results/FINDINGS.md): schema *size* does not predict difficulty
  here — no size measure correlates significantly with per-database accuracy,
  and `superhero` (10 tables) scores 88% while `thrombosis_prediction`
  (3 tables) scores 23%. Pruning attacks the wrong variable.
- **Not a leaderboard entry.** Top BIRD systems are large ensembles built over
  months. This is a single-model agent with an honest evaluation around it.

---

## Project layout

```
src/sqlsentinel/
├── llm.py             provider abstraction + response cache
├── schema_linker.py   introspection, DDL rendering, pruning
├── generator.py       prompt + SQL extraction
├── executor.py        read-only SQLite/Postgres, timeout, normalization
├── retrieval.py       TF-IDF exemplar retrieval (calib split only)
├── agent.py           orchestration, self-correction, k-sample voting
├── confidence.py      features, calibrated model, Brier/ECE
├── router.py          risk rules, threshold, routing curve
├── tracing.py         OpenTelemetry (no-ops when unconfigured)
├── api.py             FastAPI service
└── eval/              official BIRD harness wrapper + splits
app/review_ui.py       Streamlit review queue
scripts/               experiment sweep, analysis, report generation
third_party/           BIRD's official scorer, wrapped not reimplemented
results/               committed metrics, figures, splits, traces
tests/                 152 tests
```

## Documentation

| | |
|---|---|
| [results/RESULTS.md](results/RESULTS.md) | All measured numbers |
| [PROGRESS.md](PROGRESS.md) | Build log, decisions and their rationale |
| [docs/reproducing.md](docs/reproducing.md) | Fresh clone → scored number |
| [docs/migration-notes.md](docs/migration-notes.md) | QueryMind audit |
| [docs/failure-taxonomy.md](docs/failure-taxonomy.md) | Failure categories |
| [CLAUDE.md](CLAUDE.md) | Project specification |
