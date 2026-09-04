# SQLSentinel — design specification

The requirements this project was built against. Kept because the code cites it:
where a module comment says "spec §6", this is the section it means.

---

## 1. What this replaces

QueryMind: natural language → SQL via few-shot prompting, schema-aware context,
backed by cloud PostgreSQL. It worked, but had one structural weakness — **its
accuracy claim was unbenchmarked.** "92% on complex multi-table schemas" was
self-measured against a private test set. No reviewer can verify that, so it is
worth nothing.

SQLSentinel keeps the working ideas and adds the three things that turn a demo
into a system:

1. **It is benchmarked.** Every claim measured on the public BIRD-SQL dev set
   using the official execution-accuracy harness.
2. **It knows when it is unsure.** Every generated query gets a calibrated
   confidence score. Low-confidence or high-risk queries route to human review
   instead of executing blind.
3. **It is observable.** Tracing of every agent step, cost and latency per
   query, and a CI gate that blocks merges on accuracy regressions.

The headline artifact is not the agent. It is the **evaluation**: a baseline
number, an improved number, a calibration plot, a cost/latency table, and an
honest taxonomy of what still fails.

## 2. Non-goals

- Fine-tuning or training a model. API and local inference only.
- Chasing the BIRD leaderboard. Top systems are large ensembles built over
  months.
- Multi-database federation or cross-DB joins.
- A polished custom front-end. Streamlit is sufficient and correct here.
- Auth, multi-tenancy, user management.
- Every SQL dialect. SQLite is the eval target; a dialect boundary keeps the
  PostgreSQL path working without generalising further.
- Novel research in confidence calibration. Simple and defensible beats clever
  and unfinished.

## 3. Architecture

```
NL question ──► Schema Linker ──► SQL Generator ──► Self-Correction ──► Confidence Scorer
                                                                              │
                                            ┌─────────────────────────────────┴───┐
                                            ▼                                     ▼
                                    auto-execute                     human review queue
```

| Component | Input | Output | Notes |
|---|---|---|---|
| `SchemaLinker` | question, full DB schema | pruned schema string | Start dumb: include all tables if the DB is small. Optimise only if token cost hurts. |
| `SQLGenerator` | question, schema, evidence, k | k candidate SQL strings | BIRD provides an `evidence` field — use it. |
| `Executor` | SQL, connection | rows or error | Read-only. Hard timeout. Dialect-agnostic interface, SQLite + Postgres impls. |
| `ConfidenceScorer` | k candidates + execution results | float in [0,1] | See §5. |
| `Router` | confidence, risk features | `AUTO` / `REVIEW` | Threshold is a reported tunable, not a magic constant. |
| `EvalHarness` | predictions file, gold file | EX accuracy, per-difficulty breakdown | Wrap BIRD's official script. Do not reimplement it. |

### API surface

```
POST /query          → sql, confidence, decision, result | review_id, trace_id, cost_usd, latency_ms
GET  /review/queue   → pending low-confidence queries
POST /review/{id}    → approve | edit | reject
GET  /health
```

The Streamlit app exposes the same pipeline interactively: ask a question, see
the answer with its confidence and a plain-English description, or work the
review queue.

## 4. Stack

Python 3.11+, `uv`. LLM behind a thin `LLMClient` interface so at least two
providers can be compared. MLflow for eval tracking, OpenTelemetry for tracing,
scikit-learn for the confidence model, FastAPI + Pydantic for the service,
Streamlit for review, SQLite + PostgreSQL, Docker, GitHub Actions.

## 5. The confidence scorer

**v1 — self-consistency.** Generate `k` candidates at temperature ~0.7. Execute
each. Normalise result sets (sort rows, sort columns). The **agreement rate** —
fraction of candidates producing the modal result set — is the strongest single
signal and nearly free to compute.

**v2 — per-query features.** A small calibrated classifier over: agreement rate,
tables referenced, aggregation/subquery/nesting flags, pre-correction execution
error, correction rounds, result row count, question length, evidence presence,
schema coverage.

Train on a held-out slice where ground truth is known. **Report calibration
honestly** — reliability diagram and Brier score. A well-calibrated 0.6 is more
useful than an overconfident 0.9.

**Risk override.** Independent of confidence, force `REVIEW` for: any
non-`SELECT` statement, queries with no `WHERE` against large tables, and
results exceeding N rows. Safety rules are not probabilistic.

**The money metric:** at a chosen threshold, report *"routed X% of queries to
review, which contained Y% of all incorrect queries."*

## 6. Data

**BIRD-SQL** — <https://bird-bench.github.io/>. Dev set: 1,534 question/SQL
pairs, 11 databases. Includes an `evidence` field (external knowledge hints).
Ships an official execution-accuracy script — use it. The databases are real and
dirty, which is the point.

BIRD's annotations contain known errors and were re-cleaned in late 2025. Pin
the version downloaded and record it, and describe BIRD as "a realistic,
deliberately hard benchmark" rather than claiming any ranking.

## 7. Definition of done

1. A reproducible BIRD eval showing a measured baseline → improved delta
2. A working confidence-routing loop with a quantified reduction in incorrect
   query execution
3. Traces plus per-query cost and latency data
4. A Dockerised deployment and a CI regression gate
5. An honest failure taxonomy

A smaller project that is honestly evaluated beats a larger one that is not.

## 8. Working agreements

- **Measure before optimising.** No technique lands without a before/after
  number.
- **Cheap loop first.** Develop against a small subset; run the full split only
  at phase boundaries.
- **Cache aggressively.** Cached LLM responses make re-runs free.
- **Read-only always.** No code path writes to a benchmark database.
- **Never train and evaluate the confidence model on the same questions.** The
  one mistake that would invalidate the whole project.
- **Commit results, not just code.** Metrics tables and figures are versioned.
- **Record honest numbers.** If a technique makes things worse, keep the row and
  say so. Negative results are credibility.

## 9. Open questions the build had to answer

- Which model as primary? Compare two and pick on cost-per-correct-answer, not
  raw accuracy.
- Does QueryMind's existing prompting beat a naive baseline? Worth knowing and
  reporting either way.
- Is schema linking worth it, or does full-schema context win?
- Optimal k for self-consistency — 3, 5, or 7?
- Where to set the routing threshold? Present a curve rather than picking a
  number arbitrarily.

Answers, including the ones that came out negative, are in
[`results/technique-notes.md`](../results/technique-notes.md) and
[`results/FINDINGS.md`](../results/FINDINGS.md).
