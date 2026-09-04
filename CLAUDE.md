# SQLSentinel

Evolving **QueryMind AI** from an LLM-powered SQL engine into an evaluation-driven, observable text-to-SQL agent that knows when it's wrong.

> **How to use this file:** Place at the root of the existing QueryMind repo. It is the project's source of truth — architecture, git workflow, constraints, milestones, and definition of done. Work through the phases in order. Do not skip Phase 0.

---

## 1. Context: this is a brownfield project

QueryMind already exists: natural language → SQL via few-shot prompting, schema-aware context, chain-of-thought reasoning, backed by cloud PostgreSQL on Render. It works, but it has one structural weakness — **its accuracy claim is unbenchmarked.** "92% on complex multi-table schemas" is self-measured against a private test set. No reviewer can verify it, and no hiring manager will weight it.

SQLSentinel fixes that and goes further. We keep the working core ideas, rebuild the surface around them, and add the three things that turn a demo into a system:

1. **It is benchmarked.** Every claim is measured on the public BIRD-SQL dev set using the official execution-accuracy harness.
2. **It knows when it's unsure.** Every generated query gets a calibrated confidence score. Low-confidence or high-risk queries route to human review instead of executing blind.
3. **It is observable.** Full tracing of every agent step, cost and latency per query, and a CI gate that blocks merges on accuracy regressions.

The headline artifact is not the agent. It is the **evaluation report**: a baseline number, an improved number, a calibration plot, a cost/latency table, and an honest taxonomy of what still fails.

### What to keep from QueryMind

Audit before rewriting. Likely worth preserving:

- Prompt templates and few-shot exemplars (they encode real tuning effort)
- Schema introspection / context-injection logic
- Chain-of-thought prompting structure
- Any query-parsing or SQL-extraction utilities

Likely worth replacing:

- The FastAPI layer — rewriting cleanly around the new agent contract (see §4)
- Direct PostgreSQL coupling — BIRD ships SQLite, so the executor needs a dialect boundary
- Any hardcoded schema assumptions
- The existing accuracy measurement, which is superseded entirely

**Keep `main` pointing at the original QueryMind.** The "before" state is not clutter — it is evidence. The before/after delta is part of the story.

---

## 2. Git workflow

### Branch structure

```
main                    ← original QueryMind, untouched
 └── sqlsentinel        ← long-lived integration branch, all work merges here
      ├── feat/eval-harness
      ├── feat/baseline-agent
      ├── feat/schema-linking
      └── ...
```

Create the integration branch once:

```bash
git checkout main
git checkout -b sqlsentinel
git push -u origin sqlsentinel
```

### One branch per feature — non-negotiable

Every item below gets its own branch off `sqlsentinel`, merged back when its acceptance criteria pass. Never stack unrelated work on one branch.

| Phase | Branch | Merges when |
|---|---|---|
| 0 | `feat/eval-harness` | stub predictor scores end-to-end |
| 0 | `chore/project-scaffold` | deps, config, structure settled |
| 1 | `feat/llm-client` | model abstraction works against 2 providers |
| 1 | `feat/baseline-agent` | baseline EX number logged to MLflow |
| 2 | `feat/few-shot-retrieval` | measured Δ recorded |
| 2 | `feat/schema-linking` | measured Δ recorded |
| 2 | `feat/self-correction` | measured Δ recorded |
| 3 | `feat/confidence-scorer` | calibration plot + Brier score produced |
| 3 | `feat/risk-router` | routing curve produced |
| 3 | `feat/review-ui` | reviewer can approve/edit/reject |
| 4 | `feat/api-rewrite` | new FastAPI surface passes tests |
| 4 | `feat/observability` | traces visible for a full query lifecycle |
| 4 | `feat/docker-compose` | `docker compose up` works from clean clone |
| 4 | `ci/eval-regression-gate` | deliberate regression fails the build |
| 5 | `docs/results-and-failure-taxonomy` | README complete |

Branch naming: `feat/`, `fix/`, `chore/`, `ci/`, `docs/`, `refactor/`. Lowercase, hyphenated, descriptive.

### Workflow per feature

```bash
git checkout sqlsentinel && git pull
git checkout -b feat/thing
# ... work, committing in small logical units ...
git checkout sqlsentinel && git merge --no-ff feat/thing
```

Use `--no-ff` so each feature is a visible unit in the history. A clean, legible commit graph is itself a portfolio signal — a reviewer will run `git log --graph` on this.

### Commit conventions

Conventional Commits format:

```
feat(confidence): add self-consistency agreement scorer
fix(executor): enforce read-only connection on SQLite
chore(deps): pin mlflow to 2.x
docs(readme): add failure taxonomy section
```

Small, logical commits. A commit that changes the prompt and the API and the Dockerfile is three commits.

### ⚠️ No AI attribution in commits

**Do not add `Co-Authored-By: Claude`, `🤖 Generated with Claude Code`, `Claude-Session:`, or any other AI attribution trailer to commit messages or PR bodies.** Commit messages contain the message only.

Configure this in `.claude/settings.local.json` (auto-gitignored) or `~/.claude/settings.json`:

```json
{
  "attribution": {
    "commit": "",
    "pr": ""
  }
}
```

The `attribution` setting replaced `includeCoAuthoredBy` as of Claude Code v2.0.62; the older key is deprecated. There are known cases where trailers slip through anyway, so add a belt-and-braces `commit-msg` hook at `.git/hooks/commit-msg`:

```bash
#!/usr/bin/env bash
sed -i.bak '/^Co-Authored-By: Claude/d; /Generated with \[Claude Code\]/d; /^Claude-Session:/d' "$1"
rm -f "$1.bak"
```

```bash
chmod +x .git/hooks/commit-msg
```

Verify with `git log -1 --format=%B` after the first commit.

---

## 3. Non-goals (read this twice)

Scope creep kills this project. Explicitly **out of scope**:

- ❌ Fine-tuning or training a model. API models only.
- ❌ Chasing the BIRD leaderboard. Top systems are large ensembles built over months. We are not competing.
- ❌ Multi-database federation or cross-DB joins.
- ❌ A polished custom front-end. Streamlit is sufficient and correct here.
- ❌ Auth, multi-tenancy, user management.
- ❌ Every SQL dialect. SQLite (BIRD-native) is the eval target; keep a dialect boundary so QueryMind's Postgres path still works, but don't generalize further.
- ❌ Novel research in confidence calibration. Simple and defensible beats clever and unfinished.

If a phase runs long, cut from the bottom of the milestone list — never cut the evaluation harness.

---

## 4. Architecture

```
                   ┌──────────────────────────────────────┐
   NL question ──► │  Schema Linker                       │
                   │  (prune tables/cols to relevant set)  │
                   └──────────────┬───────────────────────┘
                                  ▼
                   ┌──────────────────────────────────────┐
                   │  SQL Generator                        │
                   │  few-shot + schema + evidence         │
                   │  k samples @ temperature > 0          │
                   └──────────────┬───────────────────────┘
                                  ▼
                   ┌──────────────────────────────────────┐
                   │  Self-Correction Loop                 │
                   │  execute → on error, re-prompt (≤2x)  │
                   └──────────────┬───────────────────────┘
                                  ▼
                   ┌──────────────────────────────────────┐
                   │  Confidence Scorer                    │
                   │  self-consistency + query features    │
                   │  → calibrated P(correct)              │
                   └──────────────┬───────────────────────┘
                                  ▼
                   ┌─────────────────────────┐
              high │                         │ low conf / high risk
             conf  ▼                         ▼
        ┌──────────────────┐      ┌─────────────────────────┐
        │  Auto-execute    │      │  Human Review Queue     │
        │  return result   │      │  (Streamlit UI)         │
        └──────────────────┘      └─────────────────────────┘

   Every step emits OpenTelemetry spans ──► Langfuse / Phoenix
   Every eval run logs params + metrics ──► MLflow
```

### Component contracts

| Component | Input | Output | Notes |
|---|---|---|---|
| `SchemaLinker` | question, full DB schema | pruned schema string | Port QueryMind's schema-injection logic. Start dumb: include all tables if the DB is small. Optimize only if token cost hurts. |
| `SQLGenerator` | question, pruned schema, evidence, k | k candidate SQL strings | Reuse QueryMind's prompt templates as the starting point. BIRD provides an `evidence` field — use it, it matters a lot. |
| `Executor` | SQL, connection | rows or error | Read-only. Hard timeout (5s). Dialect-agnostic interface, SQLite + Postgres impls. |
| `ConfidenceScorer` | k candidates + execution results | float in [0,1] | See §6. |
| `Router` | confidence, risk features | `AUTO` \| `REVIEW` | Threshold is a reported tunable, not a magic constant. |
| `EvalHarness` | predictions file, gold file | EX accuracy, per-difficulty breakdown | Wrap BIRD's official script. Do not reimplement it. |

### New API surface (`feat/api-rewrite`)

Rewriting the FastAPI layer around the agent contract rather than the old direct-to-SQL flow:

```
POST /query          → { sql, confidence, decision, result | review_id, trace_id, cost_usd, latency_ms }
GET  /review/queue   → pending low-confidence queries
POST /review/{id}    → { action: approve|edit|reject, edited_sql? }
GET  /health
```

Pydantic models for every request/response. The `trace_id` in the response links straight to the tracing backend — a small touch that reads as production experience.

---

## 5. Tech stack

- **Python 3.11+**, `uv` or `poetry`
- **LLM:** Anthropic Claude or OpenAI GPT-4o-class. Abstract behind a thin `LLMClient` interface — you will want to compare at least two.
- **Eval tracking:** MLflow (local file backend is fine)
- **Tracing:** Langfuse (self-hosted via Docker) or Arize Phoenix. Pick one and commit.
- **Confidence model:** scikit-learn (`LogisticRegression` + `CalibratedClassifierCV`)
- **Service:** FastAPI + Pydantic
- **Review UI:** Streamlit
- **DB:** SQLite (BIRD) + PostgreSQL (retained QueryMind path)
- **Packaging:** Docker + docker-compose
- **CI:** GitHub Actions
- **Stretch:** `fastmcp` server exposing the SQL tool

---

## 6. The confidence scorer (the differentiating piece)

This is what makes the project distinctive, so it gets its own section. The design mirrors a human-in-the-loop pattern: score each candidate, flag the uncertain ones, route them to a person.

**v1 signal — self-consistency.** Generate `k=5` candidates at temperature ~0.7. Execute each. Normalize result sets (sort rows, sort columns). The **agreement rate** — fraction of candidates producing the modal result set — is the single strongest confidence signal and nearly free to compute. Ship this first.

**v2 signals — per-query features.** Fit a small calibrated classifier over:

| Feature | Rationale |
|---|---|
| `agreement_rate` | primary signal |
| `n_tables_referenced` | joins are where it breaks |
| `has_aggregation` / `has_subquery` / `has_nested_select` | complexity proxies |
| `execution_errored` (pre-correction) | strong negative signal |
| `n_correction_rounds` | needed fixing → less trustworthy |
| `result_row_count` | 0 rows or enormous results are suspicious |
| `question_length` / `evidence_provided` | input difficulty proxies |
| `schema_coverage` | under/over-selection of tables |

Train on a held-out slice of BIRD dev where ground truth is known. **Report calibration honestly** — reliability diagram and Brier score. A well-calibrated 0.6 is more useful than an overconfident 0.9.

**Risk override.** Independent of confidence, force `REVIEW` for: any non-`SELECT` statement, queries with no `WHERE` against large tables, and results exceeding N rows. Safety rules are not probabilistic.

**The money metric:** at a chosen threshold, report *"routed X% of queries to review, which contained Y% of all incorrect queries."* Routing 20% and catching 60% of errors is a genuinely compelling result — and the sentence a hiring manager remembers.

---

## 7. Data

**Primary: BIRD-SQL** — <https://bird-bench.github.io/>

- Dev set: 1,534 question/SQL pairs, 11 databases, 37+ domains
- Includes an `evidence` field (external knowledge hints) — using it is standard practice
- Ships an official execution-accuracy (EX) evaluation script — **use it**
- Databases are real and dirty, which is the point

**Optional: Spider** — <https://yale-lily.github.io/spider>. Useful only for an easy-vs-hard contrast; models score dramatically higher on Spider than BIRD, which makes a good chart and a good talking point about benchmark difficulty.

**Note on the dev set:** BIRD's annotations contain known errors and were re-cleaned in late 2025. Pin the version you download, record it in the README, and describe BIRD as "a realistic, deliberately hard benchmark" rather than claiming any ranking.

---

## 8. Milestones

### Phase 0 — Audit + harness (Days 1–2) 🚨

**Highest-risk phase, goes first.** No agent code until the eval loop runs end to end.

`chore/project-scaffold`:
- [ ] Create `sqlsentinel` branch off `main`
- [ ] Configure attribution settings + `commit-msg` hook; verify a test commit is clean
- [ ] Audit QueryMind: inventory what's reusable per §1, note it in `docs/migration-notes.md`
- [ ] Restructure into the layout in §10; wire deps and `.env` handling

`feat/eval-harness`:
- [ ] Download BIRD dev set + databases; record version/date
- [ ] Run the official BIRD eval script against a **stub predictor** (always emits `SELECT 1`). Should report ~0% and not crash.
- [ ] Wrap as `python -m sqlsentinel.eval --subset N` so 50 questions run in under a minute
- [ ] MLflow experiment initialized; dummy run logs

**Done when:** one command produces an accuracy number.

**Common failure:** BIRD's database files are large and the eval script has fussy path expectations. Budget real time. Do not improvise your own accuracy metric — result-set comparison has ordering and type edge cases the official script already handles.

---

### Phase 1 — Baseline  

`feat/llm-client`:
- [ ] `LLMClient` abstraction (one method: `complete(system, user) -> str`)
- [ ] Response caching keyed on (prompt, model, temperature, sample index)

`feat/baseline-agent`:
- [ ] Port QueryMind's prompting into a single-shot generator: question + full schema + evidence → SQL
- [ ] SQL extraction (strip markdown fences, handle preamble)
- [ ] Read-only executor with timeout
- [ ] Full dev-set run; log EX, per-difficulty breakdown, latency, token cost to MLflow

**Done when:** you have a baseline number with cost and latency attached. Expect roughly the high-40s to low-50s percent range for a single-shot GPT-4-class model. **This is your "before" number — protect it, you will cite it forever.**

Note the honest framing: QueryMind's prompting *is* the baseline. If it beats a naive prompt, that's a real finding worth reporting.

---

### Phase 2 — Improve the agent  

One branch per technique. **Measure after each.** The per-technique delta table is itself a portfolio artifact.

- [ ] `feat/few-shot-retrieval` — retrieve similar questions, or a fixed high-quality exemplar set
- [ ] `feat/schema-linking` — column/table pruning
- [ ] `feat/self-correction` — on execution error, re-prompt with the error (cap 2 rounds)

**Done when:** you have a table of `technique → EX → Δ → cost`. Realistic landing zone is mid-50s to mid-60s. If you plateau, stop and move on — Phase 3 is where the differentiation lives.

---

### Phase 3 — Confidence + routing  

`feat/confidence-scorer`:
- [ ] k-sample generation, result-set normalization, agreement rate
- [ ] Feature extraction per §6
- [ ] Calibrated classifier on a held-out slice (**no leakage** — never train and report on the same questions)
- [ ] Reliability diagram + Brier score

`feat/risk-router`:
- [ ] Risk-based override rules
- [ ] Tunable threshold
- [ ] **Precision/recall curve for error-catching:** per threshold, % routed vs % of errors caught

`feat/review-ui`:
- [ ] Streamlit: question, candidate SQL, confidence, result preview; approve / edit / reject; decisions logged

**Done when:** you can state the money metric from §6 with real numbers.

---

### Phase 4 — Production surface  

- [ ] `feat/api-rewrite` — new FastAPI surface per §4, Pydantic models, tests
- [ ] `feat/observability` — spans for schema link, each generation sample, each execution, scoring, routing; cost + latency per query
- [ ] `feat/docker-compose` — app + tracing backend, works from clean clone
- [ ] `ci/eval-regression-gate` — GitHub Actions runs a 50-question subset, fails if EX drops >3 points from recorded baseline
- [ ] Stretch: `feat/mcp-server`

**Done when:** `docker compose up` gives a working system, and a deliberately-broken prompt fails CI.

---

### Phase 5 — The proof  

Not optional polish. **This is the phase that gets you interviews.**

`docs/results-and-failure-taxonomy`:
- [ ] Full final eval run, all metrics captured
- [ ] **Failure taxonomy:** inspect ~50 failures, categorize (schema misinterpretation, aggregation errors, join-path errors, value-matching/dirty-data errors, ambiguous questions), report the distribution with an example of each
- [ ] README: architecture diagram, before/after eval table, per-technique delta table, calibration plot, routing precision/recall curve, cost/latency table, failure taxonomy, known limitations
- [ ] Migration note: what changed from QueryMind and why
- [ ] 2-minute demo recording
- [ ] Reproducibility check: fresh clone → documented steps → working system

**Done when:** a senior engineer could read the README in five minutes and know exactly what you built, how well it works, and where it breaks.

---

## 9. Definition of done

Ships when the repo contains:

1. A reproducible BIRD eval showing a measured baseline → improved delta
2. A working confidence-routing loop with a quantified reduction in incorrect query execution
3. Traces plus per-query cost and latency data
4. A Dockerized deployment and a CI regression gate
5. A README with an honest failure taxonomy
6. A clean commit history with no AI attribution trailers

A smaller project that is honestly evaluated beats a larger one that is not.

---

## 10. Target repo layout

```
querymind/  (→ sqlsentinel)
├── CLAUDE.md
├── README.md                  ← the deliverable; write last, write well
├── pyproject.toml
├── docker-compose.yml
├── Dockerfile
├── .github/workflows/eval-gate.yml
├── data/                      ← gitignored; BIRD databases
├── docs/
│   ├── migration-notes.md     ← what was kept from QueryMind and why
│   └── failure-taxonomy.md
├── src/sqlsentinel/
│   ├── llm.py                 ← LLMClient abstraction
│   ├── schema_linker.py       ← ported from QueryMind
│   ├── generator.py           ← ported prompts + k-sampling
│   ├── executor.py            ← dialect boundary: SQLite + Postgres
│   ├── confidence.py
│   ├── router.py
│   ├── tracing.py
│   ├── api.py                 ← rewritten FastAPI
│   └── eval/
│       ├── harness.py         ← wraps official BIRD script
│       └── features.py
├── app/review_ui.py           ← Streamlit
├── notebooks/                 ← calibration plots, failure analysis
├── results/                   ← committed metrics tables + figures
└── tests/
```

---

## 11. Working agreements

- **Measure before optimizing.** No technique lands without a before/after number logged to MLflow.
- **Cheap loop first.** Develop against a 50-question subset; run the full dev set only at phase boundaries. API cost and wall-clock time are real constraints.
- **Cache aggressively.** Cached LLM responses make development re-runs free.
- **Read-only always.** The eval executor opens databases read-only. No code path writes to a benchmark database.
- **Never train and evaluate the confidence model on the same questions.** The one mistake that would invalidate the whole project.
- **Commit results, not just code.** Metrics tables and figures live in `results/`, versioned.
- **Record honest numbers.** If a technique makes things worse, keep the row and say so. Negative results are credibility.
- **One branch, one concern.** If you're about to commit two unrelated things, you needed two branches.

---

## 12. Resume bullets (fill in real numbers at the end)

Draft once Phase 5 completes, replacing every placeholder with measured values:

- Rebuilt **QueryMind** into **SQLSentinel**, an evaluation-driven text-to-SQL agent (few-shot + schema linking + self-correction) benchmarked on the BIRD-SQL dev set (1,534 questions, 11 real databases), raising execution accuracy from **XX%** single-shot baseline to **YY%**, with every experiment tracked in MLflow.
- Built a calibrated confidence scorer combining self-consistency across k samples with **N** per-query features, routing the riskiest **ZZ%** of queries to human review and catching **AA%** of incorrect queries before execution, surfaced in a Streamlit review interface.
- Instrumented the agent for production with OpenTelemetry tracing, per-query cost and latency tracking, a rewritten FastAPI service, and a GitHub Actions regression gate blocking merges on accuracy drops, cutting eval feedback time from **BB** to **CC**.

---

## 13. Open questions to resolve during the build

- Which model as primary? Run Phase 1 against two and pick on cost-per-correct-answer, not raw accuracy.
- Does QueryMind's existing prompting beat a naive baseline on BIRD? Worth knowing and reporting either way.
- Is schema linking worth it on BIRD's larger databases, or does full-schema context win? Measure.
- Optimal k for self-consistency — 3, 5, or 7? Cost/signal tradeoff worth charting.
- Where to set the routing threshold? Present a curve rather than picking a number arbitrarily.
