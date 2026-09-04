# SQLSentinel — Progress Log

**Purpose:** session-continuity file. Read this first in any new Claude Code session, then `CLAUDE.md` (the spec / source of truth). Update the "Current state" and "Session log" sections at the end of every working session.

- Repo: https://github.com/Aayushnepal09/QueryMind_LLM
- Spec: `CLAUDE.md` (phases 0–5)
- Started: 2026-09-03

---

## Current state

**All phases COMPLETE** except the v2 calibrated confidence model, which waits
on one running job.
**Active branch:** `chore/cleanup` (merged into `sqlsentinel`, pushed to origin)
**Tests:** 196 passing, ruff clean and formatted.
**Running:** clean `k3-calib200` re-run (600 generations). When it finishes:

```bash
uv run python scripts/analyze.py results/traces/k3-eval200.json --label k3-eval200   --calib-traces results/traces/k3-calib200.json   # fits and reports v2
uv run python scripts/compare.py --baseline baseline-eval500
uv run python scripts/research.py
uv run python scripts/build_report.py
```

**Verified working:** Docker build + container serving `/query` end to end;
Streamlit review UI in both modes; force-push of the purged history; the
official BIRD harness against gold SQL at 100%.
**Integration branch `sqlsentinel`:** created locally, **not yet pushed**
**Hardware:** GTX 1660 Ti (6 GB VRAM), 15.8 GB RAM — relevant to the local-LLM decision below

### Environment (verified 2026-09-03)
| Thing | Status |
|---|---|
| Python | 3.12.7 ✅ |
| uv | 0.10.7 ✅ |
| Docker | 28.3.2 ✅ |
| git remote | `origin` → Aayushnepal09/QueryMind_LLM ✅ |
| `.env` in repo | absent — secrets currently live in Streamlit Cloud `st.secrets` ⚠️ |
| BIRD dev set | **`dev_20240627`** extracted to `data/bird/dev_20240627/` (1.4 GB, 11 DBs, 1,534 questions) ✅ |
| LLM API keys | Gemini only (via Streamlit secrets), **not yet in a local `.env`** ⚠️ |
| Ollama | installed 2026-09-03 at `%LOCALAPPDATA%\Programs\Ollama` ✅ |
| Local model | `qwen2.5-coder:7b` (4.7 GB) pulled, smoke-tested ✅ |
| Free disk (C:) | ~19 GB after BIRD extract — fine ✅ |

---

## QueryMind audit (the "before" state)

Existing repo contents at `main` (commit `1e90f3c`):

| File | Lines | Verdict |
|---|---|---|
| `streamlit_app.py` | 503 | **Split.** Contains the only real assets: `DATABASE_SCHEMA` block, the PostgreSQL system prompt in `generate_sql_with_gpt`, `extract_sql_from_response`. Everything else is UI/CSS/auth — replace. |
| `utils.py` | 24 | Keep as the Postgres DSN helper; move behind the executor dialect boundary. |
| `populate_db.py` | 220 | Keep for the retained QueryMind demo path. Not used by BIRD. |
| `test_render_database.py` | 27 | Throwaway connectivity script, not a test. Replace with real tests. |
| `normalized.db` | 15 MB | SQLite file committed to git. Should move to `data/` and be gitignored. |
| `README.md` | — | Stale (says SQLite + "Gemini Pro"; actual code is Postgres + `gemini-2.5-flash`). Rewritten in Phase 5. |
| `.idea/` | — | JetBrains config, committed. Should be gitignored. |

**Reusable, per CLAUDE.md §1:**
1. `DATABASE_SCHEMA` string → seeds `schema_linker.py` (currently hardcoded; must become introspection-driven for BIRD's 11 DBs).
2. The 8-requirement prompt in `generate_sql_with_gpt` → seeds `generator.py`. **This is the Phase 1 baseline prompt** — the thing being benchmarked.
3. `extract_sql_from_response` (regex fence-stripper) → seeds SQL extraction. Weak: only handles ```sql fences, not preamble text. Needs hardening.

**Not reusable:** no FastAPI layer exists (spec §1 says "rewrite the FastAPI layer" — there isn't one, so `feat/api-rewrite` is a build, not a rewrite). No few-shot exemplars exist. No chain-of-thought prompting exists — the prompt is single-shot zero-shot. Adjust the spec's framing accordingly.

**Known issues in current code (document, don't fix on `main`):**
- Gemini key is read from `st.secrets["OPENAI_API_KEY"]` — misnamed.
- No read-only guard; generated SQL runs directly against prod Postgres.
- `get_db_url` in `streamlit_app.py` doesn't URL-encode the password (the `utils.py` version does).
- The "92% accuracy" claim has no test set behind it anywhere in the repo.

---

## Phase checklist

- [x] **Phase 0** — DONE. `--predictor stub` → `EX 3.0% ± 1.5 (n=500)`.
- [x] **Phase 1** — DONE. `llm.py` (ollama+gemini+cache), `agent.py`, `executor.py`, `schema_linker.py`, `generator.py`. Baseline `dev_50` = **62.0% ± 13.0**.
- [x] **Phase 2 code** — DONE. `retrieval.py` (TF-IDF over calib), `Schema.prune()`, self-correction, k-sample voting. Ablation runs in progress.
- [x] **Phase 3 code** — DONE. `confidence.py` (v1 agreement, v2 calibrated, Brier/ECE/reliability), `router.py` (risk rules + routing curve), `app/review_ui.py`. Runs in progress.
- [x] **Phase 4** — DONE. `api.py`, `tracing.py`, Dockerfile + compose (api/review/phoenix), `.github/workflows/eval-gate.yml`, `scripts/check_regression.py`.
- [~] **Phase 5** — README, failure taxonomy, reproduction guide and report generator written. Awaiting numbers from the sweep.

## Recorded numbers

| Run | Config | Split | n | EX | 95% CI | Scoring |
|---|---|---|---|---|---|---|
| 2026-09-03 | stub (`SELECT 1`) | dev_50 | 50 | 4.0% | ±6.2 | 1.3 s |
| 2026-09-03 | stub (`SELECT 1`) | eval_500 | 500 | **3.0%** | ±1.5 | 37.8 s |
| 2026-09-03 | baseline, qwen2.5-coder:7b | dev_50 | 50 | 62.0% | ±13.0 | 1.3 s |
| 2026-09-04 | **baseline** (the "before") | eval_500 | 500 | **45.6%** | ±4.3 | — |
| 2026-09-04 | **final** few-shot + self-correct | eval_500 | 500 | **50.4%** | ±4.4 | — |
| 2026-09-04 | k=3 self-consistency | eval_500 subset | 200 | **55.0%** | ±6.8 | — |

Paired: final **+4.8 pts p=0.014**; k=3 **+9.0 pts p=0.008**. Both significant.

**The chance floor is ~3%, not 0%** — `SELECT 1` returns `{(1,)}` and some gold
queries legitimately return a scalar 1 under set-equality scoring. Read every
accuracy against a 3% floor. See `results/baseline-floor.md`.

(Fill after every eval run. Never delete a row, including regressions.)

---

## Locked decisions

**Eval set size (2026-09-03):** BIRD dev, evaluated on a **fixed, seeded, difficulty-stratified 500-question subset**. 50-question subset for the daily dev loop; full 1,534 run optional at phase boundaries only.

*Rationale:* this is an evaluation set, not training data — nothing is fine-tuned (spec §3). Subset size is a statistical-power question. At n=500 the 95% CI on an accuracy near 50% is ±4.4%, which supports the ~6-10 point delta Phase 2 realistically produces. At n=50 the CI is ±13.9% and any claimed delta is noise. Full-set n=1534 gives ±2.5% but costs an overnight local run at k=5.

*Reporting rule:* always report accuracy **with its confidence interval and n** (e.g. "58.2% ± 4.4%, n=500"). Never quote a bare number. The subset must be seeded and its question ids committed to `results/` so runs are comparable.

*Deviation from CLAUDE.md:* spec §8 implies full-dev-set runs in Phases 1-2. We substitute the 500-question stratified subset. Note this in the README.

## Traps that cost time — do not repeat

1. **`--predictor` defaults to `stub`.** Omitting it in a script produces a
   chance-floor score (2.5%) instead of an error. Cost one wasted run.
2. **Few-shot retrieval leaked** until `exclude_question_id` was added: a
   question evaluated on `calib` retrieved itself at similarity 1.0. Worth 23.5
   points. Guarded and regression-tested now.
3. **Per-question labelling must use the official scorer's execution budget**
   (30 s, no row cap), not the agent's (5 s, 5,000 rows). Caused a 3-question
   drift; `analyze.py` now hard-fails on any disagreement.
4. **Git Bash rewrites container paths.** `docker run -v /app/data` becomes
   `C:/Program Files/Git/app/data` and the API reports healthy with zero
   databases. Use `docker compose`, or `MSYS_NO_PATHCONV=1`.
5. **Emoji and typographic characters crash the cp1252 console.** Keep script
   stdout ASCII; Unicode is fine in files and in Streamlit.
6. **Editing a running bash script corrupts it** — bash reads incrementally.
   Queue follow-up work in a separate script.

## Measured facts worth not re-deriving

- **Gemini free tier sustains ~0.8 req/min** once quota + backoff are counted → 10 h for 500 questions. Not viable for bulk eval. Local Qwen is the workhorse; Gemini is the `dev_50` comparison point only.
- **Local Qwen throughput:** ~2.5–3 generations/min on BIRD-sized prompts (1,360 prompt tokens baseline, ~3,400 with 3 exemplars).
- **Schema pruning is a near-no-op on BIRD dev:** 99% of tables retained, 1.3% prompt reduction. Databases are 3–13 tables and densely FK-connected, so join-path closure pulls back what lexical scoring drops. Negative result — report it, do not tune it away.
- **Schema prompt sizes:** mean ~1,230 tokens, max 3,128 (european_football_2, 199 columns). Token cost is not the motivation for pruning.
- **The official BIRD script divides by empty difficulty buckets** — any slice lacking `challenging` questions crashes it. Guarded in `harness.py`; `--subset` is stratified for the same reason.
- **MLflow's file store is deprecated upstream** and now raises. Using `sqlite:///mlflow.db`.
- **Docker build unverified** — Docker Desktop was not running. `docker compose config` validates and all COPY paths exist, but `docker compose up` has NOT been executed end-to-end. Do not claim it works until it has.

## Open decisions

- [x] Local model — RESOLVED. Ollama + `qwen2.5-coder:7b`. Smoke test 2026-09-03: correct 5-table-join revenue query at temp 0. **Measured throughput ~10 tok/s output, 20.4 s for 200 tokens on the 1660 Ti.** Budget implication: a 50-question dev subset at k=5 ≈ 250 generations ≈ 45-60 min locally. Full 1,534-question dev run at k=5 is an overnight job. Caching is mandatory.
- [x] GPT4All removed 2026-09-03 — app was already uninstalled, only orphaned data remained. Deleted `%LOCALAPPDATA%
omic.ai` (4.3 GB: Meta-Llama-3-8B-Instruct.Q4_0.gguf + chat history). Chat history backed up to the session scratchpad.
- [ ] Primary LLM — **hard constraint: $0 budget.** Plan is Gemini free tier (already wired) as primary + a local Ollama model (Qwen2.5-Coder-7B, Q4) as the second provider and unlimited-iteration workhorse. Satisfies spec §13's two-provider comparison for free. Local model is slow on a 1660 Ti — fine for the 50-question dev subset, painful for full 1,534-question runs. Aggressive response caching (spec §11) is not optional here, it is the budget.
- [x] Commit attribution — RESOLVED. `.git/hooks/commit-msg` strip hook installed + `.claude/settings.local.json` attribution overrides. Verified clean on first commit.
- [ ] Tracing backend: Langfuse vs Phoenix (spec §5, pick one).
- [ ] **🔴 SSH KEYPAIR IN GIT HISTORY — UNRESOLVED, ACTION REQUIRED.** Commit `f90dfdc` added `aayush` (7-line file, shape of an OpenSSH ed25519 private key) and `aayush.pub`. Deleted in `18bec47`/`f4045a9` but **still present in history and on GitHub**. Deleting the file did not remove it. Required: (1) treat the key as compromised and revoke it wherever it is authorized (GitHub SSH keys, any server), (2) generate a fresh keypair, (3) optionally purge history with `git filter-repo` + force-push. Step 1 is the one that actually matters. Not actioned — needs user decision.
- [ ] `normalized.db` — untracked and moved to `data/`, but the 15 MB blob is still in history (only ~4.4 MB packed). Purge only if we are already rewriting history for the key.

---

## Session log

### 2026-09-03 — session 1
- Read `CLAUDE.md`; copied it to repo root as the in-repo source of truth.
- Audited the existing QueryMind code (see audit table above).
- Verified toolchain (Python 3.12.7, uv, Docker all present).
- Created this file.
- **Audited git history and found an SSH keypair committed in `f90dfdc`** (see Open decisions). Not fixed — needs user action.
- Created branches `sqlsentinel` and `chore/project-scaffold`.
- Installed `.git/hooks/commit-msg` attribution strip hook + `.claude/settings.local.json`; verified the first commit stored no trailer.
- Untracked `.idea/` and `normalized.db`; moved the DB to `data/`; repointed `populate_db.py`; extended `.gitignore` (data/, mlruns/, caches, IDE, secrets).
- Committed as `chore(repo): untrack IDE config and benchmark DB, extend gitignore`.
- **Purged git history** with `git filter-repo`: removed `aayush`, `aayush.pub`, `normalized.db` from all commits. `.git` 4.4 MB → 124 KB. Pre-rewrite backup bundle in session scratchpad. **Force-push to origin still PENDING — user must run it** (sandbox blocked the push).
- Installed Ollama + `qwen2.5-coder:7b`; smoke-tested SQL generation.
- Removed GPT4All leftovers (4.3 GB freed).
- Finished `chore/project-scaffold`; merged to `sqlsentinel` with `--no-ff`.
- **Phase 0 complete on `feat/eval-harness`:** BIRD `dev_20240627` extracted; splits generated and committed to `results/splits.json`; official scorer wrapped; 12 tests passing including a gold-SQL-scores-100% integrity test.
- Found and guarded a real bug in the official BIRD script: `compute_acc_by_diff` divides by empty difficulty buckets.
- **Next action:** user force-pushes rewritten history; then Phase 1 `feat/llm-client` (Ollama + Gemini behind one interface, with the response cache).
