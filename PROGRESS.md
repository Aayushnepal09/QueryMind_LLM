# SQLSentinel — Progress Log

**Purpose:** session-continuity file. Read this first in any new Claude Code session, then `CLAUDE.md` (the spec / source of truth). Update the "Current state" and "Session log" sections at the end of every working session.

- Repo: https://github.com/Aayushnepal09/QueryMind_LLM
- Spec: `CLAUDE.md` (phases 0–5)
- Started: 2026-09-03

---

## Current state

**Phase:** 0 — **COMPLETE** ✅ (gate met: one command produces an accuracy number)
**Active branch:** `feat/eval-harness` (off `sqlsentinel`)
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

- [x] **Phase 0** — DONE 2026-09-03. `python -m sqlsentinel.eval --split eval_500 --predictor stub` → `EX 3.0% ± 1.5 (n=500)`.
- [ ] **Phase 1** — `feat/llm-client`, `feat/baseline-agent`. Done when a baseline EX number exists with cost + latency.
- [ ] **Phase 2** — few-shot retrieval, schema linking, self-correction. Done at technique → EX → Δ → cost table.
- [ ] **Phase 3** — confidence scorer, risk router, review UI. Done at the money metric.
- [ ] **Phase 4** — API, observability, docker-compose, CI gate.
- [ ] **Phase 5** — results + failure taxonomy + README.

## Recorded numbers

| Run | Config | Split | n | EX | 95% CI | Scoring |
|---|---|---|---|---|---|---|
| 2026-09-03 | stub (`SELECT 1`) | dev_50 | 50 | 4.0% | ±6.2 | 1.3 s |
| 2026-09-03 | stub (`SELECT 1`) | eval_500 | 500 | **3.0%** | ±1.5 | 37.8 s |

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
