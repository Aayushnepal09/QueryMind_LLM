# SQLSentinel — Progress Log

**Purpose:** session-continuity file. Read this first in any new Claude Code session, then `CLAUDE.md` (the spec / source of truth). Update the "Current state" and "Session log" sections at the end of every working session.

- Repo: https://github.com/Aayushnepal09/QueryMind_LLM
- Spec: `CLAUDE.md` (phases 0–5)
- Started: 2026-09-03

---

## Current state

**Phase:** 0 — in progress (`chore/project-scaffold`)
**Active branch:** `chore/project-scaffold` (off `sqlsentinel`, off `main`)
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
| BIRD dev set | not downloaded ⚠️ |
| LLM API keys | Gemini only (via Streamlit secrets); no Anthropic/OpenAI key confirmed ⚠️ |

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

- [ ] **Phase 0** — `chore/project-scaffold`, `feat/eval-harness`. Done when one command produces an accuracy number.
- [ ] **Phase 1** — `feat/llm-client`, `feat/baseline-agent`. Done when a baseline EX number exists with cost + latency.
- [ ] **Phase 2** — few-shot retrieval, schema linking, self-correction. Done at technique → EX → Δ → cost table.
- [ ] **Phase 3** — confidence scorer, risk router, review UI. Done at the money metric.
- [ ] **Phase 4** — API, observability, docker-compose, CI gate.
- [ ] **Phase 5** — results + failure taxonomy + README.

## Recorded numbers

| Run | Config | Subset | EX | Cost | Latency | MLflow run |
|---|---|---|---|---|---|---|
| — | — | — | — | — | — | — |

(Fill after every eval run. Never delete a row, including regressions.)

---

## Open decisions

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
- **Next action:** decide on the SSH key, then finish `chore/project-scaffold` (pyproject via uv, §10 directory layout, `.env.example`).
