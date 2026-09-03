# SQLSentinel — Progress Log

**Purpose:** session-continuity file. Read this first in any new Claude Code session, then `CLAUDE.md` (the spec / source of truth). Update the "Current state" and "Session log" sections at the end of every working session.

- Repo: https://github.com/Aayushnepal09/QueryMind_LLM
- Spec: `CLAUDE.md` (phases 0–5)
- Started: 2026-09-03

---

## Current state

**Phase:** 0 — not started (audit + eval harness)
**Active branch:** `main` (original QueryMind, to remain untouched)
**Integration branch `sqlsentinel`:** not created yet

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

- [ ] Primary LLM: Gemini (already wired, key exists) vs Claude vs GPT-4o. Spec §13 says decide on cost-per-correct-answer after Phase 1 against two providers.
- [ ] Commit attribution: `CLAUDE.md` §2 forbids AI trailers; this environment's harness injects `Co-Authored-By: Claude Opus 5`. Needs a `.git/hooks/commit-msg` strip hook to enforce the spec. **Unresolved — confirm with user.**
- [ ] Tracing backend: Langfuse vs Phoenix (spec §5, pick one).
- [ ] `normalized.db` — keep in git history or purge?

---

## Session log

### 2026-09-03 — session 1
- Read `CLAUDE.md`; copied it to repo root as the in-repo source of truth.
- Audited the existing QueryMind code (see audit table above).
- Verified toolchain (Python 3.12.7, uv, Docker all present).
- Created this file.
- **No code written, no branches created.** Next action: Phase 0 `chore/project-scaffold`.
