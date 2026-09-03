# Migration notes: QueryMind → SQLSentinel

Audit performed 2026-09-03 against `main` @ `1e90f3c`, per CLAUDE.md §1 ("audit before rewriting").

## Inventory

| File | Lines | Decision | Reason |
|---|---|---|---|
| `streamlit_app.py` | 503 | **Split** | ~85% presentation (CSS, hero, bcrypt login). ~15% is the actual asset: the schema block, the generation prompt, the SQL extractor. |
| `utils.py` | 24 | **Keep, relocate** | Correct Postgres DSN builder — and unlike the copy inside `streamlit_app.py`, it URL-encodes the password. Moves behind the executor's dialect boundary. |
| `populate_db.py` | 220 | **Keep as-is** | SQLite→Postgres migration for the demo DB. Unrelated to BIRD. Repointed to `data/normalized.db`. |
| `test_render_database.py` | 27 | **Delete** | Not a test — a script with a bare top-level call that connects to prod on import. Replaced by real tests. |
| `generate_password.py` | 8 | **Keep** | Bcrypt hash helper for the retained demo login. |
| `normalized.db` | 15 MB | **Untracked** | Moved to `data/`, purged from history. |
| `.idea/` | — | **Untracked** | IDE-local config. |
| `README.md` | — | **Rewrite in Phase 5** | Stale: claims SQLite and "Gemini Pro"; code uses Postgres and `gemini-2.5-flash`. |

## What was actually preserved

Three things, all extracted from `streamlit_app.py`:

1. **`DATABASE_SCHEMA`** (the 60-line schema string) → seeds `schema_linker.py`.
   *Caveat:* hardcoded for one fixed 6-table schema. BIRD has 11 databases, so this becomes a **template for introspection output**, not the output itself. The join-path hints and the "revenue = unit_price × quantity" note are the genuinely reusable part — they show the shape of context that helps.

2. **The generation prompt** in `generate_sql_with_gpt` (8 numbered requirements) → seeds `generator.py`.
   **This is the Phase 1 baseline.** It is what gets benchmarked, and its number is the "before".

3. **`extract_sql_from_response`** → seeds SQL extraction.
   *Caveat:* it only strips ` ```sql ` fences. The Qwen smoke test on 2026-09-03 produced a prose preamble ("To calculate the total revenue…") followed by a fenced block followed by a numbered explanation. The current regex would return all of it. **Needs hardening in Phase 1.**

## What the spec expected but does not exist

CLAUDE.md §1 was written expecting more prior art than is actually present. Corrections:

| Spec assumed | Reality |
|---|---|
| A FastAPI layer to rewrite | None exists. `feat/api-rewrite` is a greenfield build. |
| Chain-of-thought prompting | The prompt is zero-shot, single-shot. No CoT. |
| Few-shot exemplars | None. Phase 2's `feat/few-shot-retrieval` starts from nothing. |
| Schema introspection logic | None — the schema is a hardcoded string literal. |

Consequence: the Phase 1 baseline is a **naive zero-shot prompt**, which is an honest and defensible starting point. It also means the Phase 2 deltas measure real technique contribution rather than re-tuning of prior work.

## Defects found in the original (documented, not fixed on `main`)

1. **Gemini key read from `st.secrets["OPENAI_API_KEY"]`** — misnamed; no OpenAI is involved.
2. **No read-only guard.** Generated SQL executes directly against the production Postgres instance. A generated `DROP` or `DELETE` would run. SQLSentinel's executor is read-only with a hard timeout (spec §4).
3. **Password not URL-encoded** in `streamlit_app.py`'s local `get_db_url`, though `utils.py` does it correctly. Silent breakage on special characters.
4. **The "92% accuracy" claim has no test set behind it** anywhere in the repo — no fixtures, no gold queries, no harness. This is the specific problem SQLSentinel exists to fix.
5. **SSH keypair committed** in `f90dfdc` (`aayush`, `aayush.pub`), deleted in later commits but live in history until the 2026-09-03 `git filter-repo` purge. Key should be treated as compromised and rotated.
