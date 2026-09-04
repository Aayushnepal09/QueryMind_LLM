# Migration notes: QueryMind → SQLSentinel

Audit of QueryMind as it stood before the rebuild, per spec §1 ("audit before rewriting").
The original is preserved at tag `v0-querymind`.

> **Where the original lives.** Every file described below is preserved at the
> tag **`v0-querymind`** and the branch **`querymind-original`**, untouched.
> They were removed from the working branch once their reusable parts had been
> ported, because carrying a superseded application alongside its replacement
> makes the repository harder to read, not easier to verify.
>
> spec §1 asks that the "before" state be preserved as evidence. It is —
> just not on `main`, which now carries the rebuild so that anyone landing on
> the repository sees the finished work rather than the thing it replaced.
>
> ```bash
> git diff v0-querymind main     # the whole transformation
> git checkout v0-querymind      # the original, as it was
> ```

## Inventory

| File | Lines | Decision | Reason |
|---|---|---|---|
| `streamlit_app.py` | 503 | **Split, then left on `main`** | ~85% presentation (CSS, hero, bcrypt login). ~15% was the actual asset — the schema block, the generation prompt, the SQL extractor — all three ported. Superseded by `app/review_ui.py` and `src/sqlsentinel/api.py`. |
| `utils.py` | 24 | **Ported** | Its DSN builder is now `executor.postgres_dsn_from_env()`, behind the dialect boundary, with tests covering the URL-encoding that `streamlit_app.py`'s copy got wrong. |
| `populate_db.py` | 220 | **Left on `main`** | One-off SQLite→Postgres migration for the demo database. Unrelated to BIRD, and not part of the evaluated system. |
| `test_render_database.py` | 27 | **Delete** | Not a test — a script with a bare top-level call that connects to prod on import. Replaced by real tests. |
| `generate_password.py` | 8 | **Left on `main`** | Bcrypt helper for the demo login, which `sqlsentinel` does not have (spec §3 lists auth as a non-goal). |
| `normalized.db` | 15 MB | **Untracked** | Moved to `data/`, purged from history. |
| `.idea/` | — | **Untracked** | IDE-local config. |
| `README.md` | — | **Rewritten** | Stale: claims SQLite and "Gemini Pro"; code uses Postgres and `gemini-2.5-flash`. |

## What was actually preserved

Three things, all extracted from `streamlit_app.py`:

1. **`DATABASE_SCHEMA`** (the 60-line schema string) → seeds `schema_linker.py`.
   *Caveat:* hardcoded for one fixed 6-table schema. BIRD has 11 databases, so this becomes a **template for introspection output**, not the output itself. The join-path hints and the "revenue = unit_price × quantity" note are the genuinely reusable part — they show the shape of context that helps.

2. **The generation prompt** in `generate_sql_with_gpt` (8 numbered requirements) → seeds `generator.py`.
   **This is the measured baseline.** It is what gets benchmarked, and its number is the "before".

3. **`extract_sql_from_response`** → seeds SQL extraction.
   *Caveat:* it only strips ` ```sql ` fences. A smoke test produced a prose preamble ("To calculate the total revenue…") followed by a fenced block followed by a numbered explanation. The current regex would return all of it, so it was hardened.

## What the spec expected but does not exist

spec §1 was written expecting more prior art than is actually present. Corrections:

| Spec assumed | Reality |
|---|---|
| A FastAPI layer to rewrite | None exists. `feat/api-rewrite` is a greenfield build. |
| Chain-of-thought prompting | The prompt is zero-shot, single-shot. No CoT. |
| Few-shot exemplars | None. Retrieval was built from nothing. |
| Schema introspection logic | None — the schema is a hardcoded string literal. |

Consequence: the measured baseline is a **naive zero-shot prompt**, which is an honest and defensible starting point, and the reported deltas measure real technique contribution rather than re-tuning of prior work.

## Defects found in the original (documented, not fixed on `main`)

1. **Gemini key read from `st.secrets["OPENAI_API_KEY"]`** — misnamed; no OpenAI is involved.
2. **No read-only guard.** Generated SQL executes directly against the production Postgres instance. A generated `DROP` or `DELETE` would run. SQLSentinel's executor is read-only with a hard timeout (spec §4).
3. **Password not URL-encoded** in `streamlit_app.py`'s local `get_db_url`, though `utils.py` does it correctly. Silent breakage on special characters.
4. **The "92% accuracy" claim has no test set behind it** anywhere in the repo — no fixtures, no gold queries, no harness. This is the specific problem SQLSentinel exists to fix.
5. **SSH keypair committed** in `f90dfdc` (`aayush`, `aayush.pub`), deleted in later commits but live in history until a `git filter-repo` purge. The key was treated as compromised and removed.


## What `sqlsentinel` does not carry

Removed after the audit, all preserved at `v0-querymind`:

| Removed | Why |
|---|---|
| `streamlit_app.py`, `populate_db.py`, `generate_password.py` | The original demo application. Its reusable parts are ported; the rest is superseded. |
| `utils.py` | Ported to `executor.postgres_dsn_from_env()`. |
| `requirements.txt` | Superseded by `pyproject.toml`. Two dependency manifests that can disagree is worse than one. |
| `.devcontainer/devcontainer.json` | Actively wrong: it installed from `requirements.txt` and auto-launched `streamlit run streamlit_app.py`. |
| `third_party/evaluation_ex.py` | Downloaded while locating BIRD's scorer; the harness wraps `evaluation.py`. Unused code that looks authoritative is a trap. |
