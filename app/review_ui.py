"""Streamlit human-review queue (spec §3).

Shows the queries the router sent to REVIEW and records what a human decided.

**Designed to be usable by a non-engineer.** A review queue that only an SQL
reader can action is not a human-in-the-loop system, it is a second engineering
queue -- and it excludes the analysts and domain experts most able to recognise
a wrong answer in their own data. So the default view leads with:

  1. the question, in the words it was asked
  2. what the system will actually return -- the result table, which is the
     evidence a domain expert can judge without reading any SQL
  3. a plain-English description of what the query does
  4. why it was flagged, in words rather than as a number

The SQL, an editor, and the full expert controls are all still present, one
click away. Both audiences act on the same queue and their decisions are logged
identically -- which is what makes the human-in-the-loop claim real rather than
decorative.

Deliberately plain visually. spec §3 lists a polished custom
front-end as a non-goal; the artifact is the evaluation, not the interface.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st
import theme

from sqlsentinel.executor import bird_executor
from sqlsentinel.explain import describe_confidence, explain

REPO_ROOT = Path(__file__).resolve().parents[1]
QUEUE_FILE = REPO_ROOT / "results" / "review_queue.json"
DECISIONS_DB = REPO_ROOT / "results" / "review_decisions.db"
DB_ROOT = REPO_ROOT / "data" / "bird" / "dev_20240627" / "dev_databases"

st.set_page_config(
    page_title="SQLSentinel Review",
    page_icon="🛡️",
    layout="centered",
    initial_sidebar_state="collapsed",
)


# ---------------------------------------------------------------- storage


def init_db() -> sqlite3.Connection:
    DECISIONS_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DECISIONS_DB, check_same_thread=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS decisions ("
        " question_id INTEGER PRIMARY KEY, action TEXT NOT NULL,"
        " original_sql TEXT, final_sql TEXT, confidence REAL,"
        " reviewer_note TEXT, reviewer_mode TEXT, decided_at REAL NOT NULL)"
    )
    # older databases predate reviewer_mode
    cols = {r[1] for r in conn.execute("PRAGMA table_info(decisions)")}
    if "reviewer_mode" not in cols:
        conn.execute("ALTER TABLE decisions ADD COLUMN reviewer_mode TEXT")
    conn.commit()
    return conn


def record(conn, qid, action, original, final, confidence, note, mode) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO decisions VALUES (?,?,?,?,?,?,?,?)",
        (qid, action, original, final, confidence, note, mode, time.time()),
    )
    conn.commit()


def decided_ids(conn) -> set[int]:
    return {r[0] for r in conn.execute("SELECT question_id FROM decisions")}


@st.cache_data
def load_queue() -> list[dict]:
    if not QUEUE_FILE.exists():
        return []
    return json.loads(QUEUE_FILE.read_text(encoding="utf-8"))


def _unique_columns(columns: list[str]) -> list[str]:
    """Disambiguate repeated column names.

    A join that selects same-named columns from two tables returns duplicates --
    `SELECT T1.element, T2.element FROM ...` yields two columns called
    `element`. Arrow rejects that, so st.dataframe raises ValueError and the
    whole page dies on an otherwise valid query. Suffix the repeats instead.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in columns:
        label = name or "column"
        if label in seen:
            seen[label] += 1
            out.append(f"{label} ({seen[label]})")
        else:
            seen[label] = 1
            out.append(label)
    return out


@st.cache_data(show_spinner=False)
def run_preview(db_id: str, sql: str):
    """Execute read-only for the preview. Cached so switching items is instant."""
    res = bird_executor(DB_ROOT, db_id).execute(sql)
    if not res.ok:
        return None, res.error
    cols = _unique_columns(res.columns) if res.columns else None
    return pd.DataFrame(res.rows, columns=cols), None


# ---------------------------------------------------------------- views


def render_answer(item: dict, sql: str) -> None:
    """The result table. For a non-expert this is the actual evidence."""
    if not DB_ROOT.exists():
        st.info("Benchmark databases not present, so no preview can be shown.")
        return
    try:
        df, err = run_preview(item["db_id"], sql)
    except Exception as e:
        st.warning(f"Could not run the query: {e}")
        return

    if err:
        st.error(f"This query does not run: {err}")
        st.caption("A query that fails is always worth rejecting or fixing.")
        return

    if df is None or df.empty:
        st.warning(
            "This query runs but returns **no data**. That often means it filters "
            "on a value that does not appear in the database — check the spelling "
            "and capitalisation of any names below."
        )
        return

    st.caption(f"The system would return {len(df):,} row(s):")
    st.dataframe(df.head(25), use_container_width=True)
    if len(df) > 25:
        st.caption(f"Showing the first 25 of {len(df):,}.")


def render_plain(sql: str) -> None:
    exp = explain(sql)
    rows = "".join(f'<div class="plain-item">{d}</div>' for d in exp.details)
    st.markdown(
        f'<div class="plain"><div class="plain-lead">{exp.summary}</div>{rows}</div>',
        unsafe_allow_html=True,
    )
    for w in exp.warnings:
        st.warning(w, icon="⚠️")


def render_expert(item: dict, key: str) -> str:
    """The engineer's view: raw SQL, editable. Returns the (possibly edited) SQL."""
    st.caption("Edit the SQL if you can correct it, then approve.")
    return st.text_area(
        "SQL", value=item.get("sql", ""), height=160, key=key, label_visibility="collapsed"
    )


# ---------------------------------------------------------------- app


def main() -> None:
    conn = init_db()
    queue = load_queue()

    theme.inject()
    theme.header(
        "SQLSentinel",
        "Questions the system was unsure about. Each needs a person to confirm the answer.",
    )

    if not queue:
        st.info(
            "No review queue found. Generate one with:\n\n"
            "```\nuv run python -m sqlsentinel.eval --split dev_50 "
            "--predictor agent --k 3 --write-queue\n```"
        )
        return

    done = decided_ids(conn)
    pending = [i for i in queue if i["question_id"] not in done]

    pct = 100 * len(done) / len(queue)
    high = sum(1 for i in pending if i.get("confidence", 0) < 0.34)
    theme.stats(
        [
            ("in queue", str(len(queue))),
            ("reviewed", str(len(done))),
            ("pending", str(len(pending))),
            ("high risk", str(high)),
        ],
        progress=pct,
    )

    # Controls sit inline rather than in a sidebar. There are only four of them,
    # and a drawer that has to be opened to discover the mode switch hides the
    # single most important thing about this queue -- that two different
    # audiences can work it.
    st.markdown('<div class="label">Reviewing as</div>', unsafe_allow_html=True)
    mode = st.segmented_control(
        "Reviewing as",
        ["Anyone (plain English)", "Engineer (SQL)"],
        default="Anyone (plain English)",
        label_visibility="collapsed",
    )
    expert = bool(mode) and mode.startswith("Engineer")

    f1, f2, f3 = st.columns([3, 2, 2], vertical_alignment="bottom")
    with f1:
        max_conf = st.slider("Show confidence at or below", 0.0, 1.0, 1.0, 0.05)
    with f2:
        show_done = st.checkbox("Include reviewed", value=False)
    with f3:
        df = pd.read_sql_query("SELECT * FROM decisions", conn)
        st.download_button(
            "Export decisions",
            df.to_csv(index=False),
            "review_decisions.csv",
            "text/csv",
            use_container_width=True,
            disabled=df.empty,
        )

    with st.expander("How does a query get here?"):
        st.markdown(
            "A query reaches this queue when the system's confidence falls below "
            "the routing threshold, or when a safety rule fires regardless of "
            "confidence — it is not a read-only query, or it returns an unusually "
            "large amount of data."
        )

    st.divider()

    items = [i for i in (queue if show_done else pending) if i.get("confidence", 0) <= max_conf]
    if not items:
        st.success("Queue clear — nothing pending.")
        return

    for item in items:
        qid = item["question_id"]
        conf = float(item.get("confidence", 0.0))
        _, _, dot = theme.severity(conf)
        title = item["question"] if len(item["question"]) <= 96 else item["question"][:96] + "…"

        with st.expander(f"{dot}  {title}", expanded=False):
            st.markdown(
                f'{theme.pill(conf)}<div class="question">{item["question"]}</div>',
                unsafe_allow_html=True,
            )
            if item.get("evidence"):
                st.markdown(
                    f'<div class="context">{item["evidence"]}</div>', unsafe_allow_html=True
                )

            st.info(describe_confidence(conf, int(item.get("n_candidates", 1))), icon="🎯")
            if item.get("reasons"):
                st.caption("Flagged because: " + "; ".join(item["reasons"]))

            if expert:
                st.markdown('<div class="label">Query</div>', unsafe_allow_html=True)
                sql = render_expert(item, key=f"sql_{qid}")
                st.markdown('<div class="label">Result</div>', unsafe_allow_html=True)
                render_answer(item, sql)
                with st.expander("Plain-English description"):
                    render_plain(sql)
            else:
                st.markdown(
                    '<div class="label">The answer the system found</div>',
                    unsafe_allow_html=True,
                )
                render_answer(item, item.get("sql", ""))
                st.markdown('<div class="label">What this query does</div>', unsafe_allow_html=True)
                render_plain(item.get("sql", ""))
                with st.expander("Show the database query (for engineers)"):
                    st.code(item.get("sql", ""), language="sql")
                sql = item.get("sql", "")

            st.markdown('<div class="label">Your decision</div>', unsafe_allow_html=True)
            note = st.text_input(
                "Note (optional) — if this is wrong, what should it have shown?",
                key=f"note_{qid}",
            )

            b1, b2, b3 = st.columns(3)
            mode_tag = "expert" if expert else "plain"
            ok_label = "✅ Approve" if expert else "✅ This looks right"
            no_label = "❌ Reject" if expert else "❌ This looks wrong"

            if b1.button(ok_label, key=f"ok_{qid}", type="primary", use_container_width=True):
                action = "edit" if sql.strip() != item.get("sql", "").strip() else "approve"
                record(conn, qid, action, item.get("sql", ""), sql, conf, note, mode_tag)
                st.rerun()
            if b2.button(no_label, key=f"no_{qid}", use_container_width=True):
                record(conn, qid, "reject", item.get("sql", ""), "", conf, note, mode_tag)
                st.rerun()
            if b3.button("🤷 Not sure", key=f"unsure_{qid}", use_container_width=True):
                record(conn, qid, "unsure", item.get("sql", ""), "", conf, note, mode_tag)
                st.rerun()


if __name__ == "__main__":
    main()
