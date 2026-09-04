"""Streamlit human-review queue (CLAUDE.md section 8, `feat/review-ui`).

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

Deliberately plain visually. CLAUDE.md section 3 lists a polished custom
front-end as a non-goal; the artifact is the evaluation, not the interface.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from sqlsentinel.executor import bird_executor
from sqlsentinel.explain import describe_confidence, explain

REPO_ROOT = Path(__file__).resolve().parents[1]
QUEUE_FILE = REPO_ROOT / "results" / "review_queue.json"
DECISIONS_DB = REPO_ROOT / "results" / "review_decisions.db"
DB_ROOT = REPO_ROOT / "data" / "bird" / "dev_20240627" / "dev_databases"

st.set_page_config(page_title="SQLSentinel Review", page_icon="🛡️", layout="wide")


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


@st.cache_data(show_spinner=False)
def run_preview(db_id: str, sql: str):
    """Execute read-only for the preview. Cached so switching items is instant."""
    res = bird_executor(DB_ROOT, db_id).execute(sql)
    if not res.ok:
        return None, res.error
    return pd.DataFrame(res.rows, columns=res.columns or None), None


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


def render_plain(item: dict, sql: str) -> None:
    exp = explain(sql)
    st.markdown(f"**In plain terms:** {exp.summary}")
    for d in exp.details:
        st.markdown(f"&nbsp;&nbsp;&nbsp;• {d}", unsafe_allow_html=True)
    for w in exp.warnings:
        st.warning(w)


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

    st.title("🛡️ SQLSentinel — Review Queue")
    st.caption(
        "These questions were answered with low confidence, or triggered a safety "
        "rule. Each one needs a person to confirm the answer before it is used."
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

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("In queue", len(queue))
    c2.metric("Reviewed", len(done))
    c3.metric("Pending", len(pending))
    c4.metric("Progress", f"{100 * len(done) / len(queue):.0f}%")

    with st.sidebar:
        st.header("View")
        mode = st.radio(
            "Who is reviewing?",
            ["Anyone (plain English)", "Engineer (SQL)"],
            help=(
                "Both modes act on the same queue and log decisions identically. "
                "Plain English leads with the answer; Engineer leads with the SQL."
            ),
        )
        expert = mode.startswith("Engineer")
        st.divider()
        st.header("Filters")
        show_done = st.checkbox("Show already reviewed", value=False)
        max_conf = st.slider("Only show confidence at or below", 0.0, 1.0, 1.0, 0.05)
        st.divider()
        st.caption(
            "A query reaches this queue when the system's confidence falls below "
            "the routing threshold, or when a safety rule fires regardless of "
            "confidence (for example: it is not a read-only query, or it returns "
            "an unusually large amount of data)."
        )
        if st.button("Export decisions (CSV)", use_container_width=True):
            df = pd.read_sql_query("SELECT * FROM decisions", conn)
            st.download_button(
                "Download", df.to_csv(index=False), "review_decisions.csv", "text/csv"
            )

    items = [i for i in (queue if show_done else pending) if i.get("confidence", 0) <= max_conf]
    if not items:
        st.success("Queue clear — nothing pending.")
        return

    for item in items:
        qid = item["question_id"]
        conf = float(item.get("confidence", 0.0))
        badge = "🔴" if conf < 0.34 else "🟡" if conf < 0.7 else "🟢"

        with st.expander(f"{badge} {item['question'][:95]}", expanded=False):
            st.markdown(f"### {item['question']}")
            if item.get("evidence"):
                st.caption(f"Context provided with the question: {item['evidence']}")

            st.info(describe_confidence(conf, int(item.get("n_candidates", 1))))
            if item.get("reasons"):
                st.caption("Flagged because: " + "; ".join(item["reasons"]))

            st.divider()

            if expert:
                sql = render_expert(item, key=f"sql_{qid}")
                st.markdown("**Result preview**")
                render_answer(item, sql)
                with st.expander("Plain-English description"):
                    render_plain(item, sql)
            else:
                st.markdown("#### The answer the system found")
                render_answer(item, item.get("sql", ""))
                st.divider()
                render_plain(item, item.get("sql", ""))
                with st.expander("Show the database query (for engineers)"):
                    st.code(item.get("sql", ""), language="sql")
                sql = item.get("sql", "")

            st.divider()
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
