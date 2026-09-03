"""Streamlit human-review queue (CLAUDE.md section 8, `feat/review-ui`).

Shows the queries the router sent to REVIEW: the question, the generated SQL,
its confidence and why it was flagged, plus a live result preview. A reviewer
approves, edits, or rejects, and every decision is logged.

Deliberately plain. CLAUDE.md section 3 lists a polished custom front-end as a
non-goal -- the artifact is the evaluation, not the interface.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from sqlsentinel.executor import bird_executor
from sqlsentinel.router import Decision

REPO_ROOT = Path(__file__).resolve().parents[1]
QUEUE_FILE = REPO_ROOT / "results" / "review_queue.json"
DECISIONS_DB = REPO_ROOT / "results" / "review_decisions.db"
DB_ROOT = REPO_ROOT / "data" / "bird" / "dev_20240627" / "dev_databases"

st.set_page_config(page_title="SQLSentinel Review", page_icon="🛡️", layout="wide")


def init_db() -> sqlite3.Connection:
    DECISIONS_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DECISIONS_DB, check_same_thread=False)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS decisions ("
        " question_id INTEGER PRIMARY KEY, action TEXT NOT NULL,"
        " original_sql TEXT, final_sql TEXT, confidence REAL,"
        " reviewer_note TEXT, decided_at REAL NOT NULL)"
    )
    conn.commit()
    return conn


@st.cache_data
def load_queue() -> list[dict]:
    if not QUEUE_FILE.exists():
        return []
    return json.loads(QUEUE_FILE.read_text(encoding="utf-8"))


def record(conn, qid, action, original, final, confidence, note) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO decisions VALUES (?,?,?,?,?,?,?)",
        (qid, action, original, final, confidence, note, time.time()),
    )
    conn.commit()


def decided_ids(conn) -> set[int]:
    return {r[0] for r in conn.execute("SELECT question_id FROM decisions")}


def main() -> None:
    conn = init_db()
    queue = load_queue()

    st.title("🛡️ SQLSentinel — Review Queue")

    if not queue:
        st.info(
            "No review queue found. Generate one with:\n\n"
            "`uv run python -m sqlsentinel.eval --split dev_50 --predictor agent "
            "--k 5 --write-queue`"
        )
        return

    done = decided_ids(conn)
    pending = [item for item in queue if item["question_id"] not in done]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("In queue", len(queue))
    c2.metric("Reviewed", len(done))
    c3.metric("Pending", len(pending))
    reviewed_pct = 100 * len(done) / len(queue) if queue else 0
    c4.metric("Progress", f"{reviewed_pct:.0f}%")

    with st.sidebar:
        st.header("Filters")
        show_done = st.checkbox("Show already reviewed", value=False)
        max_conf = st.slider("Max confidence", 0.0, 1.0, 1.0, 0.05)
        st.divider()
        st.caption(
            "Queries reach this queue when confidence falls below the routing "
            "threshold, or when a risk rule fires regardless of confidence."
        )
        if st.button("Export decisions as CSV"):
            df = pd.read_sql_query("SELECT * FROM decisions", conn)
            st.download_button(
                "Download", df.to_csv(index=False), "review_decisions.csv", "text/csv"
            )

    items = queue if show_done else pending
    items = [i for i in items if i.get("confidence", 0) <= max_conf]

    if not items:
        st.success("Queue clear — nothing pending.")
        return

    for item in items:
        qid = item["question_id"]
        conf = item.get("confidence", 0.0)
        badge = "🔴" if conf < 0.3 else "🟡" if conf < 0.6 else "🟢"

        with st.expander(
            f"{badge} #{qid} · conf {conf:.2f} · {item['db_id']} — {item['question'][:80]}",
            expanded=False,
        ):
            st.markdown(f"**Question:** {item['question']}")
            if item.get("evidence"):
                st.caption(f"Evidence: {item['evidence']}")

            if item.get("reasons"):
                st.warning("Flagged because: " + "; ".join(item["reasons"]))

            edited = st.text_area(
                "SQL (edit before approving if needed)",
                value=item.get("sql", ""),
                height=140,
                key=f"sql_{qid}",
            )
            note = st.text_input("Reviewer note (optional)", key=f"note_{qid}")

            p1, p2 = st.columns(2)
            with p1:
                if st.button("▶ Preview result", key=f"prev_{qid}", use_container_width=True):
                    res = bird_executor(DB_ROOT, item["db_id"]).execute(edited)
                    if res.ok:
                        st.success(f"{res.row_count} rows in {res.duration_s:.2f}s")
                        st.dataframe(
                            pd.DataFrame(res.rows, columns=res.columns or None).head(50),
                            use_container_width=True,
                        )
                    else:
                        st.error(res.error)

            b1, b2, b3 = st.columns(3)
            if b1.button("✅ Approve", key=f"ok_{qid}", type="primary", use_container_width=True):
                action = "edit" if edited.strip() != item.get("sql", "").strip() else "approve"
                record(conn, qid, action, item.get("sql", ""), edited, conf, note)
                st.rerun()
            if b2.button("✏️ Approve as edited", key=f"ed_{qid}", use_container_width=True):
                record(conn, qid, "edit", item.get("sql", ""), edited, conf, note)
                st.rerun()
            if b3.button("❌ Reject", key=f"no_{qid}", use_container_width=True):
                record(conn, qid, "reject", item.get("sql", ""), "", conf, note)
                st.rerun()


if __name__ == "__main__":
    main()
