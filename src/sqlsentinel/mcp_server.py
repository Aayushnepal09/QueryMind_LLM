"""MCP server exposing SQLSentinel as tools (spec §4, stretch goal).

    uv run --extra mcp python -m sqlsentinel.mcp_server

Lets an MCP-speaking assistant query the benchmark databases in natural
language, while keeping this project's safety properties rather than handing
the model a raw SQL socket:

  * every query runs through the read-only executor with its timeout
  * every answer carries a confidence score and the routing decision
  * a low-confidence answer is returned **as a low-confidence answer**, not
    silently as fact

That last point is the reason this wrapper is worth more than a plain
"run_sql" tool. Findings §1 measured that 59% of this agent's wrong answers
execute cleanly and return plausible rows; an MCP tool that returned those
without qualification would launder them into a conversation as facts. Each
response states how much to trust it.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from sqlsentinel.agent import Agent
from sqlsentinel.confidence import agreement_confidence, extract_features
from sqlsentinel.executor import bird_executor
from sqlsentinel.explain import describe_confidence, explain
from sqlsentinel.llm import get_client
from sqlsentinel.router import Decision, Router
from sqlsentinel.schema_linker import bird_schema

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_ROOT = Path(os.getenv("BIRD_DEV_ROOT", "data/bird/dev_20240627")) / "dev_databases"

_agent: Agent | None = None
_router: Router | None = None


def _ensure() -> tuple[Agent, Router]:
    global _agent, _router
    if _agent is None:
        _agent = Agent(client=get_client(), db_root=DB_ROOT, k=3)
        _router = Router(threshold=float(os.getenv("ROUTING_THRESHOLD", "0.7")))
    return _agent, _router  # type: ignore[return-value]


def list_databases() -> list[dict[str, Any]]:
    """Available databases and their tables."""
    if not DB_ROOT.exists():
        return []
    out = []
    for path in sorted(DB_ROOT.glob("*/*.sqlite")):
        schema = bird_schema(str(DB_ROOT), path.stem)
        out.append(
            {
                "db_id": path.stem,
                "tables": [t.name for t in schema.tables],
                "n_columns": schema.n_columns,
            }
        )
    return out


def describe_database(db_id: str) -> str:
    """Full schema for one database, as annotated DDL."""
    if not (DB_ROOT / db_id).exists():
        return f"Unknown database '{db_id}'. Call list_databases for the available ids."
    return bird_schema(str(DB_ROOT), db_id).to_prompt()


def ask(question: str, db_id: str, evidence: str = "") -> dict[str, Any]:
    """Answer a natural-language question against a database.

    Returns the answer together with an explicit statement of how far it should
    be trusted. `trust` is the field to read first:

      "high"      the samples agreed and no safety rule fired
      "low"       the samples disagreed; treat the answer as a suggestion
      "unusable"  the query failed, or a safety rule blocked it
    """
    if not (DB_ROOT / db_id).exists():
        return {"error": f"Unknown database '{db_id}'.", "trust": "unusable"}

    agent, router = _ensure()
    trace = agent.predict_one(
        {"question_id": -1, "db_id": db_id, "question": question, "evidence": evidence}
    )
    if trace.error:
        return {"error": trace.error, "trust": "unusable"}

    schema = bird_schema(str(DB_ROOT), db_id)
    feats = extract_features(trace, question, evidence, len(schema.tables))
    confidence = agreement_confidence(feats)
    decision = router.route(
        trace.sql,
        confidence,
        row_count=trace.result_row_count,
        executed_ok=trace.executed_ok,
        n_tables=len(schema.tables),
    )

    exp = explain(trace.sql)
    result: dict[str, Any] = {
        "sql": trace.sql,
        "explanation": exp.summary,
        "confidence": round(confidence, 3),
        "confidence_in_words": describe_confidence(confidence, trace.n_candidates),
        "warnings": exp.warnings,
        "trust": "high" if decision.decision is Decision.AUTO else "low",
    }

    if not trace.executed_ok:
        result["trust"] = "unusable"
        result["error"] = "the generated query did not execute"
        return result

    if decision.decision is Decision.REVIEW:
        result["needs_review_because"] = decision.reasons

    res = bird_executor(DB_ROOT, db_id).execute(trace.sql)
    if res.ok:
        result["columns"] = res.columns
        result["rows"] = [list(r) for r in res.rows[:50]]
        result["row_count"] = res.row_count
        if res.row_count > 50:
            result["note"] = f"showing 50 of {res.row_count} rows"
    return result


def run_sql(sql: str, db_id: str) -> dict[str, Any]:
    """Execute a read-only SQL query directly.

    Non-SELECT statements, stacked statements and anything that could mutate
    data are rejected before reaching the database, and the connection is
    opened read-only regardless.
    """
    if not (DB_ROOT / db_id).exists():
        return {"error": f"Unknown database '{db_id}'."}
    res = bird_executor(DB_ROOT, db_id).execute(sql)
    if not res.ok:
        return {"error": res.error, "timed_out": res.timed_out}
    return {
        "columns": res.columns,
        "rows": [list(r) for r in res.rows[:50]],
        "row_count": res.row_count,
        "truncated": res.truncated,
        "duration_s": round(res.duration_s, 3),
    }


def build_server():
    """Construct the FastMCP server. Imported lazily so the rest of the package
    does not depend on fastmcp being installed."""
    from fastmcp import FastMCP

    mcp = FastMCP(
        "SQLSentinel",
        instructions=(
            "Query databases in natural language. Every answer carries a `trust` "
            "field: 'high' means the model's samples agreed, 'low' means they "
            "disagreed and the answer should be treated as a suggestion rather "
            "than a fact, and 'unusable' means the query failed. Prefer `ask` "
            "over `run_sql`; `run_sql` bypasses the confidence layer."
        ),
    )
    mcp.tool(list_databases)
    mcp.tool(describe_database)
    mcp.tool(ask)
    mcp.tool(run_sql)
    return mcp


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()
