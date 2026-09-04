"""FastAPI service (CLAUDE.md section 4, `feat/api-rewrite`).

QueryMind had no API layer -- this is a build, not a rewrite (see
docs/migration-notes.md). The surface is shaped around the agent contract
rather than a direct question-to-SQL call, so the confidence and routing
decision are first-class in the response:

    POST /query          -> sql, confidence, decision, result | review_id,
                            trace_id, cost_usd, latency_ms
    GET  /review/queue   -> pending low-confidence queries
    POST /review/{id}    -> approve | edit | reject
    GET  /health
"""

from __future__ import annotations

import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from sqlsentinel.agent import Agent
from sqlsentinel.confidence import ConfidenceModel, agreement_confidence, extract_features
from sqlsentinel.executor import bird_executor
from sqlsentinel.llm import get_client
from sqlsentinel.router import Decision, Router
from sqlsentinel.schema_linker import bird_schema
from sqlsentinel.tracing import get_tracer, span

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_ROOT = Path(os.getenv("BIRD_DEV_ROOT", "data/bird/dev_20240627")) / "dev_databases"

_state: dict[str, Any] = {}
_review_queue: dict[str, dict] = {}


# ---------------------------------------------------------------- models


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, description="Natural language question")
    db_id: str = Field(..., description="Target database id")
    evidence: str = Field("", description="Optional external-knowledge hint")
    k: int = Field(3, ge=1, le=7, description="Self-consistency samples")
    execute: bool = Field(True, description="Execute when routed AUTO")


class QueryResponse(BaseModel):
    sql: str
    confidence: float
    scorer: Literal["calibrated", "agreement"] = "agreement"
    decision: Literal["AUTO", "REVIEW"]
    reasons: list[str] = []
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    row_count: int | None = None
    review_id: str | None = None
    trace_id: str
    cost_usd: float
    latency_ms: float


class ReviewItem(BaseModel):
    review_id: str
    question: str
    db_id: str
    sql: str
    confidence: float
    reasons: list[str]
    created_at: float


class ReviewAction(BaseModel):
    action: Literal["approve", "edit", "reject"]
    edited_sql: str | None = None
    note: str | None = None


class ReviewResult(BaseModel):
    review_id: str
    action: str
    final_sql: str
    executed: bool = False
    row_count: int | None = None


class Health(BaseModel):
    status: str
    provider: str
    model: str
    databases: int
    threshold: float
    scorer: Literal["calibrated", "agreement"]


# ---------------------------------------------------------------- app


def _load_confidence_model() -> ConfidenceModel | None:
    """Load the calibrated scorer if one has been trained.

    Falls back to the raw agreement rate when absent. That fallback is not just
    convenience: the calibrated model is fitted on a specific agent
    configuration, and silently applying it to a different one would produce
    confident-looking numbers with no basis. Serving v1 and saying so is the
    honest default.
    """
    path = Path(os.getenv("CONFIDENCE_MODEL", REPO_ROOT / "results" / "confidence-model.pkl"))
    if not path.exists():
        return None
    try:
        return ConfidenceModel.load(path)
    except Exception:
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001 - required by FastAPI
    client = get_client()
    _state["agent"] = Agent(client=client, db_root=DB_ROOT, k=3)
    _state["router"] = Router(threshold=float(os.getenv("ROUTING_THRESHOLD", "0.5")))
    _state["tracer"] = get_tracer()
    _state["confidence_model"] = _load_confidence_model()
    yield
    _state.clear()


app = FastAPI(
    title="SQLSentinel",
    version="0.1.0",
    description="Evaluation-driven text-to-SQL with calibrated confidence and human review",
    lifespan=lifespan,
)


@app.get("/health", response_model=Health)
def health() -> Health:
    agent: Agent = _state["agent"]
    n = len(list(DB_ROOT.glob("*/*.sqlite"))) if DB_ROOT.exists() else 0
    return Health(
        status="ok",
        provider=agent.client.provider,
        model=agent.client.model,
        databases=n,
        threshold=_state["router"].threshold,
        scorer="calibrated" if _state.get("confidence_model") else "agreement",
    )


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest) -> QueryResponse:
    agent: Agent = _state["agent"]
    router: Router = _state["router"]
    trace_id = uuid.uuid4().hex
    t0 = time.time()

    db_path = DB_ROOT / req.db_id / f"{req.db_id}.sqlite"
    if not db_path.exists():
        raise HTTPException(404, f"unknown database '{req.db_id}'")

    with span(_state["tracer"], "query", trace_id=trace_id, db_id=req.db_id):
        agent.k = req.k
        trace = agent.predict_one(
            {
                "question_id": -1,
                "db_id": req.db_id,
                "question": req.question,
                "evidence": req.evidence,
            }
        )

        if trace.error:
            raise HTTPException(502, f"generation failed: {trace.error}")

        schema = bird_schema(str(DB_ROOT), req.db_id)
        feats = extract_features(trace, req.question, req.evidence, len(schema.tables))

        model: ConfidenceModel | None = _state.get("confidence_model")
        if model is not None:
            confidence = model.predict(feats)
            scorer = "calibrated"
        else:
            confidence = agreement_confidence(feats)
            scorer = "agreement"

        decision = router.route(
            trace.sql,
            confidence,
            row_count=trace.result_row_count,
            executed_ok=trace.executed_ok,
            n_tables=len(schema.tables),
        )

    resp = QueryResponse(
        sql=trace.sql,
        confidence=round(confidence, 4),
        scorer=scorer,
        decision=decision.decision.value,
        reasons=decision.reasons,
        trace_id=trace_id,
        cost_usd=round(trace.cost_usd, 8),
        latency_ms=round((time.time() - t0) * 1000, 1),
    )

    if decision.decision is Decision.REVIEW:
        rid = uuid.uuid4().hex[:12]
        _review_queue[rid] = {
            "review_id": rid,
            "question": req.question,
            "db_id": req.db_id,
            "sql": trace.sql,
            "confidence": confidence,
            "reasons": decision.reasons,
            "created_at": time.time(),
        }
        resp.review_id = rid
        return resp

    if req.execute and trace.sql:
        res = bird_executor(DB_ROOT, req.db_id).execute(trace.sql)
        if res.ok:
            resp.columns = res.columns
            resp.rows = [list(r) for r in res.rows[:100]]
            resp.row_count = res.row_count
    return resp


@app.get("/review/queue", response_model=list[ReviewItem])
def review_queue() -> list[ReviewItem]:
    return [ReviewItem(**v) for v in sorted(_review_queue.values(), key=lambda d: d["confidence"])]


@app.post("/review/{review_id}", response_model=ReviewResult)
def review(review_id: str, action: ReviewAction) -> ReviewResult:
    item = _review_queue.pop(review_id, None)
    if item is None:
        raise HTTPException(404, f"no pending review '{review_id}'")

    if action.action == "reject":
        return ReviewResult(review_id=review_id, action="reject", final_sql="")

    final_sql = action.edited_sql if action.action == "edit" else item["sql"]
    if action.action == "edit" and not final_sql:
        raise HTTPException(400, "edit requires edited_sql")

    res = bird_executor(DB_ROOT, item["db_id"]).execute(final_sql)
    if not res.ok:
        raise HTTPException(400, f"query failed: {res.error}")
    return ReviewResult(
        review_id=review_id,
        action=action.action,
        final_sql=final_sql,
        executed=True,
        row_count=res.row_count,
    )
