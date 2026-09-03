"""The baseline agent: question -> schema -> single-shot SQL.

Phase 1 (CLAUDE.md section 8) deliberately keeps this thin -- full schema, one
sample, temperature 0, no correction, no retrieval. Everything Phase 2 adds is
measured as a delta against this, so this is the number that gets cited forever.
"""

from __future__ import annotations

import concurrent.futures as cf
from dataclasses import dataclass, field
from pathlib import Path

from sqlsentinel.generator import SQLGenerator
from sqlsentinel.llm import LLMClient
from sqlsentinel.schema_linker import bird_schema


@dataclass
class AgentTrace:
    """Per-question record. Phase 4 turns this into OpenTelemetry spans."""

    question_id: int
    db_id: str
    sql: str
    raw: str = ""
    latency_s: float = 0.0
    cost_usd: float = 0.0
    prompt_tokens: int = 0
    output_tokens: int = 0
    cached: bool = False
    empty: bool = False
    error: str | None = None


@dataclass
class BaselineAgent:
    client: LLMClient
    db_root: Path
    use_evidence: bool = True
    traces: list[AgentTrace] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.generator = SQLGenerator(self.client)

    def predict_one(self, q: dict) -> AgentTrace:
        try:
            schema = bird_schema(str(self.db_root), q["db_id"])
            evidence = q.get("evidence", "") if self.use_evidence else ""
            cand = self.generator.generate(q["question"], schema, evidence, k=1)[0]
            r = cand.response
            return AgentTrace(
                question_id=q["question_id"], db_id=q["db_id"], sql=cand.sql,
                raw=cand.raw, latency_s=r.latency_s, cost_usd=r.cost_usd,
                prompt_tokens=r.prompt_tokens, output_tokens=r.output_tokens,
                cached=r.cached, empty=cand.empty,
            )
        except Exception as e:
            return AgentTrace(
                question_id=q["question_id"], db_id=q["db_id"], sql="",
                empty=True, error=f"{type(e).__name__}: {e}",
            )

    def predict(self, questions: list[dict], workers: int = 1) -> dict[int, str]:
        """Generate SQL for every question.

        workers=1 for Ollama: the local model serialises on one GPU anyway, and
        concurrent requests just add contention. Raise it for Gemini, where
        latency is network-bound -- but stay inside the free tier's rate limit.
        """
        if workers <= 1:
            self.traces = [self.predict_one(q) for q in questions]
        else:
            with cf.ThreadPoolExecutor(max_workers=workers) as pool:
                self.traces = list(pool.map(self.predict_one, questions))
        return {t.question_id: t.sql for t in self.traces}

    def summary(self) -> dict[str, float]:
        n = len(self.traces) or 1
        live = [t for t in self.traces if not t.cached]
        return {
            "n": len(self.traces),
            "empty_predictions": sum(t.empty for t in self.traces),
            "generation_errors": sum(t.error is not None for t in self.traces),
            "cache_hits": sum(t.cached for t in self.traces),
            "total_cost_usd": sum(t.cost_usd for t in self.traces),
            "mean_latency_s": sum(t.latency_s for t in live) / max(len(live), 1),
            "mean_prompt_tokens": sum(t.prompt_tokens for t in self.traces) / n,
            "mean_output_tokens": sum(t.output_tokens for t in self.traces) / n,
        }
