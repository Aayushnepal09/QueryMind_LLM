"""The text-to-SQL agent.

Phase 1 is the single-shot baseline: full schema, one sample, temperature 0, no
retrieval, no correction. Every Phase 2 technique is a flag on top of it, so
each can be enabled alone and its delta measured against the same baseline
(CLAUDE.md section 8: "one branch per technique, measure after each").

    few_shot        retrieve similar calibration questions as exemplars
    prune_schema    drop tables the question plausibly does not need
    max_corrections re-prompt with the execution error, capped at 2 rounds
    k               generate k candidates for self-consistency (Phase 3)
"""

from __future__ import annotations

import concurrent.futures as cf
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from sqlsentinel.executor import ExecutionResult, bird_executor
from sqlsentinel.generator import SQLGenerator, extract_sql
from sqlsentinel.llm import LLMClient
from sqlsentinel.retrieval import ExemplarStore, render_exemplars
from sqlsentinel.schema_linker import bird_schema

CORRECTION_TEMPLATE = """The SQL you produced failed to execute.

SQL:
{sql}

Error:
{error}

Rewrite it as a single valid SQLite query that answers the original question.
Return ONLY the corrected SQL."""


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

    # technique bookkeeping
    n_correction_rounds: int = 0
    executed_ok: bool = False
    execution_errored_pre_correction: bool = False
    result_row_count: int = 0
    n_candidates: int = 1
    agreement_rate: float = 1.0
    n_tables_in_prompt: int = 0
    exemplar_similarity: float = 0.0


@dataclass
class Agent:
    client: LLMClient
    db_root: Path
    use_evidence: bool = True
    few_shot: int = 0
    prune_schema: bool = False
    max_corrections: int = 0
    k: int = 1
    naive_prompt: bool = False
    exemplars: ExemplarStore | None = None
    traces: list[AgentTrace] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.generator = SQLGenerator(self.client, naive=self.naive_prompt)
        if self.few_shot and self.exemplars is None:
            raise ValueError("few_shot requires an ExemplarStore")

    # ---------------------------------------------------------------- one question

    def predict_one(self, q: dict) -> AgentTrace:
        t = AgentTrace(question_id=q["question_id"], db_id=q["db_id"], sql="")
        try:
            schema = bird_schema(str(self.db_root), q["db_id"])
            evidence = q.get("evidence", "") if self.use_evidence else ""
            if self.prune_schema:
                schema = schema.prune(q["question"], evidence)
            t.n_tables_in_prompt = len(schema.tables)

            prefix = ""
            if self.few_shot:
                ex = self.exemplars.retrieve(
                    q["question"],
                    k=self.few_shot,
                    db_id=q["db_id"],
                    # never retrieve the question being answered
                    exclude_question_id=q["question_id"],
                )
                prefix = render_exemplars(ex)
                t.exemplar_similarity = max((e.similarity for e in ex), default=0.0)

            cands = self.generator.generate(
                q["question"], schema, evidence, k=self.k, prompt_prefix=prefix
            )
            for c in cands:
                t.latency_s += c.response.latency_s
                t.cost_usd += c.response.cost_usd
                t.prompt_tokens += c.response.prompt_tokens
                t.output_tokens += c.response.output_tokens
            t.cached = all(c.response.cached for c in cands)
            t.n_candidates = len(cands)

            ex_runner = bird_executor(self.db_root, q["db_id"])
            if self.k > 1:
                sql, agreement, res = self._vote(cands, ex_runner)
                t.agreement_rate = agreement
            else:
                sql = cands[0].sql
                res = ex_runner.execute(sql) if sql else ExecutionResult(error="empty prediction")

            t.execution_errored_pre_correction = not res.ok

            rounds = 0
            while not res.ok and rounds < self.max_corrections and sql:
                rounds += 1
                sql, res = self._correct(q, schema, evidence, sql, res, ex_runner, rounds)
            t.n_correction_rounds = rounds

            t.sql = sql
            t.raw = cands[0].raw
            t.empty = not sql
            t.executed_ok = res.ok
            t.result_row_count = res.row_count
        except Exception as e:
            t.empty = True
            t.error = f"{type(e).__name__}: {e}"
        return t

    def _vote(self, cands, ex_runner) -> tuple[str, float, ExecutionResult]:
        """Pick the candidate whose result set the most candidates agree on.

        Agreement is over *executed results*, not SQL text -- two syntactically
        different queries that return the same rows are the same answer, and
        that is exactly the equivalence BIRD scores on.
        """
        results = [
            (c.sql, ex_runner.execute(c.sql) if c.sql else ExecutionResult(error="empty"))
            for c in cands
        ]
        buckets = Counter(r.normalized() for _, r in results if r.ok)
        if not buckets:
            return results[0][0], 0.0, results[0][1]
        modal, count = buckets.most_common(1)[0]
        for sql, res in results:
            if res.ok and res.normalized() == modal:
                return sql, count / len(results), res
        return results[0][0], 0.0, results[0][1]

    def _correct(self, q, schema, evidence, sql, res, ex_runner, round_no):
        """Re-prompt with the execution error. Capped at max_corrections."""
        user = self.generator.build_prompt(q["question"], schema, evidence)
        user += "\n\n" + CORRECTION_TEMPLATE.format(sql=sql, error=res.error or "no rows")
        resp = self.client.complete(
            "You are a SQLite expert. Fix the query. Return only SQL.",
            user,
            temperature=0.0,
            sample_index=100 + round_no,  # distinct cache namespace per round
        )
        new_sql = extract_sql(resp.text)
        if not new_sql:
            return sql, res
        return new_sql, ex_runner.execute(new_sql)

    # ---------------------------------------------------------------- batch

    def predict(self, questions: list[dict], workers: int = 1) -> dict[int, str]:
        """workers=1 for Ollama (one GPU, requests serialise anyway).
        Raise it for Gemini, but stay inside the free tier's rate limit."""
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
            "executed_ok": sum(t.executed_ok for t in self.traces),
            "corrections_applied": sum(t.n_correction_rounds > 0 for t in self.traces),
            "total_cost_usd": sum(t.cost_usd for t in self.traces),
            "mean_latency_s": sum(t.latency_s for t in live) / max(len(live), 1),
            "mean_prompt_tokens": sum(t.prompt_tokens for t in self.traces) / n,
            "mean_output_tokens": sum(t.output_tokens for t in self.traces) / n,
            "mean_agreement": sum(t.agreement_rate for t in self.traces) / n,
        }


# Backwards-compatible alias: Phase 1 shipped this name.
BaselineAgent = Agent
