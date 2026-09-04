"""SQL generation and extraction.

The prompt here is a direct port of QueryMind's `generate_sql_with_gpt` (see
docs/migration-notes.md) -- its eight numbered requirements, retargeted from
PostgreSQL to SQLite and given BIRD's `evidence` field. Keeping it recognisable
is deliberate: CLAUDE.md section 8 wants QueryMind's prompting to *be* the
baseline, so that Phase 2's deltas measure new technique rather than incidental
prompt rewriting.

The extractor is the part that needed real work. QueryMind's version was:

    re.sub(r"^```sql\\s*|\\s*```$", "", text, flags=re.I | re.M).strip()

which strips fences and nothing else. Observed model output on 2026-09-03 was
prose, then a fenced block, then a numbered explanation -- all of which that
regex would hand to the executor as "SQL".
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlsentinel.llm import LLMClient, LLMResponse
from sqlsentinel.schema_linker import Schema

SYSTEM_PROMPT = (
    "You are a SQLite expert who generates accurate SQL queries from natural "
    "language questions. You reply with a single SQL query and nothing else."
)

# Ported from QueryMind, retargeted to SQLite. Requirement 4 (QueryMind's
# "default LIMIT 100") is deliberately dropped: BIRD scores by exact result-set
# equality, so an unrequested LIMIT turns correct answers into failures.
USER_TEMPLATE = """Given the following database schema and a user's question, generate a valid SQLite query.

Database schema:
{schema}

Join paths available:
{join_paths}
{evidence_block}
User Question: {question}

Requirements:
1. Return ONLY the SQL query. No explanation, no commentary.
2. Use proper JOINs when the answer spans multiple tables.
3. Use appropriate aggregations (COUNT, AVG, SUM, MAX, MIN) when needed.
4. Quote identifiers containing spaces or special characters with backticks.
5. Only reference tables and columns that appear in the schema above.
6. Return exactly the columns the question asks for, in the order asked.
7. Do not add LIMIT unless the question asks for a specific number of results.
8. Make sure the query is syntactically valid SQLite.

SQL query:"""

_FENCE = re.compile(r"```(?:sql|sqlite)?\s*(.*?)\s*```", re.S | re.I)
_LEAD = re.compile(r"^\s*(SELECT|WITH)\b", re.I)
_TRAILING_PROSE = re.compile(r"\n\s*(?:This query|The query|Explanation|Note|\d+\.\s)", re.I)


def extract_sql(text: str) -> str:
    """Pull a single SQL statement out of a model response.

    Handles, in order: fenced blocks (preferring one that actually starts with
    SELECT/WITH, since models sometimes fence the schema too), bare SQL
    preceded by prose, and trailing explanations after the statement.
    Returns "" when nothing SQL-shaped is found -- the caller decides what an
    empty prediction means, rather than this silently emitting something
    executable.
    """
    if not text:
        return ""

    candidates = [b.strip() for b in _FENCE.findall(text)]
    for block in candidates:
        if _LEAD.match(block):
            return _tidy(block)

    # No usable fence: find where SQL starts in the raw text and cut the prose.
    m = re.search(r"\b(SELECT|WITH)\b", text, re.I)
    if not m:
        return _tidy(candidates[0]) if candidates else ""
    return _tidy(text[m.start() :])


def _tidy(sql: str) -> str:
    """Trim trailing prose, fences, and statement punctuation."""
    sql = sql.replace("```", " ")
    if m := _TRAILING_PROSE.search(sql):
        sql = sql[: m.start()]
    # keep only the first statement
    sql = sql.split(";")[0]
    return " ".join(sql.split()).strip()


@dataclass
class Candidate:
    sql: str
    raw: str
    response: LLMResponse

    @property
    def empty(self) -> bool:
        return not self.sql


class SQLGenerator:
    """Question + schema + evidence -> k candidate SQL strings."""

    def __init__(self, client: LLMClient, max_tokens: int = 512):
        self.client = client
        self.max_tokens = max_tokens

    def build_prompt(
        self, question: str, schema: Schema, evidence: str = "", prompt_prefix: str = ""
    ) -> str:
        joins = schema.join_paths()
        evidence_block = (
            f"\nExternal knowledge (use this, it is required to answer correctly):\n{evidence}\n"
            if evidence and evidence.strip()
            else ""
        )
        body = USER_TEMPLATE.format(
            schema=schema.to_prompt(),
            join_paths="\n".join(joins) if joins else "(none declared)",
            evidence_block=evidence_block,
            question=question,
        )
        return f"{prompt_prefix}\n{body}" if prompt_prefix else body

    def generate(
        self,
        question: str,
        schema: Schema,
        evidence: str = "",
        k: int = 1,
        temperature: float | None = None,
        prompt_prefix: str = "",
    ) -> list[Candidate]:
        """Generate k candidates.

        k=1 uses temperature 0 (deterministic baseline). k>1 defaults to 0.7,
        which is what makes the samples diverge enough for self-consistency
        agreement to carry signal (CLAUDE.md section 6).
        """
        if temperature is None:
            temperature = 0.0 if k == 1 else 0.7

        user = self.build_prompt(question, schema, evidence, prompt_prefix)
        out = []
        for i in range(k):
            resp = self.client.complete(
                SYSTEM_PROMPT,
                user,
                temperature=temperature,
                sample_index=i,
                max_tokens=self.max_tokens,
            )
            out.append(Candidate(sql=extract_sql(resp.text), raw=resp.text, response=resp))
        return out
