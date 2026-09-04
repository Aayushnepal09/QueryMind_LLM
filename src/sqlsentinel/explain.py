"""Describe a SQL query in plain English.

The review queue exists so a *person* can catch queries the agent is unsure
about. If that person has to read SQL, the queue only works for engineers --
which excludes exactly the analysts and domain experts most able to spot a
wrong answer in their own data.

This turns a query into sentences a non-technical reviewer can check against
their intent. It is deliberately rule-based rather than model-generated:

  * it costs nothing and cannot hallucinate a description that disagrees with
    the query it is describing, which would be worse than showing no
    description at all
  * it is deterministic, so the same query always reads the same way
  * it is testable

It describes *structure*, not semantics. It cannot tell a reviewer whether the
query is right -- that is the reviewer's job, and the result preview is the
better evidence. It tells them what the query is trying to do.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

_AGGREGATES = {
    "COUNT": "count",
    "SUM": "total",
    "AVG": "average",
    "MAX": "highest",
    "MIN": "lowest",
}

_OPERATORS = {
    "=": "is",
    "!=": "is not",
    "<>": "is not",
    ">": "is greater than",
    "<": "is less than",
    ">=": "is at least",
    "<=": "is at most",
    "LIKE": "contains",
}


@dataclass
class Explanation:
    summary: str
    details: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_text(self) -> str:
        parts = [self.summary]
        parts.extend(f"- {d}" for d in self.details)
        return "\n".join(parts)


def _humanise(identifier: str) -> str:
    """`product_unit_price` -> "product unit price"; `CDSCode` -> "CDS code"."""
    name = identifier.strip("`\"[]'")
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", name)
    return name.replace("_", " ").strip().lower()


def _tables(sql: str) -> list[str]:
    found = re.findall(r"\b(?:FROM|JOIN)\s+[`\"\[]?(\w+)", sql, re.I)
    seen: list[str] = []
    for t in found:
        if t.lower() not in {s.lower() for s in seen}:
            seen.append(t)
    return seen


def _select_clause(sql: str) -> str:
    m = re.search(r"\bSELECT\b(.*?)\bFROM\b", sql, re.I | re.S)
    return m.group(1).strip() if m else ""


def _mask_quoted(text: str) -> str:
    """Blank out quoted identifiers so keyword matching cannot see inside them.

    BIRD schemas are full of columns like `Free Meal Count (K-12)`. Searching
    for aggregate functions without masking these reads that column name as a
    COUNT(...) call and describes a plain column list as a counting query.
    """
    return re.sub(r"`[^`]*`|\[[^\]]*\]|\"[^\"]*\"", lambda m: " " * len(m.group(0)), text)


def _describe_selection(sql: str) -> str:
    clause = _select_clause(sql)
    if not clause:
        return "Looks up information"
    masked = _mask_quoted(clause)

    if re.match(r"^\s*DISTINCT\b", masked, re.I):
        clause = re.sub(r"^\s*DISTINCT\b", "", clause, flags=re.I).strip()
        masked = re.sub(r"^\s*DISTINCT\b", "", masked, flags=re.I).strip()
        prefix = "Lists the unique"
        article = ""
    else:
        prefix = "Lists"
        article = " the"

    if clause.strip() == "*":
        return f"{prefix} every column"

    # matched against the masked clause so a column named `... Count (K-12)`
    # is not mistaken for a COUNT() call
    agg = re.search(r"\b(COUNT|SUM|AVG|MAX|MIN)\s*\(([^)]*)\)", masked, re.I)
    if agg:
        word = _AGGREGATES[agg.group(1).upper()]
        target = agg.group(2).strip()
        if target in {"*", "1"} or not target:
            return "Counts how many records match"
        if word == "count":
            return f"Counts how many different {_humanise(target)} values there are"
        return f"Works out the {word} {_humanise(target)}"

    # plain column list, ignoring aliases
    cols = [c.strip() for c in re.split(r",(?![^(]*\))", clause) if c.strip()]
    names = [_humanise(re.split(r"\s+AS\s+", c, flags=re.I)[0].split(".")[-1]) for c in cols]
    if len(names) == 1:
        return f"{prefix}{article} {names[0]}"
    if len(names) <= 4:
        return f"{prefix}{article} {', '.join(names[:-1])} and {names[-1]}"
    return f"{prefix} {len(names)} columns including {names[0]} and {names[1]}"


def _describe_filters(sql: str) -> list[str]:
    m = re.search(r"\bWHERE\b(.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)", sql, re.I | re.S)
    if not m:
        return []

    out = []
    for cond in re.split(r"\s+\bAND\b\s+", m.group(1).strip(), flags=re.I)[:5]:
        cond = cond.strip().rstrip(")").lstrip("(")
        c = re.match(r"[`\"\[]?([\w.]+)[`\"\]]?\s*(=|!=|<>|>=|<=|>|<|LIKE)\s*(.+)", cond, re.I)
        if c:
            col, op, val = c.groups()
            val = val.strip().strip("'\"%")
            out.append(
                f"only where the {_humanise(col.split('.')[-1])} "
                f'{_OPERATORS.get(op.upper(), op)} "{val}"'
            )
        elif re.search(r"\bIS\s+NOT\s+NULL\b", cond, re.I):
            col = cond.split()[0]
            out.append(f"only where the {_humanise(col)} has a value")
        elif re.search(r"\bOR\b", cond, re.I):
            out.append("with a condition matching any of several values")
    return out


def explain(sql: str) -> Explanation:
    """Turn a SELECT into sentences a non-technical reviewer can check."""
    if not sql or not sql.strip():
        return Explanation(
            summary="No query was produced.",
            warnings=["The system could not turn this question into a database query."],
        )

    flat = " ".join(sql.split())
    exp = Explanation(summary=_describe_selection(flat))

    tables = _tables(flat)
    if tables:
        readable = [_humanise(t) for t in tables]
        if len(readable) == 1:
            exp.details.append(f"from the {readable[0]} records")
        else:
            exp.details.append(
                f"combining the {', '.join(readable[:-1])} and {readable[-1]} records"
            )

    exp.details.extend(_describe_filters(flat))

    if g := re.search(r"\bGROUP\s+BY\b\s+([\w.`\"\[\], ]+)", flat, re.I):
        cols = [_humanise(c.split(".")[-1]) for c in g.group(1).split(",")[:3]]
        exp.details.append(f"grouped by {', '.join(cols)}")

    if o := re.search(r"\bORDER\s+BY\b\s+(.+?)(?:\bLIMIT\b|$)", flat, re.I):
        direction = "highest first" if re.search(r"\bDESC\b", o.group(1), re.I) else "lowest first"
        exp.details.append(f"sorted {direction}")

    if lim := re.search(r"\bLIMIT\s+(\d+)", flat, re.I):
        exp.details.append(f"showing only the top {lim.group(1)}")

    # things a reviewer should be told about explicitly, matched on the masked
    # text so identifiers containing SQL keywords do not trigger them
    masked_flat = _mask_quoted(flat)
    if re.search(r"\bJOIN\b", masked_flat, re.I) and len(tables) > 2:
        exp.warnings.append(
            f"This combines {len(tables)} different record types. "
            f"Queries that join many tables are the most common source of errors."
        )
    if re.search(r"\(\s*SELECT\b", masked_flat, re.I):
        exp.warnings.append("This contains a nested lookup, which is harder to verify.")
    if not re.search(r"\bWHERE\b", masked_flat, re.I) and not re.search(
        r"\bCOUNT\b", masked_flat, re.I
    ):
        exp.warnings.append("This has no filter, so it looks at every record.")

    return exp


def describe_confidence(confidence: float, n_candidates: int = 1) -> str:
    """Explain a confidence score in words rather than as a number."""
    if n_candidates > 1:
        agreed = round(confidence * n_candidates)
        if agreed == n_candidates:
            base = f"All {n_candidates} attempts produced the same answer."
        elif agreed <= 1:
            base = f"All {n_candidates} attempts produced different answers."
        else:
            base = f"Only {agreed} of {n_candidates} attempts produced this answer."
    else:
        base = "This answer was produced in a single attempt."

    if confidence >= 0.8:
        return f"{base} The system is fairly confident."
    if confidence >= 0.5:
        return f"{base} The system is unsure."
    return f"{base} The system has little confidence in this answer."
