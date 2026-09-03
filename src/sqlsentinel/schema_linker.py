"""Schema introspection and prompt rendering.

CLAUDE.md section 4: "Port QueryMind's schema-injection logic. Start dumb:
include all tables if the DB is small. Optimize only if token cost hurts."

QueryMind's `DATABASE_SCHEMA` was a hand-written string literal describing one
fixed 6-table database. That does not survive contact with BIRD's 11 databases
(75 tables, 798 columns), so what carries over is the *shape* of the context it
provided, not the text:

  * tables and typed columns
  * primary keys and foreign keys, stated explicitly so join paths are visible
  * a note on how to read the data

rendered from live introspection instead of being typed out by hand.

Phase 1 emits the full schema. Pruning is `feat/schema-linking` in Phase 2, and
whether it beats full context is an open question (section 13) to be measured,
not assumed.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

SAMPLE_VALUES = 3


@dataclass
class Column:
    name: str
    type: str
    is_pk: bool = False
    samples: list[str] = field(default_factory=list)


@dataclass
class Table:
    name: str
    columns: list[Column] = field(default_factory=list)
    # (local_column, referenced_table, referenced_column)
    foreign_keys: list[tuple[str, str, str]] = field(default_factory=list)
    row_count: int = 0


@dataclass
class Schema:
    db_id: str
    tables: list[Table] = field(default_factory=list)

    @property
    def n_columns(self) -> int:
        return sum(len(t.columns) for t in self.tables)

    def to_prompt(self, include_samples: bool = True) -> str:
        """Render as annotated DDL.

        DDL rather than prose because the model is being asked to write SQL and
        CREATE TABLE is the form it has seen most of in training. Sample values
        matter more than they look: BIRD questions frequently filter on literal
        strings ("Charter School (Y/N)", county names), and without examples the
        model guesses at capitalisation and encoding and silently returns zero
        rows.
        """
        out: list[str] = []
        for t in self.tables:
            out.append(f"CREATE TABLE {_quote(t.name)} (")
            lines = []
            for c in t.columns:
                line = f"  {_quote(c.name)} {c.type or 'TEXT'}"
                if c.is_pk:
                    line += " PRIMARY KEY"
                if include_samples and c.samples:
                    line += f"  -- e.g. {', '.join(c.samples)}"
                lines.append(line)
            for local, ftable, fcol in t.foreign_keys:
                lines.append(
                    f"  FOREIGN KEY ({_quote(local)}) "
                    f"REFERENCES {_quote(ftable)}({_quote(fcol)})"
                )
            out.append(",\n".join(lines))
            out.append(f");  -- {t.row_count:,} rows\n")
        return "\n".join(out)

    def join_paths(self) -> list[str]:
        """Foreign-key edges as readable join hints.

        QueryMind's hand-written schema listed its join paths explicitly and
        that was one of its better ideas -- joins are where text-to-SQL breaks
        (CLAUDE.md section 6 lists n_tables_referenced as a top failure signal).
        """
        return [
            f"{t.name}.{local} -> {ftable}.{fcol}"
            for t in self.tables
            for local, ftable, fcol in t.foreign_keys
        ]


def _quote(name: str) -> str:
    return f"`{name}`" if not name.isidentifier() else name


def introspect(db_path: str | Path, sample_values: int = SAMPLE_VALUES) -> Schema:
    """Read a SQLite database's structure. Opens read-only."""
    db_path = Path(db_path)
    schema = Schema(db_id=db_path.stem)
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.text_factory = lambda b: b.decode("utf-8", "replace")
    try:
        names = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        for tname in names:
            table = Table(name=tname)
            for _, cname, ctype, _, _, pk in conn.execute(
                f'PRAGMA table_info("{tname}")'
            ):
                col = Column(name=cname, type=ctype, is_pk=bool(pk))
                if sample_values:
                    col.samples = _samples(conn, tname, cname, sample_values)
                table.columns.append(col)

            for row in conn.execute(f'PRAGMA foreign_key_list("{tname}")'):
                # (id, seq, table, from, to, on_update, on_delete, match)
                ref_table, from_col, to_col = row[2], row[3], row[4]
                if from_col and ref_table:
                    table.foreign_keys.append((from_col, ref_table, to_col or from_col))

            try:
                table.row_count = conn.execute(
                    f'SELECT COUNT(*) FROM "{tname}"'
                ).fetchone()[0]
            except sqlite3.Error:
                table.row_count = 0

            schema.tables.append(table)
    finally:
        conn.close()
    return schema


def _samples(conn: sqlite3.Connection, table: str, column: str, n: int) -> list[str]:
    """A few distinct non-null values, truncated so wide text columns cannot
    dominate the prompt."""
    try:
        rows = conn.execute(
            f'SELECT DISTINCT "{column}" FROM "{table}" '
            f'WHERE "{column}" IS NOT NULL LIMIT {n}'
        ).fetchall()
    except sqlite3.Error:
        return []
    out = []
    for (v,) in rows:
        s = str(v)
        out.append(f"{s[:40]}..." if len(s) > 40 else s)
    return out


@lru_cache(maxsize=32)
def bird_schema(db_root: str | Path, db_id: str, sample_values: int = SAMPLE_VALUES) -> Schema:
    """Introspect one BIRD database by db_id.

    Cached: introspection costs up to 6.5s on the larger databases (the row
    counts are full table scans), and an eval run asks for the same 11 schemas
    hundreds of times. Schemas are immutable for the life of a run.
    """
    return introspect(Path(db_root) / db_id / f"{db_id}.sqlite", sample_values=sample_values)
