"""Read-only SQL execution behind a dialect boundary.

CLAUDE.md section 4: read-only, hard timeout, dialect-agnostic interface with
SQLite and Postgres implementations. BIRD ships SQLite; QueryMind's retained
path is Postgres.

Read-only is enforced in three independent layers, because any one of them can
be defeated on its own:

  1. Statement inspection -- reject anything that is not a single SELECT/WITH.
  2. Connection mode -- SQLite opens with `?mode=ro`; Postgres sets the
     transaction read-only.
  3. Timeout -- a hard wall-clock cap so a pathological query cannot hang the
     eval loop.

The original QueryMind executed generated SQL straight against production
Postgres with none of these (see docs/migration-notes.md, defect 2).
"""

from __future__ import annotations

import contextlib
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

DEFAULT_TIMEOUT_S = 5.0
MAX_ROWS = 5000

# Anything that could mutate data or schema. Checked against the statement even
# though the connection is read-only, so a rejection is explainable rather than
# surfacing as a driver-level error.
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|GRANT|REVOKE|"
    r"ATTACH|DETACH|PRAGMA|VACUUM|REINDEX)\b",
    re.IGNORECASE,
)


class UnsafeQueryError(ValueError):
    """The statement is not a plain read."""


@dataclass
class ExecutionResult:
    rows: list[tuple] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    error: str | None = None
    timed_out: bool = False
    duration_s: float = 0.0
    truncated: bool = False

    @property
    def ok(self) -> bool:
        return self.error is None and not self.timed_out

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def normalized(self) -> frozenset:
        """Order-insensitive view of the result set, for candidate agreement.

        BIRD scores by set equality, so self-consistency agreement (CLAUDE.md
        section 6) must compare the same way: row order and column order carry
        no meaning. Cells are stringified because the same value can come back
        as int or float depending on how the query was phrased.
        """
        if not self.ok:
            return frozenset()
        return frozenset(tuple(sorted(str(c) for c in row)) for row in self.rows)


def assert_read_only(sql: str) -> None:
    """Raise UnsafeQueryError unless `sql` is a single read statement."""
    stripped = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql, flags=re.S).strip().rstrip(";")
    if not stripped:
        raise UnsafeQueryError("empty statement")
    if ";" in stripped:
        raise UnsafeQueryError("multiple statements are not allowed")
    if not re.match(r"^\s*(SELECT|WITH)\b", stripped, re.IGNORECASE):
        raise UnsafeQueryError(f"not a read: starts with {stripped.split()[0]!r}")
    if m := _FORBIDDEN.search(stripped):
        raise UnsafeQueryError(f"forbidden keyword {m.group(0).upper()!r}")


class Executor(Protocol):
    def execute(self, sql: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> ExecutionResult: ...


class SQLiteExecutor:
    """Read-only SQLite executor. The BIRD evaluation path."""

    def __init__(self, db_path: str | Path, max_rows: int = MAX_ROWS):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(self.db_path)
        self.max_rows = max_rows

    def execute(self, sql: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> ExecutionResult:
        import time

        try:
            assert_read_only(sql)
        except UnsafeQueryError as e:
            return ExecutionResult(error=f"rejected: {e}")

        result = ExecutionResult()
        done = threading.Event()
        t0 = time.time()

        def run() -> None:
            conn = None
            try:
                # mode=ro is enforced by SQLite itself, not just by our check
                conn = sqlite3.connect(
                    f"file:{self.db_path.as_posix()}?mode=ro",
                    uri=True, timeout=timeout_s, check_same_thread=False,
                )
                conn.text_factory = lambda b: b.decode("utf-8", "replace")
                cur = conn.execute(sql)
                rows = cur.fetchmany(self.max_rows + 1)
                if len(rows) > self.max_rows:
                    rows, result.truncated = rows[: self.max_rows], True
                result.rows = rows
                result.columns = [d[0] for d in (cur.description or [])]
            except Exception as e:
                result.error = f"{type(e).__name__}: {e}"
            finally:
                if conn is not None:
                    # interrupt() races with a query that already finished
                    with contextlib.suppress(Exception):
                        conn.interrupt()
                    conn.close()
                done.set()

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        if not done.wait(timeout_s):
            # The daemon thread is abandoned; sqlite releases it when the
            # process exits. Never join, or a pathological query blocks the run.
            result.timed_out = True
            result.error = f"timeout after {timeout_s}s"
        result.duration_s = time.time() - t0
        return result


class PostgresExecutor:
    """Read-only Postgres executor. Retained QueryMind path, not used by BIRD."""

    def __init__(self, dsn: str, max_rows: int = MAX_ROWS):
        self.dsn = dsn
        self.max_rows = max_rows

    def execute(self, sql: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> ExecutionResult:
        import time

        try:
            assert_read_only(sql)
        except UnsafeQueryError as e:
            return ExecutionResult(error=f"rejected: {e}")

        import psycopg2

        result = ExecutionResult()
        t0 = time.time()
        conn = None
        try:
            conn = psycopg2.connect(self.dsn, connect_timeout=int(timeout_s) or 1)
            conn.set_session(readonly=True, autocommit=False)
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(timeout_s * 1000)}")
                cur.execute(sql)
                rows = cur.fetchmany(self.max_rows + 1)
                if len(rows) > self.max_rows:
                    rows, result.truncated = rows[: self.max_rows], True
                result.rows = rows
                result.columns = [d[0] for d in (cur.description or [])]
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            result.error = msg
            result.timed_out = "statement timeout" in msg.lower()
        finally:
            if conn is not None:
                conn.rollback()
                conn.close()
        result.duration_s = time.time() - t0
        return result


def bird_executor(db_root: str | Path, db_id: str, **kw: Any) -> SQLiteExecutor:
    """Executor for one BIRD database by its db_id."""
    return SQLiteExecutor(Path(db_root) / db_id / f"{db_id}.sqlite", **kw)
