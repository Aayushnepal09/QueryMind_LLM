import sqlite3

import pytest

from sqlsentinel.executor import (
    ExecutionResult,
    SQLiteExecutor,
    UnsafeQueryError,
    assert_read_only,
)


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "t.sqlite"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")
    conn.executemany("INSERT INTO t VALUES (?,?)", [(1, "x"), (2, "y"), (3, "z")])
    conn.commit()
    conn.close()
    return p


# ---------------------------------------------------------------- read-only gate


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE t",
        "DELETE FROM t",
        "UPDATE t SET a=1",
        "INSERT INTO t VALUES (4,'w')",
        "ALTER TABLE t ADD COLUMN c INT",
        "CREATE TABLE u (a INT)",
        "ATTACH DATABASE 'x' AS y",
        "PRAGMA table_info(t)",
        "VACUUM",
    ],
)
def test_writes_are_rejected(sql):
    with pytest.raises(UnsafeQueryError):
        assert_read_only(sql)


def test_stacked_statement_rejected():
    with pytest.raises(UnsafeQueryError, match="multiple statements"):
        assert_read_only("SELECT 1; DROP TABLE t")


def test_comment_hidden_write_still_rejected():
    """Comments are stripped before inspection, so they cannot mask a write."""
    with pytest.raises(UnsafeQueryError):
        assert_read_only("-- harmless\nDROP TABLE t")


def test_empty_rejected():
    with pytest.raises(UnsafeQueryError, match="empty"):
        assert_read_only("   ")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t",
        "select * from t where b='x'",
        "WITH c AS (SELECT 1 AS n) SELECT n FROM c",
        "SELECT a FROM t;",
    ],
)
def test_reads_allowed(sql):
    assert_read_only(sql)


# ---------------------------------------------------------------- execution


def test_successful_read(db):
    r = SQLiteExecutor(db).execute("SELECT a, b FROM t ORDER BY a")
    assert r.ok
    assert r.row_count == 3
    assert r.columns == ["a", "b"]


def test_write_blocked_at_execute_not_just_parse(db):
    r = SQLiteExecutor(db).execute("DELETE FROM t")
    assert not r.ok and "rejected" in r.error
    # and the data is untouched
    assert SQLiteExecutor(db).execute("SELECT COUNT(*) FROM t").rows == [(3,)]


def test_connection_itself_is_read_only(db):
    """Even if the statement gate were bypassed, the connection blocks writes."""
    conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO t VALUES (9,'q')")
    conn.close()


def test_invalid_sql_reports_error_without_raising(db):
    r = SQLiteExecutor(db).execute("SELECT nope FROM t")
    assert not r.ok and "no such column" in r.error.lower()


def test_missing_database_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        SQLiteExecutor(tmp_path / "nope.sqlite")


def test_row_cap_truncates(db):
    r = SQLiteExecutor(db, max_rows=2).execute("SELECT a FROM t")
    assert r.truncated and r.row_count == 2


# ---------------------------------------------------------------- normalization


def test_row_order_does_not_change_normalized_form():
    a = ExecutionResult(rows=[(1, "x"), (2, "y")])
    b = ExecutionResult(rows=[(2, "y"), (1, "x")])
    assert a.normalized() == b.normalized()


def test_numeric_string_equivalence():
    """1 and 1.0 come back differently depending on how the query was phrased."""
    assert ExecutionResult(rows=[(1,)]).normalized() == ExecutionResult(rows=[("1",)]).normalized()


def test_different_values_do_not_normalize_equal():
    assert ExecutionResult(rows=[(1,)]).normalized() != ExecutionResult(rows=[(2,)]).normalized()


def test_failed_result_normalizes_empty():
    assert ExecutionResult(error="boom").normalized() == frozenset()


# ---------------------------------------------------------------- postgres dsn


def test_dsn_from_env(monkeypatch):
    from sqlsentinel.executor import postgres_dsn_from_env

    for k, v in {
        "POSTGRES_USERNAME": "u", "POSTGRES_PASSWORD": "p",
        "POSTGRES_SERVER": "h:5432", "POSTGRES_DATABASE": "d",
    }.items():
        monkeypatch.setenv(k, v)
    assert postgres_dsn_from_env() == "postgresql://u:p@h:5432/d?sslmode=require"


def test_dsn_url_encodes_the_password(monkeypatch):
    """QueryMind's in-app copy of this did not, and broke on reserved chars."""
    from sqlsentinel.executor import postgres_dsn_from_env

    for k, v in {
        "POSTGRES_USERNAME": "u", "POSTGRES_PASSWORD": "p@ss/w:rd",
        "POSTGRES_SERVER": "h", "POSTGRES_DATABASE": "d",
    }.items():
        monkeypatch.setenv(k, v)
    dsn = postgres_dsn_from_env()
    assert "p%40ss%2Fw%3Ard" in dsn
    assert "p@ss/w:rd" not in dsn


def test_dsn_missing_vars_names_them(monkeypatch):
    from sqlsentinel.executor import postgres_dsn_from_env

    for k in ("POSTGRES_USERNAME", "POSTGRES_PASSWORD", "POSTGRES_SERVER", "POSTGRES_DATABASE"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(ValueError, match="POSTGRES_USERNAME"):
        postgres_dsn_from_env()
