import pytest

from sqlsentinel.generator import extract_sql


def test_plain_sql():
    assert extract_sql("SELECT a FROM t") == "SELECT a FROM t"


def test_fenced():
    assert extract_sql("```sql\nSELECT a FROM t\n```") == "SELECT a FROM t"


def test_fence_without_language_tag():
    assert extract_sql("```\nSELECT a FROM t\n```") == "SELECT a FROM t"


def test_prose_before_fence():
    """The exact shape qwen2.5-coder produced in practice."""
    raw = (
        "To calculate the total revenue per product, you can use the following query:\n\n"
        "```sql\nSELECT p.name, SUM(o.qty) AS total\nFROM orders o\n"
        "JOIN product p ON p.id = o.pid\nGROUP BY p.name;\n```\n\n"
        "This query performs the following steps:\n"
        "1. Joins the tables.\n2. Sums the quantity.\n"
    )
    got = extract_sql(raw)
    assert got.startswith("SELECT p.name")
    assert "This query" not in got
    assert "1." not in got
    assert "```" not in got


def test_prose_before_bare_sql():
    raw = "Sure! Here is the query:\n\nSELECT a FROM t WHERE b = 1"
    assert extract_sql(raw) == "SELECT a FROM t WHERE b = 1"


def test_trailing_explanation_without_fence():
    raw = "SELECT a FROM t\n\nThis query returns column a."
    assert extract_sql(raw) == "SELECT a FROM t"


def test_with_cte_is_recognised():
    raw = "```sql\nWITH x AS (SELECT 1 AS n) SELECT n FROM x\n```"
    assert extract_sql(raw).startswith("WITH x AS")


def test_prefers_the_fence_containing_sql():
    """Models sometimes fence the schema before fencing the answer."""
    raw = (
        "First the schema:\n```\nCREATE TABLE t (a int);\n```\n"
        "And the query:\n```sql\nSELECT a FROM t\n```"
    )
    assert extract_sql(raw) == "SELECT a FROM t"


def test_only_first_statement_survives():
    assert extract_sql("SELECT a FROM t; DROP TABLE t;") == "SELECT a FROM t"


def test_whitespace_and_newlines_collapse():
    assert extract_sql("SELECT   a\n\n  FROM    t") == "SELECT a FROM t"


def test_trailing_semicolon_removed():
    assert extract_sql("SELECT a FROM t;") == "SELECT a FROM t"


@pytest.mark.parametrize("junk", ["", "I cannot answer that.", "Sorry!", "null"])
def test_no_sql_returns_empty_not_garbage(junk):
    """Never hand the executor something that is not SQL."""
    assert extract_sql(junk) == ""


def test_backticked_identifiers_preserved():
    raw = "SELECT `County Name` FROM frpm WHERE `Charter School (Y/N)` = 1"
    assert extract_sql(raw) == raw
