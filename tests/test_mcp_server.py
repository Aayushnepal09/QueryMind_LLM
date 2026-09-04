"""MCP server tests.

The tool functions are plain callables, so they are tested directly without a
protocol client. Server construction is tested separately and skipped when
fastmcp (an optional extra) is absent.
"""

from __future__ import annotations

import sqlite3

import pytest

from sqlsentinel import mcp_server


@pytest.fixture
def db_root(tmp_path, monkeypatch):
    root = tmp_path / "dev_databases" / "shop"
    root.mkdir(parents=True)
    conn = sqlite3.connect(root / "shop.sqlite")
    conn.executescript(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
        "INSERT INTO items VALUES (1,'a',1.0),(2,'b',2.0);"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(mcp_server, "DB_ROOT", root.parent)
    from sqlsentinel.schema_linker import bird_schema

    bird_schema.cache_clear()
    return root.parent


def test_list_databases(db_root):
    dbs = mcp_server.list_databases()
    assert len(dbs) == 1
    assert dbs[0]["db_id"] == "shop"
    assert "items" in dbs[0]["tables"]


def test_list_databases_empty_when_root_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(mcp_server, "DB_ROOT", tmp_path / "nope")
    assert mcp_server.list_databases() == []


def test_describe_database(db_root):
    ddl = mcp_server.describe_database("shop")
    assert "CREATE TABLE items" in ddl


def test_describe_unknown_database_is_helpful(db_root):
    msg = mcp_server.describe_database("nope")
    assert "Unknown database" in msg
    assert "list_databases" in msg


def test_run_sql_reads(db_root):
    r = mcp_server.run_sql("SELECT COUNT(*) FROM items", "shop")
    assert r["rows"] == [[2]]
    assert r["row_count"] == 1


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE items",
        "DELETE FROM items",
        "UPDATE items SET price=0",
        "INSERT INTO items VALUES (3,'c',3.0)",
        "SELECT 1; DROP TABLE items",
    ],
)
def test_run_sql_rejects_writes(db_root, sql):
    """The MCP surface must not become a way around the read-only executor."""
    r = mcp_server.run_sql(sql, "shop")
    assert "error" in r
    assert mcp_server.run_sql("SELECT COUNT(*) FROM items", "shop")["rows"] == [[2]]


def test_run_sql_unknown_database(db_root):
    assert "error" in mcp_server.run_sql("SELECT 1", "nope")


def test_run_sql_invalid_sql_returns_error(db_root):
    assert "error" in mcp_server.run_sql("SELECT missing FROM items", "shop")


def test_ask_unknown_database_is_unusable(db_root):
    r = mcp_server.ask("how many?", "nope")
    assert r["trust"] == "unusable"


def test_ask_reports_trust(db_root, monkeypatch, tmp_path):
    """Every answer must state how far it can be trusted."""
    from sqlsentinel.agent import Agent
    from sqlsentinel.llm import LLMResponse, ResponseCache, _BaseClient

    class Stub(_BaseClient):
        provider = "stub"

        def _call(self, system, user, temperature, max_tokens):
            return LLMResponse(
                text="```sql\nSELECT COUNT(*) FROM items\n```",
                model=self.model,
                provider=self.provider,
            )

    monkeypatch.setattr(
        mcp_server,
        "_agent",
        Agent(
            client=Stub("stub", ResponseCache(tmp_path / "c.db")),
            db_root=db_root,
            k=1,
        ),
    )
    from sqlsentinel.router import Router

    monkeypatch.setattr(mcp_server, "_router", Router(threshold=0.5))

    r = mcp_server.ask("how many items?", "shop")
    assert r["trust"] in {"high", "low", "unusable"}
    assert "confidence_in_words" in r
    assert r["sql"].lower().startswith("select")


@pytest.mark.parametrize("tool", ["list_databases", "describe_database", "ask", "run_sql"])
def test_tools_are_documented(tool):
    """Docstrings are the tool descriptions an assistant reads."""
    fn = getattr(mcp_server, tool)
    assert fn.__doc__ and len(fn.__doc__.strip()) > 20


def test_server_builds_with_all_tools():
    import asyncio
    import inspect

    pytest.importorskip("fastmcp")
    server = mcp_server.build_server()
    listed = server.list_tools()
    if inspect.iscoroutine(listed):
        listed = asyncio.run(listed)
    assert {t.name for t in listed} == {"list_databases", "describe_database", "ask", "run_sql"}


def test_server_instructions_warn_about_run_sql():
    pytest.importorskip("fastmcp")
    text = mcp_server.build_server().instructions or ""
    assert "trust" in text
    assert "bypasses the confidence layer" in text
