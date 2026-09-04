"""API tests with a stub LLM, so no model or network is involved."""

from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from sqlsentinel import api as api_module
from sqlsentinel.agent import Agent
from sqlsentinel.llm import LLMResponse, ResponseCache, _BaseClient
from sqlsentinel.router import Router
from sqlsentinel.schema_linker import bird_schema


class StubClient(_BaseClient):
    provider = "stub"

    def __init__(self, sql: str, cache):
        super().__init__("stub-model", cache)
        self.sql = sql

    def _call(self, system, user, temperature, max_tokens):
        return LLMResponse(
            text=f"```sql\n{self.sql}\n```",
            model=self.model,
            provider=self.provider,
            prompt_tokens=100,
            output_tokens=10,
        )


@pytest.fixture
def db_root(tmp_path):
    root = tmp_path / "dev_databases" / "shop"
    root.mkdir(parents=True)
    conn = sqlite3.connect(root / "shop.sqlite")
    conn.executescript(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
        "INSERT INTO items VALUES (1,'a',1.0),(2,'b',2.0);"
    )
    conn.commit()
    conn.close()
    return root.parent


def make_client(monkeypatch, tmp_path, db_root, sql: str, threshold: float = 0.5):
    bird_schema.cache_clear()
    monkeypatch.setattr(api_module, "DB_ROOT", db_root)
    cache = ResponseCache(tmp_path / "c.db")
    stub = StubClient(sql, cache)

    def fake_lifespan_state():
        api_module._state["agent"] = Agent(client=stub, db_root=db_root, k=1)
        api_module._state["router"] = Router(threshold=threshold)
        api_module._state["tracer"] = None

    client = TestClient(api_module.app)
    client.__enter__()
    fake_lifespan_state()  # override whatever lifespan built
    api_module._review_queue.clear()
    return client


def test_health(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT 1")
    r = c.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["databases"] == 1


def test_unknown_database_is_404(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT 1")
    r = c.post("/query", json={"question": "how many?", "db_id": "nope"})
    assert r.status_code == 404


def test_confident_query_auto_executes(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT COUNT(*) FROM items", threshold=0.5)
    r = c.post("/query", json={"question": "how many items?", "db_id": "shop", "k": 1})
    assert r.status_code == 200
    body = r.json()
    assert body["decision"] == "AUTO"
    assert body["rows"] == [[2]]
    assert body["review_id"] is None
    assert body["trace_id"]


def test_response_carries_cost_and_latency(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT COUNT(*) FROM items")
    body = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()
    assert body["latency_ms"] > 0
    assert body["cost_usd"] >= 0


def test_broken_sql_is_routed_to_review(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT nope FROM missing")
    body = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()
    assert body["decision"] == "REVIEW"
    assert body["review_id"]
    assert body["rows"] is None


def test_review_queue_lists_pending(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT nope FROM missing")
    c.post("/query", json={"question": "q", "db_id": "shop", "k": 1})
    q = c.get("/review/queue").json()
    assert len(q) == 1
    assert q[0]["reasons"]


def test_review_approve_after_edit(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT nope FROM missing")
    rid = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()["review_id"]
    r = c.post(
        f"/review/{rid}", json={"action": "edit", "edited_sql": "SELECT COUNT(*) FROM items"}
    )
    assert r.status_code == 200
    assert r.json()["row_count"] == 1
    # consumed
    assert c.get("/review/queue").json() == []


def test_review_reject_consumes_without_executing(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT nope FROM missing")
    rid = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()["review_id"]
    body = c.post(f"/review/{rid}", json={"action": "reject"}).json()
    assert body["action"] == "reject" and body["executed"] is False


def test_unknown_review_id_is_404(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT 1")
    assert c.post("/review/deadbeef", json={"action": "approve"}).status_code == 404


def test_edit_without_sql_is_400(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT nope FROM missing")
    rid = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()["review_id"]
    assert c.post(f"/review/{rid}", json={"action": "edit"}).status_code == 400


def test_empty_question_rejected_by_validation(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT 1")
    assert c.post("/query", json={"question": "", "db_id": "shop"}).status_code == 422


def test_k_is_bounded(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT 1")
    assert c.post("/query", json={"question": "q", "db_id": "shop", "k": 99}).status_code == 422


# ---------------------------------------------------------------- scorer selection


def test_health_reports_which_scorer_is_active(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT 1")
    assert c.get("/health").json()["scorer"] == "agreement"


def test_query_reports_the_scorer_it_used(monkeypatch, tmp_path, db_root):
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT COUNT(*) FROM items")
    body = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()
    assert body["scorer"] == "agreement"


def test_calibrated_model_is_used_when_present(monkeypatch, tmp_path, db_root):
    """A trained scorer must actually reach production, not just results/."""
    import numpy as np

    from sqlsentinel.confidence import ConfidenceModel, QueryFeatures

    rng = np.random.default_rng(0)
    feats, correct = [], []
    for _ in range(120):
        a = rng.uniform(0, 1)
        feats.append(QueryFeatures(agreement_rate=a))
        correct.append(int(rng.uniform() < a))
    ConfidenceModel().fit(feats, correct, question_ids=list(range(120))).save(
        tmp_path / "model.pkl"
    )

    monkeypatch.setenv("CONFIDENCE_MODEL", str(tmp_path / "model.pkl"))
    c = make_client(monkeypatch, tmp_path, db_root, "SELECT COUNT(*) FROM items")
    api_module._state["confidence_model"] = api_module._load_confidence_model()

    body = c.post("/query", json={"question": "q", "db_id": "shop", "k": 1}).json()
    assert body["scorer"] == "calibrated"
    assert 0.0 <= body["confidence"] <= 1.0


def test_unreadable_model_falls_back_rather_than_failing(monkeypatch, tmp_path):
    """A corrupt model file must not take the service down."""
    bad = tmp_path / "bad.pkl"
    bad.write_text("not a pickle", encoding="utf-8")
    monkeypatch.setenv("CONFIDENCE_MODEL", str(bad))
    assert api_module._load_confidence_model() is None


def test_missing_model_falls_back_to_agreement(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIDENCE_MODEL", str(tmp_path / "nope.pkl"))
    assert api_module._load_confidence_model() is None
