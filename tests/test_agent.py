"""Agent orchestration tests.

Covers the two pieces of logic that are easy to get subtly wrong and hard to
notice: self-consistency voting (which must compare executed results, not SQL
text) and the self-correction loop (which must preserve the pre-correction
error signal that the confidence model depends on).
"""

from __future__ import annotations

import sqlite3

import pytest

from sqlsentinel.agent import Agent
from sqlsentinel.llm import LLMResponse, ResponseCache, _BaseClient
from sqlsentinel.schema_linker import bird_schema


@pytest.fixture
def db_root(tmp_path):
    root = tmp_path / "dev_databases" / "shop"
    root.mkdir(parents=True)
    conn = sqlite3.connect(root / "shop.sqlite")
    conn.executescript(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
        "INSERT INTO items VALUES (1,'a',1.0),(2,'b',2.0),(3,'c',3.0);"
    )
    conn.commit()
    conn.close()
    bird_schema.cache_clear()
    return root.parent


class ScriptedClient(_BaseClient):
    """Returns a fixed sequence of responses, cycling if exhausted."""

    provider = "scripted"

    def __init__(self, replies: list[str], cache):
        super().__init__("scripted-model", cache)
        self.replies = replies
        self.calls = 0

    def _call(self, system, user, temperature, max_tokens):
        text = self.replies[min(self.calls, len(self.replies) - 1)]
        self.calls += 1
        return LLMResponse(text=text, model=self.model, provider=self.provider)


def make_agent(db_root, tmp_path, replies, **kw):
    return Agent(
        client=ScriptedClient(replies, ResponseCache(tmp_path / "c.db")),
        db_root=db_root,
        **kw,
    )


QUESTION = {"question_id": 1, "db_id": "shop", "question": "how many?", "evidence": ""}


# ---------------------------------------------------------------- single shot


def test_single_sample_returns_the_only_candidate(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["```sql\nSELECT COUNT(*) FROM items\n```"], k=1)
    t = a.predict_one(QUESTION)
    assert t.sql == "SELECT COUNT(*) FROM items"
    assert t.executed_ok
    assert t.agreement_rate == 1.0


def test_unparseable_response_yields_an_empty_prediction(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["I cannot answer that."], k=1)
    t = a.predict_one(QUESTION)
    assert t.empty and t.sql == ""


def test_schema_table_count_is_recorded(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT 1"], k=1)
    assert a.predict_one(QUESTION).n_tables_in_prompt == 1


# ---------------------------------------------------------------- voting


def test_agreement_is_computed_over_results_not_sql_text(db_root, tmp_path):
    """Two differently-written queries returning the same rows are one answer.

    This is the equivalence BIRD scores on, so voting must use it too --
    comparing SQL strings would count these as a disagreement and destroy the
    confidence signal.
    """
    a = make_agent(
        db_root,
        tmp_path,
        [
            "SELECT COUNT(*) FROM items",
            "SELECT COUNT(id) FROM items",  # same result, different text
            "SELECT COUNT(*) FROM items WHERE id > 0",  # same again
        ],
        k=3,
    )
    t = a.predict_one(QUESTION)
    assert t.agreement_rate == 1.0
    assert t.n_candidates == 3


def test_split_vote_lowers_agreement(db_root, tmp_path):
    a = make_agent(
        db_root,
        tmp_path,
        [
            "SELECT COUNT(*) FROM items",
            "SELECT COUNT(*) FROM items",
            "SELECT price FROM items",  # different result
        ],
        k=3,
    )
    t = a.predict_one(QUESTION)
    assert t.agreement_rate == pytest.approx(2 / 3)


def test_modal_result_is_the_one_returned(db_root, tmp_path):
    a = make_agent(
        db_root,
        tmp_path,
        [
            "SELECT price FROM items",
            "SELECT COUNT(*) FROM items",
            "SELECT COUNT(*) FROM items",
        ],
        k=3,
    )
    t = a.predict_one(QUESTION)
    assert "COUNT" in t.sql.upper()


def test_all_candidates_failing_gives_zero_agreement(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT nope FROM missing"], k=3)
    t = a.predict_one(QUESTION)
    assert t.agreement_rate == 0.0
    assert not t.executed_ok


# ---------------------------------------------------------------- correction


def test_correction_repairs_a_broken_query(db_root, tmp_path):
    a = make_agent(
        db_root,
        tmp_path,
        ["SELECT nope FROM items", "SELECT COUNT(*) FROM items"],
        k=1,
        max_corrections=2,
    )
    t = a.predict_one(QUESTION)
    assert t.n_correction_rounds == 1
    assert t.executed_ok


def test_correction_preserves_the_pre_correction_error_signal(db_root, tmp_path):
    """The confidence model depends on knowing the first attempt failed.

    Self-correction improves executability without improving correctness
    (see results/technique-notes.md), so a repaired query is *less* trustworthy
    than one that worked first time. Losing this flag would hide that.
    """
    a = make_agent(
        db_root,
        tmp_path,
        ["SELECT nope FROM items", "SELECT COUNT(*) FROM items"],
        k=1,
        max_corrections=2,
    )
    t = a.predict_one(QUESTION)
    assert t.execution_errored_pre_correction is True
    assert t.executed_ok is True  # and it succeeded afterwards


def test_correction_is_capped(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT nope FROM items"], k=1, max_corrections=2)
    t = a.predict_one(QUESTION)
    assert t.n_correction_rounds == 2
    assert not t.executed_ok


def test_no_correction_when_disabled(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT nope FROM items"], k=1, max_corrections=0)
    t = a.predict_one(QUESTION)
    assert t.n_correction_rounds == 0
    assert t.execution_errored_pre_correction


# ---------------------------------------------------------------- batch + summary


def test_predict_returns_one_prediction_per_question(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT COUNT(*) FROM items"], k=1)
    qs = [{**QUESTION, "question_id": i} for i in range(4)]
    assert set(a.predict(qs)) == {0, 1, 2, 3}


def test_predict_with_workers_covers_every_question(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT COUNT(*) FROM items"], k=1)
    qs = [{**QUESTION, "question_id": i} for i in range(6)]
    assert len(a.predict(qs, workers=3)) == 6


def test_summary_reports_the_run(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT COUNT(*) FROM items"], k=1)
    a.predict([{**QUESTION, "question_id": i} for i in range(3)])
    s = a.summary()
    assert s["n"] == 3
    assert s["executed_ok"] == 3
    assert s["empty_predictions"] == 0


def test_few_shot_without_a_store_is_rejected_at_construction(db_root):
    with pytest.raises(ValueError, match="few_shot requires"):
        Agent(client=None, db_root=db_root, few_shot=3)


def test_evidence_can_be_ablated(db_root, tmp_path):
    a = make_agent(db_root, tmp_path, ["SELECT COUNT(*) FROM items"], k=1, use_evidence=False)
    q = {**QUESTION, "evidence": "SHOULD NOT APPEAR"}
    a.predict_one(q)
    schema = bird_schema(str(db_root), "shop")
    assert "SHOULD NOT APPEAR" not in a.generator.build_prompt(q["question"], schema, "")
