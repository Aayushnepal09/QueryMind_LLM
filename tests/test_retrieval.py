import pytest

from sqlsentinel.retrieval import ExemplarStore, render_exemplars


@pytest.fixture
def records():
    return [
        {"question_id": 1, "db_id": "shop", "question": "How many orders were placed?",
         "SQL": "SELECT COUNT(*) FROM orders", "evidence": ""},
        {"question_id": 2, "db_id": "shop", "question": "How many customers are there?",
         "SQL": "SELECT COUNT(*) FROM customers", "evidence": "customers table"},
        {"question_id": 3, "db_id": "school", "question": "List the schools in Alameda county",
         "SQL": "SELECT name FROM schools WHERE county='Alameda'", "evidence": ""},
        {"question_id": 4, "db_id": "school", "question": "Which county has the most schools?",
         "SQL": "SELECT county FROM schools GROUP BY county", "evidence": ""},
    ]


def test_retrieves_by_similarity(records):
    hits = ExemplarStore(records).retrieve("How many orders exist?", k=2)
    assert hits[0].question == "How many orders were placed?"
    assert hits[0].similarity > hits[1].similarity


def test_respects_k(records):
    assert len(ExemplarStore(records).retrieve("orders", k=3)) == 3


# ---------------------------------------------------------------- leakage guard


def test_self_retrieval_leaks_the_answer_without_the_guard(records):
    """Documents the bug this guard exists for.

    Scoring a question that is itself in the exemplar pool retrieves it at
    similarity 1.0 and hands the model its own gold SQL. Measured at +23.5
    points of contamination on the calibration split before the fix.
    """
    hit = ExemplarStore(records).retrieve("How many orders were placed?", k=1)[0]
    assert hit.similarity == pytest.approx(1.0)
    assert hit.sql == "SELECT COUNT(*) FROM orders"


def test_exclude_question_id_removes_the_self_match(records):
    hits = ExemplarStore(records).retrieve(
        "How many orders were placed?", k=2, exclude_question_id=1
    )
    assert all(h.question != "How many orders were placed?" for h in hits)
    assert all(h.similarity < 1.0 for h in hits)


def test_excluding_an_absent_id_is_harmless(records):
    assert len(ExemplarStore(records).retrieve("orders", k=2, exclude_question_id=999)) == 2


def test_exclusion_composes_with_same_db_only(records):
    hits = ExemplarStore(records, same_db_only=True).retrieve(
        "How many orders were placed?", k=1, db_id="shop", exclude_question_id=1
    )
    assert hits[0].db_id == "shop"
    assert hits[0].question != "How many orders were placed?"


# ---------------------------------------------------------------- db scoping


def test_same_db_only_restricts_pool(records):
    hits = ExemplarStore(records, same_db_only=True).retrieve(
        "How many schools?", k=2, db_id="school"
    )
    assert all(h.db_id == "school" for h in hits)


def test_same_db_only_falls_back_when_db_has_no_examples(records):
    hits = ExemplarStore(records, same_db_only=True).retrieve("anything", k=1, db_id="unknown")
    assert len(hits) == 1


def test_cross_db_allowed_by_default(records):
    store = ExemplarStore(records)
    dbs = {h.db_id for h in store.retrieve("How many schools are there?", k=4)}
    assert len(dbs) > 1


# ---------------------------------------------------------------- rendering


def test_render_includes_question_and_sql(records):
    text = render_exemplars(ExemplarStore(records).retrieve("orders", k=1))
    assert "-- Question:" in text
    assert "SELECT" in text


def test_render_includes_evidence_when_present(records):
    text = render_exemplars(ExemplarStore(records).retrieve("How many customers?", k=1))
    assert "-- Knowledge: customers table" in text


def test_render_empty_is_empty_string():
    assert render_exemplars([]) == ""


def test_empty_pool_rejected():
    with pytest.raises(ValueError, match="empty"):
        ExemplarStore([]).retrieve("x", k=1)
