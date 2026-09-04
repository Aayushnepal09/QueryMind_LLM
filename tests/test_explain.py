import pytest

from sqlsentinel.explain import describe_confidence, explain


def test_count_star():
    assert "Counts how many records" in explain("SELECT COUNT(*) FROM schools").summary


def test_aggregates_read_naturally():
    assert "average" in explain("SELECT AVG(price) FROM product").summary
    assert "total" in explain("SELECT SUM(qty) FROM orders").summary
    assert "highest" in explain("SELECT MAX(score) FROM t").summary


def test_single_column():
    assert explain("SELECT city FROM customer").summary == "Lists the city"


def test_distinct_reads_correctly():
    """Regression: this produced 'Lists the unique the city'."""
    assert explain("SELECT DISTINCT city FROM customer").summary == "Lists the unique city"


def test_column_names_are_humanised():
    assert "product unit price" in explain("SELECT product_unit_price FROM p").summary


def test_camel_case_split():
    assert "status type" in explain("SELECT StatusType FROM schools").summary


def test_select_star():
    assert "every column" in explain("SELECT * FROM t").summary


def test_aggregate_word_inside_a_quoted_column_is_not_an_aggregate():
    """Regression: `Free Meal Count (K-12)` was read as a COUNT() call.

    BIRD schemas are full of column names containing SQL keywords, so keyword
    matching has to ignore quoted identifiers.
    """
    sql = "SELECT T2.School, T1.`Free Meal Count (K-12)` FROM frpm AS T1 JOIN schools AS T2"
    summary = explain(sql).summary
    assert summary.startswith("Lists")
    assert "Counts" not in summary


def test_tables_are_named():
    assert "schools records" in explain("SELECT a FROM schools").as_text()


def test_join_lists_both_tables():
    text = explain("SELECT a FROM frpm JOIN schools ON x=y").as_text()
    assert "frpm" in text and "schools" in text


def test_filters_are_described_in_words():
    text = explain("SELECT a FROM t WHERE County = 'Alameda'").as_text()
    assert "county" in text and "is" in text and "Alameda" in text


def test_numeric_comparison_operators():
    assert "is greater than" in explain("SELECT a FROM t WHERE n > 5").as_text()
    assert "is at most" in explain("SELECT a FROM t WHERE n <= 5").as_text()


def test_ordering_and_limit():
    text = explain("SELECT a FROM t ORDER BY a DESC LIMIT 5").as_text()
    assert "highest first" in text
    assert "top 5" in text


def test_group_by():
    assert "grouped by" in explain("SELECT a, COUNT(*) FROM t GROUP BY a").as_text()


# ---------------------------------------------------------------- warnings


def test_unfiltered_query_warns():
    assert any("no filter" in w for w in explain("SELECT * FROM customer").warnings)


def test_count_without_filter_does_not_warn():
    """An aggregate over everything is normal, not a runaway scan."""
    assert not explain("SELECT COUNT(*) FROM customer").warnings


def test_many_table_join_warns():
    sql = "SELECT a FROM x JOIN y ON 1=1 JOIN z ON 1=1"
    assert any("record types" in w for w in explain(sql).warnings)


def test_subquery_warns():
    sql = "SELECT a FROM t WHERE b IN (SELECT c FROM u)"
    assert any("nested" in w for w in explain(sql).warnings)


def test_empty_sql_is_explained_not_crashed():
    e = explain("")
    assert "No query" in e.summary
    assert e.warnings


# ---------------------------------------------------------------- confidence wording


def test_unanimous_is_described_as_confident():
    text = describe_confidence(1.0, 3)
    assert "All 3 attempts produced the same answer" in text
    assert "confident" in text


def test_split_vote_is_described_as_unsure():
    text = describe_confidence(2 / 3, 3)
    assert "2 of 3" in text
    assert "unsure" in text


def test_total_disagreement():
    assert "different answers" in describe_confidence(0.0, 3)


def test_single_sample_wording():
    assert "single attempt" in describe_confidence(1.0, 1)


@pytest.mark.parametrize("conf", [0.0, 0.33, 0.5, 0.67, 1.0])
def test_confidence_wording_never_empty(conf):
    assert describe_confidence(conf, 3).strip()
