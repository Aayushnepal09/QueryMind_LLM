"""Review UI helper tests.

Covers the pure functions. The Streamlit rendering itself is exercised by
launching the app; these pin the logic that broke it.
"""

from __future__ import annotations

import pandas as pd
import pytest
from review_ui import _unique_columns


def test_unique_columns_leaves_distinct_names_alone():
    assert _unique_columns(["a", "b", "c"]) == ["a", "b", "c"]


def test_duplicate_columns_are_suffixed():
    """Regression: this crashed the page.

    `SELECT T1.element, T2.element FROM ...` returns two columns named
    `element`. Arrow rejects duplicates, so st.dataframe raised ValueError and
    took the whole app down on an otherwise valid query.
    """
    assert _unique_columns(["bond_type", "element", "element"]) == [
        "bond_type",
        "element",
        "element (2)",
    ]


def test_three_way_duplicates():
    assert _unique_columns(["x", "x", "x"]) == ["x", "x (2)", "x (3)"]


def test_empty_column_name_gets_a_placeholder():
    assert _unique_columns(["", ""]) == ["column", "column (2)"]


def test_result_of_unique_columns_is_accepted_by_pandas():
    """The point of the fix: the frame must actually build."""
    cols = _unique_columns(["bond_type", "element", "element"])
    df = pd.DataFrame([("single", "H", "O")], columns=cols)
    assert list(df.columns) == cols
    assert len(df) == 1


def test_duplicates_would_break_pyarrow_without_the_fix():
    """Shows the failure this guards against is real, not hypothetical."""
    pa = pytest.importorskip("pyarrow")
    df = pd.DataFrame([("single", "H", "O")], columns=["bond_type", "element", "element"])
    with pytest.raises(ValueError, match="Duplicate column names"):
        pa.Table.from_pandas(df)
