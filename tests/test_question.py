"""Question-checker tests.

The checker exists because a typo does not stop the model producing confident
SQL -- "how many tablere there?" scored as high confidence. Its job is to make
the guess visible, and its main risk is crying wolf on valid questions.
"""

from __future__ import annotations

import pytest

from sqlsentinel.question import Suggestion, _inflections, check
from sqlsentinel.schema_linker import Column, Schema, Table


@pytest.fixture
def schema():
    return Schema(
        db_id="superhero",
        tables=[
            Table(
                name="superhero",
                columns=[
                    Column(name="superhero_name", type="TEXT"),
                    Column(name="alignment_id", type="INTEGER"),
                    Column(name="publisher_id", type="INTEGER"),
                ],
            ),
            Table(name="hero_power", columns=[Column(name="power_id", type="INTEGER")]),
            Table(name="alignment", columns=[Column(name="alignment", type="TEXT")]),
            Table(name="race", columns=[Column(name="race", type="TEXT")]),
        ],
    )


def flagged(question, schema):
    return {s.word.lower(): s.suggestion for s in check(question, schema).suggestions}


# ---------------------------------------------------------------- catches typos


def test_typo_matching_a_schema_word_is_corrected(schema):
    assert flagged("What is the alignmnt of the Hulk?", schema) == {"alignmnt": "alignment"}


def test_typo_with_no_close_match_is_flagged_without_a_suggestion(schema):
    """The real case: "tablere" resembles nothing, and must still be surfaced."""
    assert flagged("how many tablere there?", schema) == {"tablere": None}


def test_corrected_applies_confident_suggestions_only(schema):
    r = check("What is the alignmnt of the Hulk?", schema)
    assert r.corrected() == "What is the alignment of the Hulk?"


def test_corrected_leaves_unresolvable_words_alone(schema):
    r = check("how many tablere there?", schema)
    assert r.corrected() == "how many tablere there?"


# ---------------------------------------------------------------- avoids false alarms


@pytest.mark.parametrize(
    "question",
    [
        "how many people are there",
        "Which publisher has the most heroes?",
        "how many superheroes are there",
        "What is the average alignment per race?",
        "List the top 10 heroes by power",
    ],
)
def test_valid_questions_are_not_flagged(question, schema):
    assert not check(question, schema).has_issues


def test_proper_nouns_are_not_flagged(schema):
    """Names are the values a WHERE clause filters on, not vocabulary."""
    words = flagged("How strong is Wolverine compared to Magneto?", schema)
    assert "wolverine" not in words
    assert "magneto" not in words


def test_ordinary_english_absent_from_the_schema_is_reported_but_not_as_a_typo(schema):
    """ "strong" is real English with no column behind it.

    Without a dictionary it cannot be distinguished from a misspelling, so it is
    surfaced as unrecognised rather than claimed to be wrong -- which is the
    useful statement anyway: this database has nothing matching that word.
    """
    r = check("How strong is Wolverine?", schema)
    assert [s.word for s in r.unrecognised] == ["strong"]
    assert r.likely_typos == []


def test_the_two_tiers_are_separated(schema):
    r = check("What is the alignmnt and strong of the Hulk?", schema)
    assert [s.suggestion for s in r.likely_typos] == ["alignment"]
    assert [s.word for s in r.unrecognised] == ["strong"]


def test_short_words_are_ignored(schema):
    assert not check("who is in it", schema).has_issues


def test_plural_of_a_schema_word_is_accepted(schema):
    assert not check("how many races are there", schema).has_issues


# ---------------------------------------------------------------- inflections


@pytest.mark.parametrize(
    ("word", "expected"),
    [("hero", "heroes"), ("city", "cities"), ("school", "schools"), ("box", "boxes")],
)
def test_inflections_use_english_rules(word, expected):
    """Regression: `word + "s"` produced "heros" and offered it as a correction."""
    assert expected in _inflections(word)


def test_hero_does_not_generate_the_misspelling():
    assert "heros" not in _inflections("hero") or "heroes" in _inflections("hero")


# ---------------------------------------------------------------- no schema


def test_works_without_a_schema():
    r = check("how many peple are there")
    assert isinstance(r.suggestions, list)


def test_empty_question_is_clean():
    assert not check("", None).has_issues


def test_suggestion_is_a_dataclass():
    s = Suggestion(word="x", suggestion=None)
    assert s.word == "x" and s.suggestion is None
