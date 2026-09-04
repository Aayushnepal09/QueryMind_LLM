"""Check a question for words the database will not recognise.

A typo does not stop the model producing confident SQL. Asked "how many tablere
there?", the agent returned an answer and all samples agreed, so it scored as
high confidence -- the model silently guessed at what was meant and the user had
no way to see that a guess had been made.

This flags the guess before the answer is trusted. It is deliberately not a
general spell checker and not an LLM call:

  * The vocabulary is the *schema itself* plus ordinary question words, so a
    word is "unrecognised" when it matches nothing in the database being
    queried. That is the definition that matters here -- `alignment` is a real
    word to this checker only because the superhero database has that column.
  * Suggestions come from difflib against that vocabulary, so they are
    deterministic and cheap.
  * Nothing is ever rewritten silently. The caller shows the suggestion and the
    user decides. Quietly "fixing" a question would replace one invisible guess
    with another.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field

# Ordinary words a question is built from. Not in the schema, but not typos
# either, so they must not be flagged.
COMMON_WORDS = {
    "a",
    "about",
    "above",
    "across",
    "after",
    "all",
    "also",
    "among",
    "and",
    "any",
    "are",
    "as",
    "at",
    "average",
    "be",
    "been",
    "before",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "compared",
    "count",
    "did",
    "do",
    "does",
    "each",
    "every",
    "find",
    "first",
    "for",
    "from",
    "get",
    "give",
    "greater",
    "group",
    "had",
    "has",
    "have",
    "highest",
    "how",
    "identify",
    "if",
    "in",
    "insert",
    "into",
    "is",
    "it",
    "its",
    "largest",
    "last",
    "least",
    "less",
    "like",
    "list",
    "lowest",
    "make",
    "many",
    "max",
    "maximum",
    "mean",
    "median",
    "min",
    "minimum",
    "more",
    "most",
    "much",
    "name",
    "named",
    "no",
    "not",
    "number",
    "of",
    "on",
    "one",
    "only",
    "or",
    "order",
    "over",
    "people",
    "per",
    "percent",
    "percentage",
    "please",
    "provide",
    "range",
    "rank",
    "ratio",
    "returns",
    "row",
    "rows",
    "same",
    "show",
    "smallest",
    "some",
    "sort",
    "state",
    "sum",
    "than",
    "that",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "top",
    "total",
    "under",
    "up",
    "was",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "whose",
    "why",
    "with",
    "within",
    "without",
    "would",
    "year",
    "years",
}

MIN_LENGTH = 4  # shorter tokens produce noisy suggestions
SIMILARITY = 0.72


@dataclass
class Suggestion:
    word: str
    suggestion: str | None  # None when nothing close enough was found


@dataclass
class ValueMatch:
    """A question word that names a *value* in the data, not a column.

    "who is the strongest?" contains no column called `strongest`, but the
    `attribute_name` column holds the value `Strength`. Reporting that is
    useful; reporting "no column matches strongest" is noise.
    """

    word: str
    value: str
    column: str


@dataclass
class QuestionCheck:
    question: str
    suggestions: list[Suggestion] = field(default_factory=list)
    value_matches: list[ValueMatch] = field(default_factory=list)

    @property
    def likely_typos(self) -> list[Suggestion]:
        """Words with a close schema match -- confidently a misspelling."""
        return [s for s in self.suggestions if s.suggestion]

    @property
    def unrecognised(self) -> list[Suggestion]:
        """Words matching nothing in this database.

        Deliberately *not* called typos. Without an English dictionary there is
        no way to tell "tablere" from "strong": both are absent from the
        superhero schema, one is a misspelling and one is ordinary English that
        simply has no column behind it. Saying "this database has nothing
        matching that word" is true of both, and is the useful thing to tell
        someone -- it explains why the model had to guess.
        """
        return [s for s in self.suggestions if not s.suggestion]

    @property
    def has_issues(self) -> bool:
        return bool(self.suggestions)

    def corrected(self) -> str:
        """The question with every confident suggestion applied.

        Offered to the caller to *display*; applying it is the user's choice.
        """
        out = self.question
        for s in self.suggestions:
            if s.suggestion:
                out = re.sub(rf"\b{re.escape(s.word)}\b", s.suggestion, out, flags=re.I)
        return out


def _split_identifier(name: str) -> list[str]:
    """`superhero_name` / `CDSCode` -> individual lowercase words."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", name)
    return [w.lower() for w in re.split(r"[^A-Za-z0-9]+", spaced) if w]


def _inflections(word: str) -> set[str]:
    """Plural and singular forms of a schema word.

    English rules, not `word + "s"`. The naive version generated "heros" from
    "hero", and difflib then happily offered that misspelling as a *correction*
    for the correctly-spelled "heroes" -- a checker that introduces typos is
    worse than none.
    """
    forms = {word}
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        forms.add(word[:-1] + "ies")
    elif word.endswith(("s", "x", "z", "ch", "sh", "o")):
        forms.add(word + "es")
        forms.add(word + "s")
    else:
        forms.add(word + "s")

    # and the singular, for a schema that names its tables in the plural
    if word.endswith("ies") and len(word) > 3:
        forms.add(word[:-3] + "y")
    elif word.endswith("es") and len(word) > 2:
        forms.add(word[:-2])
        forms.add(word[:-1])
    elif word.endswith("s") and len(word) > 1:
        forms.add(word[:-1])
    return forms


def schema_vocabulary(schema) -> set[str]:
    """Every word appearing in a table or column name, with its inflections.

    Inflections matter because people ask about "schools" when the table is
    `school`; without them every such question would be flagged.
    """
    vocab: set[str] = set()
    for table in schema.tables:
        vocab.update(_split_identifier(table.name))
        for col in table.columns:
            vocab.update(_split_identifier(col.name))

    for word in list(vocab):
        vocab |= _inflections(word)
    return vocab


# Comparatives and superlatives: "strongest" and "stronger" both point at
# "strength". Stripped before matching so the question's grammar does not hide
# the concept the data actually holds.
_SUFFIXES = ("est", "er", "ing", "ed", "s")


def _stem(word: str) -> str:
    for suffix in _SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[: -len(suffix)]
    return word


def value_index(schema) -> dict[str, tuple[str, str]]:
    """Map words appearing in sample data values to (value, column).

    The schema carries a few example values per column. Those are what a
    question usually refers to -- people ask about "Marvel" and "female" and
    "strength", none of which are column names.
    """
    index: dict[str, tuple[str, str]] = {}
    for table in schema.tables:
        for col in table.columns:
            for sample in col.samples:
                for word in re.findall(r"[A-Za-z]{3,}", str(sample)):
                    key = word.lower()
                    index.setdefault(key, (str(sample), f"{table.name}.{col.name}"))
                    index.setdefault(_stem(key), (str(sample), f"{table.name}.{col.name}"))
    return index


def check(question: str, schema=None) -> QuestionCheck:
    """Flag words the database cannot account for, and explain the ones it can."""
    vocab = set(COMMON_WORDS)
    values: dict[str, tuple[str, str]] = {}
    if schema is not None:
        vocab |= schema_vocabulary(schema)
        values = value_index(schema)

    result = QuestionCheck(question=question)
    seen: set[str] = set()

    for raw in re.findall(r"[A-Za-z][A-Za-z'-]*", question):
        word = raw.lower()
        if (
            word in vocab
            or word in seen
            or len(word) < MIN_LENGTH
            or any(ch.isdigit() for ch in word)
        ):
            continue
        seen.add(word)

        # Proper nouns are values, not vocabulary: "Alameda" and "Hulk" are
        # exactly what a WHERE clause filters on and must not be flagged.
        if raw[0].isupper() and raw != question.split()[0]:
            continue

        # A stemmed form may still be schema vocabulary: "powers" -> "power".
        stem = _stem(word)
        if stem in vocab:
            continue

        # Does it name a value in the data rather than a column? That is an
        # explanation, not a problem, so it is reported separately.
        hit = values.get(word) or values.get(stem)
        if not hit:
            # Looser than the typo cutoff on purpose: "strong" against
            # "strength" scores 0.71, and relating the two is exactly the job.
            near = difflib.get_close_matches(stem, values.keys(), n=1, cutoff=0.68)
            hit = values.get(near[0]) if near else None
        if hit:
            result.value_matches.append(ValueMatch(word=raw, value=hit[0], column=hit[1]))
            continue

        match = difflib.get_close_matches(word, vocab, n=1, cutoff=SIMILARITY)
        result.suggestions.append(Suggestion(word=raw, suggestion=match[0] if match else None))

    return result


def describe_contents(schema, per_column: int = 3, max_columns: int = 6) -> list[str]:
    """A short account of what this database actually holds.

    Shown when a question uses a word the database cannot account for. Telling
    someone "nothing matches 'fastest'" leaves them stuck; showing them that the
    data has attribute names of Intelligence, Strength and Speed lets them
    rephrase and get an answer. Lexical matching will never connect "fastest"
    to "Speed" -- that is semantic -- so the honest fallback is to show the
    vocabulary and let the person make the leap.
    """
    out: list[str] = []
    for table in schema.tables:
        for col in table.columns:
            values = [
                s
                for s in col.samples
                if s and not s.replace(".", "").replace("-", "").isdigit() and len(s) > 1
            ][:per_column]
            if len(values) >= 2:
                out.append(f"**{col.name}**: {', '.join(values)}")
            if len(out) >= max_columns:
                return out
    return out
