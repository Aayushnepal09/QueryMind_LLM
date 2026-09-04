"""Few-shot exemplar retrieval (Phase 2, `feat/few-shot-retrieval`).

QueryMind had no exemplars at all, so this starts from nothing rather than
porting anything.

Exemplars are drawn **only from the `calib` split**, which is disjoint from
`eval_500` by construction (see eval/subsets.py). That is what keeps this from
being leakage: retrieving a near-identical question together with its gold SQL
from the set being scored would inflate accuracy without teaching the model
anything.

Similarity is TF-IDF cosine over question text. Deliberately not embeddings:
character/word overlap is a strong signal for this task (BIRD questions reuse
domain vocabulary heavily), it needs no model, no API and no key, and it is
fast enough to run per question. If it underperforms, that is a measurable
finding rather than an assumption.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


@dataclass
class Exemplar:
    question: str
    sql: str
    evidence: str
    db_id: str
    similarity: float = 0.0


class ExemplarStore:
    """TF-IDF retrieval over the calibration split."""

    def __init__(self, records: list[dict], same_db_only: bool = False):
        self.records = records
        self.same_db_only = same_db_only

    @classmethod
    def from_bird(cls, bird_root: str | Path, splits_file: str | Path, **kw) -> ExemplarStore:
        bird_root = Path(bird_root)
        all_recs = json.loads((bird_root / "dev.json").read_text(encoding="utf-8"))
        calib = set(json.loads(Path(splits_file).read_text(encoding="utf-8"))["calib"])
        pool = [r for r in all_recs if r["question_id"] in calib]
        if not pool:
            raise ValueError("calibration split is empty; cannot build exemplar store")
        return cls(pool, **kw)

    @cached_property
    def _vectorizer(self) -> TfidfVectorizer:
        v = TfidfVectorizer(ngram_range=(1, 2), sublinear_tf=True, min_df=1, stop_words="english")
        v.fit([r["question"] for r in self.records])
        return v

    @cached_property
    def _matrix(self):
        return self._vectorizer.transform([r["question"] for r in self.records])

    def retrieve(
        self,
        question: str,
        k: int = 3,
        db_id: str | None = None,
        exclude_question_id: int | None = None,
    ) -> list[Exemplar]:
        """Top-k most similar calibration questions.

        `same_db_only` restricts to the target database. That trades away
        coverage (some BIRD databases hold few calibration questions) for
        exemplars whose table and column names actually appear in the target
        schema. Which wins is measured, not assumed.

        `exclude_question_id` drops the query's own record from the pool.
        **This is a leakage guard, not a nicety.** Scoring a question that is
        itself in the exemplar pool retrieves it at similarity 1.0 and hands the
        model its own gold SQL: measured at +23.5 points of pure contamination
        on the calibration split before this was added.
        """
        idx = range(len(self.records))
        if self.same_db_only and db_id:
            idx = [i for i in idx if self.records[i]["db_id"] == db_id]
            if not idx:
                idx = range(len(self.records))

        if exclude_question_id is not None:
            idx = [i for i in idx if self.records[i]["question_id"] != exclude_question_id]

        sims = cosine_similarity(self._vectorizer.transform([question]), self._matrix)[0]
        ranked = sorted(idx, key=lambda i: sims[i], reverse=True)[:k]
        return [
            Exemplar(
                question=self.records[i]["question"],
                sql=self.records[i]["SQL"],
                evidence=self.records[i].get("evidence", ""),
                db_id=self.records[i]["db_id"],
                similarity=float(sims[i]),
            )
            for i in ranked
        ]


def render_exemplars(exemplars: list[Exemplar]) -> str:
    """Format retrieved examples for the prompt."""
    if not exemplars:
        return ""
    blocks = ["Here are examples of correct queries on similar questions:\n"]
    for e in exemplars:
        if e.evidence:
            blocks.append(f"-- Knowledge: {e.evidence}")
        blocks.append(f"-- Question: {e.question}")
        blocks.append(f"{' '.join(e.sql.split())}\n")
    return "\n".join(blocks)
