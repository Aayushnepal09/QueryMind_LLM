# Technique notes

Diagnostics that explain *why* a technique moved the number, recorded alongside
the number itself. Measured on `dev_50` unless stated.

## Few-shot retrieval (k=3 exemplars)

**Result: 58.0% vs 62.0% baseline — 4 points worse.**

At n=50 the 95% CI is ±13 points, so this is **not a significant decline**. It
is reported as "no measurable benefit", not as "few-shot hurts".

Diagnostics run to check for an implementation fault rather than a real null:

| Check | Value | Reading |
|---|---|---|
| Exemplars drawn from the same database as the question | 140/150 (93%) | Retrieval is not mostly pulling in foreign schemas |
| Exemplars from a *different* database | 10/150 (7%) | Small, but these do show table names absent from the target schema |
| Mean TF-IDF cosine similarity | 0.338 | Weak. Retrieved questions are only loosely related |
| Prompt size, baseline | ~1,360 tokens | |
| Prompt size, with 3 exemplars | ~3,400 tokens | **2.5x** |

Two plausible mechanisms, not separated by this data:

1. **Weak retrieval.** A mean similarity of 0.34 means the exemplars often are
   not analogous. BIRD's calibration pool has ~94 questions per database on
   average, which is thin for nearest-neighbour retrieval.
2. **Context dilution.** Tripling the prompt gives a 7B model more to attend to
   without adding proportionally more signal.

Worth testing before drawing a conclusion: restricting retrieval to the same
database (`ExemplarStore(same_db_only=True)`), and k=1 to isolate prompt length
from exemplar quality. Both are cheap; neither has been run.

**Not done:** tuning the retriever until the number improves. With a ±13 margin
that would be fitting to noise, and `dev_50` is the development loop, not
evidence.

## Schema pruning

**Result: 62.0% — identical to the 62.0% baseline, to the question.**

Predicted before the run from a structural diagnostic, and confirmed exactly:

| Measurement | Value |
|---|---|
| Tables retained across `dev_50` | 351/356 (99%) |
| Mean prompt reduction | 1.3% |
| BIRD dev database sizes | 3–13 tables |

The pruner scores tables on lexical overlap with the question, then re-adds
anything reachable by a foreign key from a kept table. On BIRD's dev databases —
small and densely FK-connected — that closure pulls back essentially everything
the lexical scoring dropped. The technique is a no-op here, and the identical
accuracy is the expected consequence, not a coincidence.

**Why the FK closure stays.** Removing it would prune more aggressively and make
the technique *look* like it does something. It would also drop bridge tables
that joins depend on, and a missing bridge table does not produce an error — it
produces confidently wrong SQL. A silent accuracy loss traded for a visible
token saving is a bad trade on a benchmark scored by exact result sets.

**Answering CLAUDE.md §13** ("Is schema linking worth it on BIRD's larger
databases, or does full-schema context win?"): on BIRD *dev*, full-schema context
wins by default because there is nothing meaningful to prune. Mean schema is
~1,230 tokens and the largest is 3,128, so there is no token pressure to relieve
either. Schema linking is a technique for wide schemas; BIRD dev does not have
them. It would need BIRD *train* (or a genuinely wide warehouse schema) to be
tested properly.
