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
