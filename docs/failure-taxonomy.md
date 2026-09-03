# Failure taxonomy

How SQLSentinel's incorrect queries are categorised. Definitions live here;
measured distributions live in `results/failures-*.json` and are summarised in
the README.

## Why a taxonomy at all

An accuracy number says *how often* the agent is wrong. It says nothing about
*how* it is wrong, and those are different questions with different fixes. A
system failing mostly on value matching needs better sample values in the
prompt; one failing mostly on join paths needs schema linking. Without the
breakdown, improvement is guesswork.

## Categories

Applied in the order listed — the first matching rule wins, so earlier
categories take precedence.

| Category | Rule | What it usually means |
|---|---|---|
| `empty_prediction` | No SQL extracted from the model response | The model refused, rambled, or the extractor found nothing SQL-shaped. A pipeline failure, not a reasoning failure. |
| `execution_error` | SQL did not execute | Hallucinated column or table, syntax error, type mismatch. The most *honest* failure mode: it announces itself, and self-correction can act on it. |
| `join_path_error` | Predicted and gold reference different table sets | The model misunderstood which entities the question spans. CLAUDE.md §6 flags joins as the dominant failure surface, and this is where that shows. |
| `aggregation_error` | One of predicted/gold aggregates and the other does not | Confusing "how many X" with "which X", or missing a GROUP BY. |
| `value_matching` | Ran fine, returned zero rows, filters on a string literal | The query is structurally right but filters on a value that does not match what is stored — `'CA'` vs `'California'`, case, whitespace. This is BIRD's "dirty data" difficulty made concrete, and it is the most dangerous category because it looks like a valid empty answer. |
| `empty_result` | Ran fine, returned zero rows, no string filter | Over-restrictive conditions. |
| `ordering_or_limit` | Differs only in ORDER BY / LIMIT presence | Often a near-miss: the right rows in the wrong order, or an unrequested LIMIT. |
| `wrong_columns` | Different number of selected expressions | Right rows, wrong projection. |
| `other` | Anything else | Held deliberately as a residual so the classifier cannot silently overreach. A large `other` share means the rules need work, and that is worth knowing. |

## What the classifier cannot see

Stated plainly because it bounds how far these numbers should be trusted:

- **It compares SQL text, not semantics.** Two queries can join the same tables
  by different paths and be classified identically.
- **`join_path_error` absorbs aliasing noise.** The regex extracts identifiers
  after `FROM`/`JOIN`, so subqueries and CTEs can inflate this bucket.
- **It cannot recognise an ambiguous question.** BIRD's annotations contain
  known errors (§7 of CLAUDE.md), so some "failures" are cases where the
  prediction is defensible and the gold is not. Those land in `other` or
  `wrong_columns` and are only separable by hand.

The rules do the bulk sorting so that hand inspection can be spent where it
matters. `results/failures-*.json` carries every failure with its question,
predicted SQL and gold SQL, so any category can be spot-checked.
