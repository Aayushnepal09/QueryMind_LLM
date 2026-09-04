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
| `join_path_error` | Predicted and gold reference different table sets | The model misunderstood which entities the question spans. spec §6 flags joins as the dominant failure surface, and this is where that shows. |
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
  known errors (§7 of docs/spec.md), so some "failures" are cases where the
  prediction is defensible and the gold is not. Those land in `other` or
  `wrong_columns` and are only separable by hand.

The rules do the bulk sorting so that hand inspection can be spent where it
matters. `results/failures-*.json` carries every failure with its question,
predicted SQL and gold SQL, so any category can be spot-checked.

---

# Worked examples

One real failure per category, from the baseline `eval_500` run (273 failures).
Taken as the first instance in each bucket rather than hand-picked, so they are
representative of what the classifier actually sorts — including where it sorts
badly.

Full data, with every failure's question, prediction and gold SQL:
`results/failures-baseline-eval500.json`.

### `execution_error` — 82 (30%)

> *"What is the average writing score of the school who has the highest number
> of test takers…"*
>
> **Predicted:** `SELECT AVG(s.AvgScrWrite), s.City FROM satscores s JOIN … WHERE s.NumGE1500 = (SELECT MAX(NumGE1500) …)`
> **Gold:** `SELECT T1.AvgScrWrite, T2.City FROM … ORDER BY T1.NumGE1500 DESC LIMIT 1`

The model reached for an aggregate plus a correlated subquery where gold sorts
and takes the top row. It also selects a bare column alongside `AVG()` without a
`GROUP BY`. A "highest / most" question has two idiomatic forms, and the
`ORDER BY … LIMIT 1` form is both simpler and the one that executes.

### `join_path_error` — 73 (27%)

> *"How many schools in Fresno (directly funded) have number of test takers not
> more than 250?"*
>
> **Predicted:** joins `schools` + `frpm` + `satscores`
> **Gold:** joins `frpm` + `satscores`

**Over-joining.** The model pulled in `schools` because the question says
"schools", although every column it needs already exists on `frpm`. This is the
dominant shape in this bucket: not a *missing* join, but an unnecessary one that
changes row multiplicity.

### `other` — 60 (22%)

> *"…what are the website addresses of the schools with a free meal count of
> 1,900–2,000…"*
>
> **Predicted:** `SELECT T2.Website, T2.School FROM frpm AS T1 JOIN schools AS T2 …`
> **Gold:** `SELECT T2.Website, T1.``School Name`` FROM frpm AS T1 INNER JOIN schools AS T2 …`

**This prediction is arguably correct.** `schools.School` and
`frpm.School Name` hold the same school name; the model read it from one table
and the annotation reads it from the other. Under exact result-set comparison
that scores as a failure.

This matters for how the whole number should be read. A share of the `other`
bucket — the second largest — is **benchmark strictness rather than model
error**. BIRD's annotations are known to contain such cases, which is part of
why this project describes it as a realistic, deliberately hard benchmark rather
than a ranking. It also means the true accuracy is somewhat *higher* than 45.6%,
by an amount not automatically measurable.

### `wrong_columns` — 27 (10%)

> *"List the names of schools with more than 30 difference in enrolments… Please
> also give the…"*
>
> **Predicted:** 6 columns — `School Name`, `Street`, `StreetAbr`, `City`, `Zip`, `State`
> **Gold:** 2 columns — `School`, `Street`

**Over-selection against an ambiguous question.** "Please also give the…" does
not pin down how much address detail is wanted, and the model returned all of
it. A human analyst would ask; the agent guesses, and exact-match scoring
punishes the guess.

### `aggregation_error` — 16 (6%)

> *"State the names and full communication address of high schools in Monterey…"*
>
> **Predicted:** filters `s.SOCType = 'High Schools (Public)'`
> **Gold:** filters on `T2.County = 'Monterey'` and reads the name from `frpm`

**A misclassification, shown deliberately.** The real defect here is which table
the columns come from, not aggregation. The rule fired because one query
contains an aggregate keyword and the other does not. This is the limitation
stated at the top of this file made concrete: the classifier compares SQL
structure, not meaning, and roughly one bucket in eight is sorted on a
superficial cue.

### `value_matching` — 9 (3%)

> *"…account holders whose transactions on the credit card are less than the
> average…"*
>
> **Predicted:** `WHERE t1.type = 'VYBER KARTOU' AND t1.amount < (SELECT AVG …)`
> **Gold:** `WHERE STRFTIME('%Y', T1.date) = '1998' AND T1.operation = …`

The model dropped the year constraint entirely and filtered on a different
column. Also arguably misfiled — it returns zero rows and contains a string
literal, which is what the rule keys on, but the cause is a missing condition.

Note that `results/FINDINGS.md` §7 tests the usual remedy for this category
directly: relaxing string matching to be case-insensitive recovers only **8%**
of these, so fuzzy value matching is not the fix it is often assumed to be.

### `ordering_or_limit` — 5 (2%)

> *"Between San Diego and Santa Barbara, which county offers the most schools
> that does not offer physical buildings?"*
>
> **Predicted:** `SELECT COUNT(*) … WHERE County IN ('San Diego','Santa Barbara')`
> **Gold:** `SELECT County, COUNT(Virtual) … GROUP BY County ORDER BY COUNT(Virtual) DESC LIMIT 1`

The model counted across both counties instead of grouping and comparing them.
"Which of A and B has more" requires a comparison; it produced a total.

### `empty_result` — 1 (0%)

> *"Who was the player that got the lap time of 0:01:27 in race No. 161?"*
>
> **Predicted:** `SELECT d.url … WHERE lt.time LIKE '0:01:27%'`
> **Gold:** `SELECT DISTINCT T2.forename, T2.surname, T2.url …`

Right filter, wrong projection — the question asks *who*, and the prediction
returns only the URL.

---

## What the examples change about the headline numbers

Two of the eight categories contain a meaningful share of predictions that are
defensible: `other` (22% of failures) and parts of `wrong_columns` (10%), where
the disagreement is with the annotation's choice of source table or level of
detail rather than with the data.

**The reported 45.6% is therefore a lower bound on what a human would call
correct.** Quantifying the gap would require manual adjudication of ~90
failures, which has not been done here. It is stated rather than corrected for,
because silently adjusting a benchmark number in your own favour is exactly the
practice this project was built to move away from.
