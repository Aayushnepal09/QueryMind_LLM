"""Secondary analyses over the trace data.

Everything here is derived from this project's own runs. Nothing is quoted from
the literature; where a result happens to agree with published work that is
noted as agreement, not as a source.

    uv run python scripts/research.py

Writes results/findings.json and prints a report. Per-question correctness
labels are cached in results/.labels_cache.json because computing them means
executing every prediction and its gold query, which is slow and deterministic.
"""

from __future__ import annotations

import itertools
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from analyze import TraceShim, per_question_correct  # noqa: E402

from sqlsentinel.eval.harness import BirdHarness  # noqa: E402
from sqlsentinel.eval.stats import mcnemar, wilson_interval  # noqa: E402
from sqlsentinel.executor import bird_executor  # noqa: E402

RESULTS = REPO_ROOT / "results"
TRACES = RESULTS / "traces"
LABEL_CACHE = RESULTS / ".labels_cache.json"


# ---------------------------------------------------------------- data loading


def load_labels(harness: BirdHarness, tag: str) -> dict[int, int]:
    cache = json.loads(LABEL_CACHE.read_text(encoding="utf-8")) if LABEL_CACHE.exists() else {}
    if tag in cache:
        return {int(k): v for k, v in cache[tag].items()}

    traces = load_traces(tag)
    labels = dict(
        zip(
            [t.question_id for t in traces],
            per_question_correct(harness, traces),
            strict=True,
        )
    )
    cache[tag] = {str(k): v for k, v in labels.items()}
    LABEL_CACHE.write_text(json.dumps(cache), encoding="utf-8")
    return labels


def load_traces(tag: str) -> list[TraceShim]:
    return [TraceShim(d) for d in json.loads((TRACES / f"{tag}.json").read_text(encoding="utf-8"))]


# ---------------------------------------------------------------- findings


def finding_silent_failures(tag: str, labels: dict[int, int]) -> dict:
    """How many wrong answers look like right ones?

    A wrong query that raises an error announces itself. A wrong query that
    executes cleanly and returns plausible-looking rows does not -- it is
    indistinguishable from success without the gold answer. That second class is
    the one a confidence layer exists to catch, and its size is the argument for
    building one.
    """
    traces = load_traces(tag)
    wrong = [t for t in traces if not labels.get(t.question_id, 0)]
    silent = [t for t in wrong if t.executed_ok and t.result_row_count > 0]
    loud_error = [t for t in wrong if not t.executed_ok]
    empty = [t for t in wrong if t.executed_ok and t.result_row_count == 0]

    return {
        "n_total": len(traces),
        "n_wrong": len(wrong),
        "silent_failures": len(silent),
        "silent_share_of_errors": len(silent) / max(len(wrong), 1),
        "silent_share_of_all": len(silent) / max(len(traces), 1),
        "loud_execution_errors": len(loud_error),
        "empty_results": len(empty),
    }


def finding_per_database(harness, tag: str, labels: dict[int, int]) -> list[dict]:
    """Accuracy per database, against that database's schema size.

    BIRD is usually reported as one number. Per-database accuracy varies far
    more than that summary suggests, and the spread is what tells you whether
    the agent is generally mediocre or specifically defeated by certain schemas.
    """
    from sqlsentinel.schema_linker import bird_schema

    by_db: dict[str, list[int]] = defaultdict(list)
    for t in load_traces(tag):
        by_db[t.db_id].append(labels.get(t.question_id, 0))

    rows = []
    for db, vals in by_db.items():
        schema = bird_schema(str(harness.db_root), db)
        lo, hi = wilson_interval(sum(vals), len(vals))
        rows.append(
            {
                "db_id": db,
                "n": len(vals),
                "accuracy": 100 * sum(vals) / len(vals),
                "ci_low": lo,
                "ci_high": hi,
                "n_tables": len(schema.tables),
                "n_columns": schema.n_columns,
                "n_foreign_keys": len(schema.join_paths()),
            }
        )
    return sorted(rows, key=lambda r: r["accuracy"])


def _pearson(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """Correlation plus a two-sided p-value via the t approximation."""
    n = len(xs)
    if n < 3:
        return 0.0, 1.0
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return 0.0, 1.0
    r = sxy / math.sqrt(sxx * syy)
    if abs(r) >= 1.0:
        return r, 0.0
    t = r * math.sqrt((n - 2) / (1 - r**2))
    # survival function of |t| with n-2 df, via the incomplete beta
    df = n - 2
    x = df / (df + t * t)
    p = _betainc(df / 2, 0.5, x)
    return r, min(1.0, p)


def _betainc(a: float, b: float, x: float) -> float:
    """Regularised incomplete beta, continued fraction. Enough for a p-value."""
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lbeta = math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
    front = math.exp(math.log(x) * a + math.log(1 - x) * b - lbeta) / a
    f, c, d = 1.0, 1.0, 0.0
    for i in range(200):
        m = i // 2
        if i == 0:
            num = 1.0
        elif i % 2 == 0:
            num = (m * (b - m) * x) / ((a + 2 * m - 1) * (a + 2 * m))
        else:
            num = -((a + m) * (a + b + m) * x) / ((a + 2 * m) * (a + 2 * m + 1))
        d = 1.0 + num * d
        d = 1e-30 if abs(d) < 1e-30 else d
        d = 1.0 / d
        c = 1.0 + num / c
        c = 1e-30 if abs(c) < 1e-30 else c
        f *= c * d
        if abs(1.0 - c * d) < 1e-8:
            break
    return front * (f - 1.0)


def finding_schema_size_correlation(rows: list[dict]) -> dict:
    """Does schema width predict how badly the agent does?"""
    out = {}
    for key in ("n_tables", "n_columns", "n_foreign_keys"):
        r, p = _pearson([float(x[key]) for x in rows], [x["accuracy"] for x in rows])
        out[key] = {"pearson_r": r, "p_value": p, "significant": p < 0.05}
    return out


def finding_evidence_by_difficulty(harness) -> dict:
    """Does BIRD's evidence hint help uniformly, or only where it is hard?"""
    with_e = load_labels(harness, "baseline-dev50")
    without_e = load_labels(harness, "noevidence-dev50")
    shared = sorted(set(with_e) & set(without_e))

    by_diff: dict[str, dict[str, int]] = defaultdict(lambda: {"helped": 0, "hurt": 0, "n": 0})
    for qid in shared:
        d = harness.by_id[qid]["difficulty"]
        by_diff[d]["n"] += 1
        if with_e[qid] and not without_e[qid]:
            by_diff[d]["helped"] += 1
        elif without_e[qid] and not with_e[qid]:
            by_diff[d]["hurt"] += 1

    cmp = mcnemar(without_e, with_e, "no evidence", "with evidence")
    return {
        "overall": {
            "delta_points": cmp.delta_points,
            "helped": cmp.b_only,
            "hurt": cmp.a_only,
            "p_value": cmp.p_value,
        },
        "by_difficulty": {
            d: {**v, "net_points": 100 * (v["helped"] - v["hurt"]) / max(v["n"], 1)}
            for d, v in by_diff.items()
        },
    }


def finding_failure_transitions(harness) -> dict:
    """Which questions did the techniques flip, in each direction?

    A net gain of +N points can be N questions fixed, or 3N fixed and 2N broken.
    Those are different systems with the same score.
    """
    base = load_labels(harness, "baseline-eval500")
    final = load_labels(harness, "final-eval500")
    shared = sorted(set(base) & set(final))

    fixed = [q for q in shared if final[q] and not base[q]]
    broken = [q for q in shared if base[q] and not final[q]]

    def by_difficulty(ids):
        out = defaultdict(int)
        for q in ids:
            out[harness.by_id[q]["difficulty"]] += 1
        return dict(out)

    cmp = mcnemar(base, final, "baseline", "final")
    return {
        "n": len(shared),
        "fixed": len(fixed),
        "broken": len(broken),
        "net": len(fixed) - len(broken),
        "churn_ratio": (len(fixed) + len(broken)) / max(abs(len(fixed) - len(broken)), 1),
        "fixed_by_difficulty": by_difficulty(fixed),
        "broken_by_difficulty": by_difficulty(broken),
        "p_value": cmp.p_value,
        "significant": cmp.significant,
    }


def finding_agreement_vs_difficulty(harness) -> dict:
    """Does the model's uncertainty track the benchmark's own difficulty labels?

    If agreement is lower on questions BIRD calls challenging, the confidence
    signal is picking up genuine task difficulty rather than noise -- which is
    what makes it worth routing on.
    """
    traces = load_traces("k3-eval200")
    by_diff: dict[str, list[float]] = defaultdict(list)
    for t in traces:
        by_diff[harness.by_id[t.question_id]["difficulty"]].append(t.agreement_rate)

    order = ["simple", "moderate", "challenging"]
    means = {d: sum(v) / len(v) for d, v in by_diff.items() if v}
    monotone = all(
        means.get(a, 0) >= means.get(b, 0)
        for a, b in itertools.pairwise(order)
        if a in means and b in means
    )
    return {
        "mean_agreement": means,
        "counts": {d: len(v) for d, v in by_diff.items()},
        "decreases_with_difficulty": monotone,
    }


def finding_cost_per_correct(harness) -> list[dict]:
    """Cost per correct answer -- CLAUDE.md section 13's actual decision metric.

    Local generation is free in dollars but not in time, so wall-clock is
    reported alongside the nominal API price the same run would have cost.
    """
    rows = []
    for tag in ("baseline-eval500", "final-eval500", "k3-eval200"):
        path = TRACES / f"{tag}.json"
        if not path.exists():
            continue
        traces = load_traces(tag)
        labels = load_labels(harness, tag)
        n_correct = sum(labels.values())
        gen_seconds = sum(t.latency_s for t in traces)
        rows.append(
            {
                "tag": tag,
                "n": len(traces),
                "correct": n_correct,
                "accuracy": 100 * n_correct / len(traces),
                "total_generation_seconds": gen_seconds,
                "seconds_per_correct": gen_seconds / max(n_correct, 1),
                "nominal_usd": sum(t.cost_usd for t in traces),
                "mean_prompt_tokens": sum(t.prompt_tokens for t in traces) / len(traces),
            }
        )
    return rows


def finding_question_length(harness) -> dict:
    """Do longer questions fail more?"""
    labels = load_labels(harness, "baseline-eval500")
    xs, ys = [], []
    buckets: dict[str, list[int]] = defaultdict(list)
    for qid, ok in labels.items():
        words = len(harness.by_id[qid]["question"].split())
        xs.append(float(words))
        ys.append(float(ok))
        key = (
            "1-10" if words <= 10 else "11-20" if words <= 20 else "21-30" if words <= 30 else "31+"
        )
        buckets[key].append(ok)
    r, p = _pearson(xs, ys)
    return {
        "pearson_r": r,
        "p_value": p,
        "significant": p < 0.05,
        "by_bucket": {
            k: {"n": len(v), "accuracy": 100 * sum(v) / len(v)} for k, v in sorted(buckets.items())
        },
    }


def finding_value_matching(harness) -> dict:
    """How often does a query fail purely on a string literal?

    BIRD is described as 'dirty', and this quantifies one specific form of that:
    queries that are structurally sound but filter on a value whose spelling or
    casing does not exist in the column.
    """
    labels = load_labels(harness, "baseline-eval500")
    traces = load_traces("baseline-eval500")
    wrong = [t for t in traces if not labels.get(t.question_id, 0)]

    zero_row_with_literal = 0
    recoverable = 0
    for t in wrong:
        if not (t.executed_ok and t.result_row_count == 0):
            continue
        literals = re.findall(r"=\s*'([^']+)'", t.sql or "")
        if not literals:
            continue
        zero_row_with_literal += 1
        # would a case-insensitive match have returned rows?
        relaxed = re.sub(r"=\s*'([^']+)'", lambda m: f"LIKE '{m.group(1)}'", t.sql)
        try:
            if bird_executor(harness.db_root, t.db_id).execute(relaxed).row_count > 0:
                recoverable += 1
        except Exception:
            pass

    return {
        "zero_row_failures_with_string_filter": zero_row_with_literal,
        "recoverable_by_relaxing_the_match": recoverable,
        "share_recoverable": recoverable / max(zero_row_with_literal, 1),
    }


# ---------------------------------------------------------------- report


def main() -> None:
    harness = BirdHarness(Path("data/bird/dev_20240627"))
    findings: dict = {}

    print("computing per-question labels (cached after the first run)...")
    base_labels = load_labels(harness, "baseline-eval500")
    final_labels = load_labels(harness, "final-eval500")

    findings["silent_failures_baseline"] = finding_silent_failures("baseline-eval500", base_labels)
    findings["silent_failures_final"] = finding_silent_failures("final-eval500", final_labels)
    findings["per_database"] = finding_per_database(harness, "baseline-eval500", base_labels)
    findings["schema_size_correlation"] = finding_schema_size_correlation(findings["per_database"])
    findings["evidence_by_difficulty"] = finding_evidence_by_difficulty(harness)
    findings["failure_transitions"] = finding_failure_transitions(harness)
    findings["agreement_vs_difficulty"] = finding_agreement_vs_difficulty(harness)
    findings["cost_per_correct"] = finding_cost_per_correct(harness)
    findings["question_length"] = finding_question_length(harness)
    findings["value_matching"] = finding_value_matching(harness)

    (RESULTS / "findings.json").write_text(json.dumps(findings, indent=2), encoding="utf-8")
    _print(findings)
    print(f"\nwrote {RESULTS / 'findings.json'}")


def _print(f: dict) -> None:
    w = 74
    print("\n" + "=" * w)
    print("SILENT FAILURES")
    print("=" * w)
    for name, key in (("baseline", "silent_failures_baseline"), ("final", "silent_failures_final")):
        d = f[key]
        print(
            f"{name:9s} wrong {d['n_wrong']:3d}/{d['n_total']} | "
            f"silent {d['silent_failures']:3d} "
            f"({d['silent_share_of_errors']:.0%} of errors, "
            f"{d['silent_share_of_all']:.0%} of all queries) | "
            f"loud {d['loud_execution_errors']:3d} | empty {d['empty_results']:3d}"
        )

    print("\n" + "=" * w)
    print("ACCURACY BY DATABASE (baseline)")
    print("=" * w)
    print(
        f"{'database':26s} {'n':>4s} {'EX':>7s} {'95% CI':>16s} {'tbl':>4s} {'col':>5s} {'FK':>4s}"
    )
    for r in f["per_database"]:
        print(
            f"{r['db_id']:26s} {r['n']:4d} {r['accuracy']:6.1f}% "
            f"[{r['ci_low']:5.1f},{r['ci_high']:5.1f}] "
            f"{r['n_tables']:4d} {r['n_columns']:5d} {r['n_foreign_keys']:4d}"
        )
    print("\ncorrelation of accuracy with schema size:")
    for k, v in f["schema_size_correlation"].items():
        mark = "significant" if v["significant"] else "not significant"
        print(f"  {k:16s} r = {v['pearson_r']:+.3f}  p = {v['p_value']:.4f}  {mark}")

    print("\n" + "=" * w)
    print("VALUE OF BIRD'S EVIDENCE FIELD, BY DIFFICULTY")
    print("=" * w)
    e = f["evidence_by_difficulty"]
    print(
        f"overall {e['overall']['delta_points']:+.1f} points "
        f"(helped {e['overall']['helped']}, hurt {e['overall']['hurt']}, "
        f"p={e['overall']['p_value']:.4f})"
    )
    for d, v in e["by_difficulty"].items():
        print(
            f"  {d:12s} n={v['n']:3d}  helped {v['helped']:2d}  hurt {v['hurt']:2d}  "
            f"net {v['net_points']:+.1f} points"
        )

    print("\n" + "=" * w)
    print("WHAT THE TECHNIQUES ACTUALLY CHANGED (baseline -> final, n=500)")
    print("=" * w)
    t = f["failure_transitions"]
    print(
        f"fixed {t['fixed']}  broken {t['broken']}  net {t['net']:+d}  "
        f"p={t['p_value']:.4f}  churn {t['churn_ratio']:.1f}x the net effect"
    )
    print(f"  fixed by difficulty : {t['fixed_by_difficulty']}")
    print(f"  broken by difficulty: {t['broken_by_difficulty']}")

    print("\n" + "=" * w)
    print("DOES UNCERTAINTY TRACK BENCHMARK DIFFICULTY?")
    print("=" * w)
    a = f["agreement_vs_difficulty"]
    for d in ("simple", "moderate", "challenging"):
        if d in a["mean_agreement"]:
            print(f"  {d:12s} mean agreement {a['mean_agreement'][d]:.3f}  (n={a['counts'][d]})")
    print(f"  decreases with difficulty: {a['decreases_with_difficulty']}")

    print("\n" + "=" * w)
    print("COST PER CORRECT ANSWER")
    print("=" * w)
    for r in f["cost_per_correct"]:
        print(
            f"{r['tag']:20s} {r['accuracy']:5.1f}%  "
            f"{r['seconds_per_correct']:6.1f} s/correct  "
            f"${r['nominal_usd']:.4f} nominal  "
            f"{r['mean_prompt_tokens']:.0f} prompt tok"
        )

    print("\n" + "=" * w)
    print("QUESTION LENGTH VS ACCURACY")
    print("=" * w)
    q = f["question_length"]
    print(f"  r = {q['pearson_r']:+.3f}  p = {q['p_value']:.4f}")
    for k, v in q["by_bucket"].items():
        print(f"  {k:8s} words  n={v['n']:3d}  {v['accuracy']:5.1f}%")

    print("\n" + "=" * w)
    print("VALUE-MATCHING FAILURES")
    print("=" * w)
    v = f["value_matching"]
    print(
        f"  zero-row failures filtering on a string : {v['zero_row_failures_with_string_filter']}"
    )
    print(f"  would return rows if the match relaxed  : {v['recoverable_by_relaxing_the_match']}")
    print(f"  share recoverable                       : {v['share_recoverable']:.0%}")


if __name__ == "__main__":
    main()
