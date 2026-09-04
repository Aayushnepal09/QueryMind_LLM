"""Turn dumped traces into the report artifacts (CLAUDE.md section 8, Phase 5).

Produces, into results/:

    calibration-<label>.json    Brier score, ECE, reliability curve
    calibration-<label>.png     reliability diagram
    routing-<label>.json        % routed vs % of errors caught, per threshold
    routing-<label>.png         the routing curve
    failures-<label>.json       failure taxonomy over the incorrect queries

Usage:
    uv run python scripts/analyze.py results/traces/<run>.json --label <name>

The confidence model, when fitted, trains on calibration traces only and is
handed the evaluation split as `forbidden_ids`, so leakage raises rather than
silently inflating the numbers.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import numpy as np

from sqlsentinel.confidence import (
    ConfidenceModel,
    agreement_confidence,
    extract_features,
    save_calibration_report,
)
from sqlsentinel.eval.harness import BirdHarness
from sqlsentinel.router import best_operating_point, routing_curve

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS = REPO_ROOT / "results"

# The official BIRD scorer uses a 30s per-query timeout; per-question labelling
# must use the same budget or it diverges from the headline number.
OFFICIAL_TIMEOUT_S = 30.0


class TraceShim:
    """Rehydrate a dumped AgentTrace dict into attribute access."""

    def __init__(self, d: dict):
        self.__dict__.update(d)


# ---------------------------------------------------------------- taxonomy

TAXONOMY = {
    "empty_prediction": "No SQL was produced at all",
    "execution_error": "SQL was syntactically or semantically invalid",
    "wrong_columns": "Right rows, wrong columns returned",
    "empty_result": "Query ran but returned no rows where gold returns some",
    "aggregation_error": "Aggregation or grouping differs from gold",
    "join_path_error": "Different set of tables joined than gold",
    "value_matching": "Filters on a literal that does not match the stored value",
    "ordering_or_limit": "Differs only in ORDER BY / LIMIT",
    "other": "Incorrect for a reason not automatically classifiable",
}

_TABLES = re.compile(r"\b(?:FROM|JOIN)\s+[`\"\[]?(\w+)", re.I)
_AGG = re.compile(r"\b(COUNT|SUM|AVG|MAX|MIN|GROUP\s+BY|HAVING)\b", re.I)
_ORDER = re.compile(r"\b(ORDER\s+BY|LIMIT)\b", re.I)


def classify_failure(trace, gold_sql: str) -> str:
    """Heuristic failure category.

    Deliberately rule-based and transparent: the point of the taxonomy is that
    a reader can check it, and CLAUDE.md asks for ~50 failures inspected with
    an example of each. These rules do the bulk sorting; the report carries
    examples so the categories can be spot-checked by hand.
    """
    sql = (trace.sql or "").strip()
    if not sql:
        return "empty_prediction"
    if not trace.executed_ok:
        return "execution_error"

    pred_tables = {t.lower() for t in _TABLES.findall(sql)}
    gold_tables = {t.lower() for t in _TABLES.findall(gold_sql)}
    if pred_tables != gold_tables:
        return "join_path_error"

    if bool(_AGG.search(sql)) != bool(_AGG.search(gold_sql)):
        return "aggregation_error"

    if trace.result_row_count == 0:
        # ran fine, returned nothing: usually a literal that does not match
        return "value_matching" if re.search(r"=\s*'[^']+'", sql) else "empty_result"

    if bool(_ORDER.search(sql)) != bool(_ORDER.search(gold_sql)):
        return "ordering_or_limit"

    pred_cols = sql.upper().split("FROM")[0].count(",")
    gold_cols = gold_sql.upper().split("FROM")[0].count(",")
    if pred_cols != gold_cols:
        return "wrong_columns"

    return "other"


# ---------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("traces", help="JSON produced by --dump-traces")
    ap.add_argument("--label", required=True)
    ap.add_argument("--calib-traces", default="", help="traces to fit the confidence model on")
    ap.add_argument("--bird-root", default="data/bird/dev_20240627")
    args = ap.parse_args()

    RESULTS.mkdir(exist_ok=True)
    harness = BirdHarness(Path(args.bird_root))
    splits = json.loads((RESULTS / "splits.json").read_text(encoding="utf-8"))

    traces = [TraceShim(d) for d in json.loads(Path(args.traces).read_text(encoding="utf-8"))]
    qids = [t.question_id for t in traces]

    # score each prediction against gold using the official harness
    preds = {t.question_id: t.sql or "SELECT 1" for t in traces}
    result = harness.evaluate(preds, qids)
    print(f"{args.label}: {result}")

    # per-question labels, needed to fit and score the confidence model
    correct = per_question_correct(harness, traces)

    # Cross-check our per-question labelling against the official aggregate.
    # They are computed by different code paths (our executor + normalization vs
    # BIRD's script), so agreement is real evidence the labels are sound. A
    # divergence would silently corrupt calibration and the routing curve.
    ours = 100.0 * sum(correct) / len(correct)
    if abs(ours - result.accuracy) > 0.51:  # half a question at n=100
        raise SystemExit(
            f"per-question labels disagree with the official scorer: "
            f"{ours:.1f}% vs {result.accuracy:.1f}%. Calibration and routing "
            f"would be built on wrong labels; fix before reporting."
        )

    feats = []
    for t in traces:
        rec = harness.by_id[t.question_id]
        feats.append(
            extract_features(t, rec["question"], rec.get("evidence", ""), t.n_tables_in_prompt or 1)
        )

    # ---- confidence
    conf_v1 = np.array([agreement_confidence(f) for f in feats])
    reports = {}
    reports["v1_agreement"] = save_calibration_report(
        RESULTS / f"calibration-{args.label}-v1.json",
        conf_v1,
        correct,
        f"{args.label} v1 (agreement only)",
    )

    conf = conf_v1
    if args.calib_traces and Path(args.calib_traces).exists():
        cal = [
            TraceShim(d) for d in json.loads(Path(args.calib_traces).read_text(encoding="utf-8"))
        ]
        cal_correct = per_question_correct(harness, cal)
        cal_feats = []
        for t in cal:
            rec = harness.by_id[t.question_id]
            cal_feats.append(
                extract_features(
                    t, rec["question"], rec.get("evidence", ""), t.n_tables_in_prompt or 1
                )
            )
        model = ConfidenceModel().fit(
            cal_feats,
            cal_correct,
            question_ids=[t.question_id for t in cal],
            forbidden_ids=set(splits["eval_500"]),
        )
        model.save(RESULTS / f"confidence-model-{args.label}.pkl")
        conf = model.predict(feats)
        reports["v2_calibrated"] = save_calibration_report(
            RESULTS / f"calibration-{args.label}-v2.json",
            conf,
            correct,
            f"{args.label} v2 (calibrated, {len(cal)} calibration questions)",
        )

    # ---- routing
    curve = routing_curve(conf, correct)
    point = best_operating_point(curve, max_pct_routed=25.0)
    (RESULTS / f"routing-{args.label}.json").write_text(
        json.dumps({"curve": curve, "operating_point": point}, indent=2), encoding="utf-8"
    )

    # ---- failure taxonomy
    failures = []
    for t, ok in zip(traces, correct, strict=True):
        if ok:
            continue
        gold = harness.gold_lines[t.question_id].split("\t")[0]
        rec = harness.by_id[t.question_id]
        failures.append(
            {
                "question_id": t.question_id,
                "db_id": t.db_id,
                "difficulty": rec["difficulty"],
                "category": classify_failure(t, gold),
                "question": rec["question"],
                "predicted_sql": t.sql,
                "gold_sql": gold,
            }
        )
    counts: dict[str, int] = {}
    for f in failures:
        counts[f["category"]] = counts.get(f["category"], 0) + 1
    (RESULTS / f"failures-{args.label}.json").write_text(
        json.dumps(
            {
                "n_failures": len(failures),
                "n_total": len(traces),
                "distribution": dict(sorted(counts.items(), key=lambda kv: -kv[1])),
                "definitions": TAXONOMY,
                "failures": failures,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    _plots(args.label, conf, correct, curve)
    _print_summary(args.label, result, reports, point, counts, len(failures), len(traces))


def per_question_correct(harness: BirdHarness, traces) -> list[int]:
    """Correctness per question.

    The official scorer only reports aggregates, so run it once over the whole
    slice and once per difficulty is not enough -- we need per-question labels
    to fit and evaluate the confidence model. Executes gold and prediction and
    compares result sets the same way the official script does.
    """
    from sqlsentinel.executor import bird_executor

    # Match the official scorer's execution parameters, not the agent's.
    # The agent runs with a 5s timeout and a 5,000-row cap because it is
    # serving queries; the scorer fetches everything with a 30s limit. Scoring
    # with the agent's limits silently marks large or slow-but-valid results
    # wrong and drifts from the official number -- observed as a 3-question
    # disagreement on eval_500 before this was aligned.
    out = []
    for t in traces:
        gold = harness.gold_lines[t.question_id].split("\t")[0]
        ex = bird_executor(harness.db_root, t.db_id, max_rows=10_000_000)
        if not t.sql:
            out.append(0)
            continue
        pred_res = ex.execute(t.sql, timeout_s=OFFICIAL_TIMEOUT_S)
        gold_res = ex.execute(gold, timeout_s=OFFICIAL_TIMEOUT_S)
        out.append(
            int(pred_res.ok and gold_res.ok and pred_res.normalized() == gold_res.normalized())
        )
    return out


def _plots(label, conf, correct, curve) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    from sqlsentinel.confidence import brier_score, expected_calibration_error, reliability_curve

    accent, muted = "#2563eb", "#94a3b8"

    # ---- reliability diagram
    rc = reliability_curve(conf, correct)
    fig, ax = plt.subplots(figsize=(5.4, 5.4))
    ax.plot([0, 1], [0, 1], "--", color=muted, lw=1.3, label="perfect calibration", zorder=1)

    if rc:
        xs = [b["mean_predicted"] for b in rc]
        ys = [b["observed_accuracy"] for b in rc]
        # shading between the curve and the diagonal makes the *direction* of
        # miscalibration readable at a glance: below means overconfident
        ax.fill_between(xs, ys, xs, color=accent, alpha=0.12, zorder=2)
        # marker area encodes bin population, so sparse bins cannot be mistaken
        # for equally well-evidenced ones
        counts = [b["count"] for b in rc]
        scale = max(counts) or 1
        ax.scatter(
            xs,
            ys,
            s=[40 + 320 * c / scale for c in counts],
            color=accent,
            alpha=0.85,
            zorder=4,
            edgecolors="white",
            linewidths=1.2,
        )
        ax.plot(xs, ys, "-", color=accent, lw=2, zorder=3, label="observed")
        for x, y, c in zip(xs, ys, counts, strict=True):
            ax.annotate(
                f"n={c}",
                (x, y),
                textcoords="offset points",
                xytext=(9, -13),
                fontsize=8,
                color="#475569",
            )

    ax.set_xlim(-0.04, 1.04)
    ax.set_ylim(-0.04, 1.04)
    ax.set_xlabel("predicted confidence")
    ax.set_ylabel("observed accuracy")
    ax.set_title(
        f"Reliability — {label}\n"
        f"Brier {brier_score(conf, correct):.3f} · "
        f"ECE {expected_calibration_error(conf, correct):.3f} · n={len(correct)}",
        fontsize=11,
    )
    ax.grid(alpha=0.25, lw=0.6)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=9, loc="upper left")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(RESULTS / f"calibration-{label}.png", dpi=150)
    plt.close(fig)

    # ---- routing curve
    fig, ax = plt.subplots(figsize=(6.4, 4.8))
    xs = [r["pct_routed"] for r in curve]
    ys = [r["pct_errors_caught"] for r in curve]

    # anything above the diagonal beats routing at random
    ax.fill_between(
        xs,
        ys,
        xs,
        where=[y >= x for x, y in zip(xs, ys, strict=True)],
        color=accent,
        alpha=0.12,
        label="better than random",
    )
    ax.plot([0, 100], [0, 100], "--", color=muted, lw=1.3, label="random routing")
    ax.plot(xs, ys, "o-", color=accent, lw=2, ms=5)

    pt = best_operating_point(curve, max_pct_routed=25.0)
    if pt:
        ax.scatter(
            [pt["pct_routed"]],
            [pt["pct_errors_caught"]],
            s=180,
            facecolor="none",
            edgecolor="#dc2626",
            lw=2,
            zorder=5,
        )
        ax.annotate(
            f"routed {pt['pct_routed']:.0f}% → caught {pt['pct_errors_caught']:.0f}%\n"
            f"auto-executed accuracy {pt['auto_accuracy']:.0f}%",
            (pt["pct_routed"], pt["pct_errors_caught"]),
            textcoords="offset points",
            xytext=(14, -32),
            fontsize=9,
            color="#dc2626",
        )

    ax.set_xlim(-2, 102)
    ax.set_ylim(-2, 102)
    ax.set_xlabel("% of queries routed to human review")
    ax.set_ylabel("% of incorrect queries caught")
    ax.set_title(f"Routing — {label}", fontsize=11)
    ax.grid(alpha=0.25, lw=0.6)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=9, loc="lower right")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    fig.tight_layout()
    fig.savefig(RESULTS / f"routing-{label}.png", dpi=150)
    plt.close(fig)


def _print_summary(label, result, reports, point, counts, n_fail, n_total) -> None:
    print(f"\n{'=' * 62}\n{label}\n{'=' * 62}")
    print(f"execution accuracy   {result.accuracy:.1f}% +/- {result.ci95:.1f} (n={result.n})")
    for name, rep in reports.items():
        print(
            f"{name:20s} Brier {rep['brier_score']:.4f}  ECE {rep['expected_calibration_error']:.4f}"
        )
    if point:
        print(
            f"\noperating point      routed {point['pct_routed']:.0f}% of queries, "
            f"caught {point['pct_errors_caught']:.0f}% of errors "
            f"(auto-executed accuracy {point['auto_accuracy']:.1f}%)"
        )
    print(f"\nfailures {n_fail}/{n_total}")
    for cat, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {cat:22s} {n:4d}  ({100 * n / max(n_fail, 1):.0f}%)")


if __name__ == "__main__":
    main()
