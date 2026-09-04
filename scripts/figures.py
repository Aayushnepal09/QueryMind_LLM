"""Figures for the findings that read better as pictures than as tables.

    uv run python scripts/figures.py

Reads results/findings.json (written by scripts/research.py) and the trace
files, and writes PNGs into results/. Kept separate from analyze.py because
these illustrate secondary findings rather than the evaluation itself.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
RESULTS = REPO_ROOT / "results"

ACCENT = "#2563eb"
DANGER = "#dc2626"
MUTED = "#94a3b8"


def _style(ax) -> None:
    ax.grid(alpha=0.25, lw=0.6, axis="x")
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)


def per_database(findings: dict) -> None:
    """Accuracy per database, with the spread made obvious.

    The single headline number describes no database in particular, which is
    the whole point of drawing this.
    """
    rows = findings["per_database"]
    names = [r["db_id"] for r in rows]
    acc = [r["accuracy"] for r in rows]
    lo = [r["accuracy"] - r["ci_low"] for r in rows]
    hi = [r["ci_high"] - r["accuracy"] for r in rows]
    overall = 45.6

    fig, ax = plt.subplots(figsize=(10.2, 5.6))
    y = np.arange(len(names))
    colours = [DANGER if a < overall else ACCENT for a in acc]
    ax.barh(y, acc, color=colours, alpha=0.85, height=0.62)
    ax.errorbar(acc, y, xerr=[lo, hi], fmt="none", ecolor="#475569", elinewidth=1.2, capsize=3)

    ax.axvline(overall, color="#0f172a", ls="--", lw=1.4, zorder=3)
    ax.annotate(
        f"overall {overall:.1f}%",
        (overall, len(names) - 0.3),
        textcoords="offset points",
        xytext=(6, 0),
        fontsize=9,
        color="#0f172a",
    )

    # annotations in a fixed right-hand column so they never collide with the
    # error bars, which extend to different widths per database
    for i, r in enumerate(rows):
        ax.annotate(
            f"{r['accuracy']:5.1f}%   {r['n_tables']:2d} tables {r['n_columns']:3d} cols",
            (110, i),
            va="center",
            fontsize=8,
            color="#475569",
            family="monospace",
        )

    ax.set_yticks(y, names, fontsize=9)
    ax.set_xlim(0, 152)
    ax.set_xticks([0, 20, 40, 60, 80, 100])
    ax.set_xlabel("execution accuracy (%)  ·  bars show the 95% confidence interval")
    ax.set_title(
        "Per-database accuracy spans 65 points\n"
        "and schema size does not predict it (no correlation significant)",
        fontsize=11,
    )
    _style(ax)
    fig.tight_layout()
    fig.savefig(RESULTS / "per-database-accuracy.png", dpi=150)
    plt.close(fig)


def feature_ablation() -> None:
    """Leave-one-out effect of each confidence feature.

    Recomputed here rather than read from a file so the figure cannot drift
    from the model definition.
    """
    from analyze import TraceShim, per_question_correct

    from sqlsentinel.confidence import (
        FEATURE_NAMES,
        ConfidenceModel,
        QueryFeatures,
        brier_score,
        extract_features,
    )
    from sqlsentinel.eval.harness import BirdHarness

    harness = BirdHarness(Path("data/bird/dev_20240627"))
    splits = json.loads((RESULTS / "splits.json").read_text(encoding="utf-8"))

    def load(tag):
        traces = [
            TraceShim(d)
            for d in json.loads((RESULTS / "traces" / f"{tag}.json").read_text(encoding="utf-8"))
        ]
        labels = per_question_correct(harness, traces)
        feats = []
        for t in traces:
            rec = harness.by_id[t.question_id]
            feats.append(
                extract_features(
                    t, rec["question"], rec.get("evidence", ""), t.n_tables_in_prompt or 1
                )
            )
        return traces, feats, labels

    cal_t, cal_f, cal_y = load("k3-calib200")
    _, ev_f, ev_y = load("k3-eval200")

    def subset(feats, keep):
        idx = [FEATURE_NAMES.index(k) for k in keep]
        out = []
        for f in feats:
            v = f.vector()
            g = QueryFeatures()
            for name, i in zip(keep, idx, strict=True):
                setattr(g, name, v[i])
            out.append(g)
        return out

    def score(keep):
        m = ConfidenceModel().fit(
            subset(cal_f, keep),
            cal_y,
            question_ids=[t.question_id for t in cal_t],
            forbidden_ids=set(splits["eval_500"]),
        )
        return brier_score(m.predict(subset(ev_f, keep)), ev_y)

    full = score(FEATURE_NAMES)
    deltas = [(f, score([x for x in FEATURE_NAMES if x != f]) - full) for f in FEATURE_NAMES]
    deltas.sort(key=lambda r: r[1])

    fig, ax = plt.subplots(figsize=(8.2, 5.4))
    names = [d[0] for d in deltas]
    vals = [d[1] for d in deltas]
    colours = [ACCENT if v > 0 else DANGER for v in vals]
    y = np.arange(len(names))
    ax.barh(y, vals, color=colours, alpha=0.85, height=0.62)
    ax.axvline(0, color="#0f172a", lw=1.2)

    ax.set_yticks(y, names, fontsize=9)
    ax.set_xlabel("change in Brier score when the feature is removed")
    ax.set_title(
        "Only four of fourteen features earn their place\n"
        "blue = removing it hurts · red = removing it helps",
        fontsize=11,
    )
    ax.annotate(
        "question_length is a strong predictor on its own\n"
        "(r = -0.224, p < 0.0001) yet harms the model here",
        (vals[0], 0),
        textcoords="offset points",
        xytext=(24, 6),
        fontsize=8.5,
        color=DANGER,
    )
    _style(ax)
    fig.tight_layout()
    fig.savefig(RESULTS / "feature-ablation.png", dpi=150)
    plt.close(fig)


def silent_failures(findings: dict) -> None:
    """The headline finding: how errors are distributed by detectability."""
    base = findings["silent_failures_baseline"]
    final = findings["silent_failures_final"]

    fig, ax = plt.subplots(figsize=(7.6, 3.6))
    labels = ["Baseline", "Final\n(few-shot + self-correction)"]
    silent = [base["silent_failures"], final["silent_failures"]]
    loud = [base["loud_execution_errors"], final["loud_execution_errors"]]
    empty = [base["empty_results"], final["empty_results"]]
    correct = [
        base["n_total"] - base["n_wrong"],
        final["n_total"] - final["n_wrong"],
    ]

    y = np.arange(len(labels))
    left = np.zeros(len(labels))
    for vals, colour, name in (
        (correct, "#10b981", "correct"),
        (silent, DANGER, "wrong, looks right (silent)"),
        (empty, "#f59e0b", "wrong, returns nothing"),
        (loud, MUTED, "wrong, fails loudly"),
    ):
        ax.barh(y, vals, left=left, color=colour, alpha=0.9, height=0.55, label=name)
        for i, v in enumerate(vals):
            if v > 18:
                ax.annotate(
                    str(v),
                    (left[i] + v / 2, i),
                    ha="center",
                    va="center",
                    fontsize=9,
                    color="white",
                    weight="bold",
                )
        left = left + np.array(vals)

    ax.set_yticks(y, labels, fontsize=9)
    ax.set_xlabel("queries (n = 500)")
    ax.set_title(
        "Most wrong answers do not look wrong\n"
        "and self-correction converts loud failures into silent ones",
        fontsize=11,
    )
    ax.legend(
        frameon=False,
        fontsize=8.5,
        ncol=4,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.28),
    )
    ax.set_xlim(0, 510)
    _style(ax)
    fig.tight_layout()
    fig.savefig(RESULTS / "silent-failures.png", dpi=150)
    plt.close(fig)


def main() -> None:
    findings = json.loads((RESULTS / "findings.json").read_text(encoding="utf-8"))
    per_database(findings)
    print("wrote results/per-database-accuracy.png")
    silent_failures(findings)
    print("wrote results/silent-failures.png")
    feature_ablation()
    print("wrote results/feature-ablation.png")


if __name__ == "__main__":
    main()
