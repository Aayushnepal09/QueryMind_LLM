"""Assemble results/RESULTS.md from MLflow runs and the analysis artifacts.

Generated rather than hand-written so the reported numbers cannot drift from
the measured ones. CLAUDE.md section 11: "commit results, not just code" and
"record honest numbers" -- including the rows where a technique made things
worse.

    uv run python scripts/build_report.py
"""

from __future__ import annotations

import json
import math
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS = REPO_ROOT / "results"

# Human-readable descriptions for the run tags produced by run_experiments.sh
TECHNIQUES = {
    "baseline-dev50": "Baseline (ported QueryMind prompt, single-shot)",
    "fewshot3-dev50": "+ few-shot retrieval (k=3 exemplars)",
    "pruned-dev50": "+ schema pruning",
    "selfcorrect-dev50": "+ self-correction (max 2 rounds)",
    "noevidence-dev50": "− BIRD evidence field (ablation)",
    "combined-dev50": "Combined (few-shot + self-correction)",
    "baseline-eval500": "Baseline",
    "final-eval500": "Final (few-shot + self-correction)",
    "k3-eval200": "Final with k=3 self-consistency",
}


def ci95(accuracy_pct: float, n: int) -> float:
    if not n:
        return 0.0
    p, z = accuracy_pct / 100, 1.96
    denom = 1 + z**2 / n
    return 100 * z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom


def load_runs() -> list[dict]:
    try:
        import mlflow
    except ImportError:
        return []
    mlflow.set_tracking_uri(f"sqlite:///{(REPO_ROOT / 'mlflow.db').as_posix()}")
    try:
        df = mlflow.search_runs(experiment_names=["sqlsentinel"])
    except Exception:
        return []
    if df is None or df.empty:
        return []

    runs = []
    for _, r in df.iterrows():
        runs.append(
            {
                "tag": r.get("params.tag") or r.get("params.predictor"),
                "predictor": r.get("params.predictor"),
                "provider": r.get("params.provider"),
                "model": r.get("params.model"),
                "split": r.get("params.split"),
                "n": int(float(r.get("params.n") or 0)),
                "k": int(float(r.get("params.k") or 1)),
                "ex": r.get("metrics.ex_accuracy"),
                "simple": r.get("metrics.ex_simple"),
                "moderate": r.get("metrics.ex_moderate"),
                "challenging": r.get("metrics.ex_challenging"),
                "cost": r.get("metrics.agent_total_cost_usd"),
                "latency": r.get("metrics.agent_mean_latency_s"),
                "empty": r.get("metrics.agent_empty_predictions"),
                "start": r.get("start_time"),
            }
        )
    # keep the newest run per tag
    best: dict[str, dict] = {}
    for run in sorted(runs, key=lambda r: r["start"] or 0):
        if run["tag"]:
            best[run["tag"]] = run
    return list(best.values())


def fmt(v, spec=".1f", dash="—"):
    return dash if v is None or (isinstance(v, float) and math.isnan(v)) else format(v, spec)


def section_headline(runs: dict[str, dict]) -> str:
    base = runs.get("baseline-eval500")
    final = runs.get("final-eval500")
    if not base:
        return "_Headline run not yet complete._\n"

    lines = [
        "| Configuration | Split | n | EX | 95% CI | simple | moderate | challenging |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for key in ("baseline-eval500", "final-eval500"):
        r = runs.get(key)
        if not r:
            continue
        lines.append(
            f"| {TECHNIQUES.get(key, key)} | `{r['split']}` | {r['n']} | "
            f"**{fmt(r['ex'])}%** | ±{fmt(ci95(r['ex'] or 0, r['n']))} | "
            f"{fmt(r['simple'])} | {fmt(r['moderate'])} | {fmt(r['challenging'])} |"
        )
    out = "\n".join(lines)
    if base and final and base["ex"] is not None and final["ex"] is not None:
        delta = final["ex"] - base["ex"]
        margin = ci95(base["ex"], base["n"])
        verdict = (
            "larger than the measurement margin, so it is a real improvement"
            if abs(delta) > margin
            else f"**inside the ±{margin:.1f} margin, so it is not distinguishable from noise**"
        )
        out += f"\n\nDelta: **{delta:+.1f} points** — {verdict}.\n"
    return out


def section_techniques(runs: dict[str, dict]) -> str:
    order = [
        "baseline-dev50",
        "fewshot3-dev50",
        "pruned-dev50",
        "selfcorrect-dev50",
        "combined-dev50",
        "noevidence-dev50",
    ]
    present = [k for k in order if k in runs]
    if not present:
        return "_Ablations not yet complete._\n"

    base_ex = runs.get("baseline-dev50", {}).get("ex")
    lines = [
        "| Technique | EX | Δ vs baseline | mean latency | empty preds |",
        "|---|---:|---:|---:|---:|",
    ]
    for key in present:
        r = runs[key]
        d = "—" if base_ex is None or r["ex"] is None else f"{r['ex'] - base_ex:+.1f}"
        lines.append(
            f"| {TECHNIQUES.get(key, key)} | {fmt(r['ex'])}% | {d} | "
            f"{fmt(r['latency'], '.1f')}s | {fmt(r['empty'], '.0f')} |"
        )
    n = runs.get("baseline-dev50", {}).get("n", 50)
    lines.append("")
    lines.append(
        f"> All rows measured on `dev_50` (n={n}), where the 95% CI is "
        f"±{ci95(base_ex or 50, n):.1f} points. **Deltas smaller than that are not "
        f"significant** — they are reported because negative and null results are "
        f"part of the record, not because they are conclusive."
    )
    return "\n".join(lines)


def section_calibration() -> str:
    files = sorted(RESULTS.glob("calibration-*.json"))
    if not files:
        return "_Calibration not yet computed._\n"
    lines = ["| Scorer | n | Brier ↓ | ECE ↓ | base accuracy |", "|---|---:|---:|---:|---:|"]
    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        lines.append(
            f"| {d['label']} | {d['n']} | {d['brier_score']:.4f} | "
            f"{d['expected_calibration_error']:.4f} | {100 * d['base_accuracy']:.1f}% |"
        )
    return "\n".join(lines)


def section_routing() -> str:
    files = sorted(RESULTS.glob("routing-*.json"))
    if not files:
        return "_Routing curve not yet computed._\n"
    out = []
    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        pt = d.get("operating_point")
        rows = [
            "| threshold | % routed to review | % of errors caught | auto-executed accuracy |",
            "|---:|---:|---:|---:|",
        ]
        for r in d["curve"]:
            if r["n_routed"] == 0 or r["threshold"] > 0.9:
                continue
            rows.append(
                f"| {r['threshold']:.2f} | {r['pct_routed']:.0f}% | "
                f"{r['pct_errors_caught']:.0f}% | {fmt(r['auto_accuracy'])}% |"
            )
        out.append(f"**{f.stem.replace('routing-', '')}**\n\n" + "\n".join(rows))
        if pt:
            out.append(
                f"\n> At threshold {pt['threshold']:.2f}: routed "
                f"**{pt['pct_routed']:.0f}%** of queries to review, catching "
                f"**{pt['pct_errors_caught']:.0f}%** of all incorrect queries. "
                f"Everything auto-executed was {fmt(pt['auto_accuracy'])}% correct."
            )
    return "\n\n".join(out)


def section_failures() -> str:
    files = sorted(RESULTS.glob("failures-*.json"))
    if not files:
        return "_Failure taxonomy not yet computed._\n"
    d = json.loads(files[-1].read_text(encoding="utf-8"))
    lines = [
        f"{d['n_failures']} incorrect of {d['n_total']} scored.\n",
        "| Category | Count | Share | What it means |",
        "|---|---:|---:|---|",
    ]
    for cat, n in d["distribution"].items():
        lines.append(
            f"| `{cat}` | {n} | {100 * n / max(d['n_failures'], 1):.0f}% | "
            f"{d['definitions'].get(cat, '')} |"
        )
    return "\n".join(lines)


def section_cost(runs: dict[str, dict]) -> str:
    lines = [
        "| Run | Provider | Model | n | EX | nominal cost | mean latency |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    any_row = False
    for tag, r in sorted(runs.items()):
        if r["ex"] is None or r["provider"] in (None, "none"):
            continue
        any_row = True
        lines.append(
            f"| `{tag}` | {r['provider']} | `{r['model']}` | {r['n']} | "
            f"{fmt(r['ex'])}% | ${fmt(r['cost'], '.4f')} | {fmt(r['latency'], '.1f')}s |"
        )
    if not any_row:
        return "_No agent runs recorded yet._\n"
    lines.append("")
    lines.append(
        "> Nominal cost is what the run *would* cost at list price. Actual spend "
        "was **$0**: the local model is free and Gemini ran on its free tier."
    )
    return "\n".join(lines)


def main() -> None:
    runs = {r["tag"]: r for r in load_runs() if r["tag"]}

    doc = f"""# Results

Generated by `scripts/build_report.py` from MLflow runs and the artifacts in
`results/`. Do not edit by hand — regenerate it.

BIRD version `dev_20240627`. Evaluation split `eval_500` (fixed, seeded,
stratified by difficulty × database); `dev_50` is the fast development loop and
a strict subset of it. The confidence model is fitted only on `calib`, which is
disjoint from `eval_500` by construction.

## Headline: before and after

{section_headline(runs)}

## Per-technique deltas

{section_techniques(runs)}

## Confidence calibration

{section_calibration()}

Reliability diagrams: `results/calibration-*.png`

## Routing — the money metric

{section_routing()}

Routing curves: `results/routing-*.png`

## Cost and latency

{section_cost(runs)}

## Failure taxonomy

{section_failures()}

Category definitions and the limits of the classifier: `docs/failure-taxonomy.md`.
Every failure with its question, prediction and gold SQL: `results/failures-*.json`.

## Reference points

| | EX |
|---|---:|
| Stub predictor (`SELECT 1`) — the chance floor | 3.0% |
| Published single-shot GPT-4-class results on BIRD dev | ~45–55% |

The chance floor is not zero: BIRD scores by result-set equality and some gold
queries return a scalar `1`. See `results/baseline-floor.md`.
"""
    (RESULTS / "RESULTS.md").write_text(doc, encoding="utf-8")
    print(f"wrote {RESULTS / 'RESULTS.md'} ({len(runs)} runs)")


if __name__ == "__main__":
    main()
