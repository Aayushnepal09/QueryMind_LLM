"""Paired comparison of every technique run against the baseline.

    uv run python scripts/compare.py --baseline baseline-dev50

Writes results/comparisons.json and prints a table. Uses McNemar rather than
comparing independent confidence intervals: the runs share their questions, and
throwing away that pairing costs enough power to hide real effects (see
src/sqlsentinel/eval/stats.py).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from analyze import TraceShim, per_question_correct  # noqa: E402

from sqlsentinel.eval.harness import BirdHarness  # noqa: E402
from sqlsentinel.eval.stats import mcnemar, wilson_interval  # noqa: E402

RESULTS = REPO_ROOT / "results"
TRACES = RESULTS / "traces"


def labels(harness: BirdHarness, path: Path) -> dict[int, int]:
    traces = [TraceShim(d) for d in json.loads(path.read_text(encoding="utf-8"))]
    return dict(
        zip(
            [t.question_id for t in traces],
            per_question_correct(harness, traces),
            strict=True,
        )
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", default="baseline-dev50")
    ap.add_argument("--bird-root", default="data/bird/dev_20240627")
    args = ap.parse_args()

    harness = BirdHarness(Path(args.bird_root))
    base_path = TRACES / f"{args.baseline}.json"
    if not base_path.exists():
        raise SystemExit(f"baseline traces not found: {base_path}")

    base = labels(harness, base_path)
    n_base = sum(base.values())
    lo, hi = wilson_interval(n_base, len(base))
    print(
        f"baseline {args.baseline}: {100 * n_base / len(base):.1f}% "
        f"CI [{lo:.1f}, {hi:.1f}] (n={len(base)})\n"
    )

    rows = []
    for path in sorted(TRACES.glob("*.json")):
        tag = path.stem
        if tag == args.baseline:
            continue
        try:
            other = labels(harness, path)
        except Exception as e:
            print(f"  {tag}: skipped ({type(e).__name__})")
            continue
        if len(set(other) & set(base)) < 10:
            continue

        cmp = mcnemar(base, other, label_a=args.baseline, label_b=tag)
        acc = 100 * sum(other[i] for i in other) / len(other)
        rows.append(
            {
                "tag": tag,
                "accuracy": acc,
                "n": cmp.n,
                "delta_points": cmp.delta_points,
                "helped": cmp.b_only,
                "hurt": cmp.a_only,
                "discordant": cmp.discordant,
                "p_value": cmp.p_value,
                "significant": cmp.significant,
            }
        )

    rows.sort(key=lambda r: -r["delta_points"])
    (RESULTS / "comparisons.json").write_text(
        json.dumps({"baseline": args.baseline, "comparisons": rows}, indent=2),
        encoding="utf-8",
    )

    print(f"{'run':24s} {'EX':>7s} {'delta':>7s} {'help':>5s} {'hurt':>5s} {'p':>8s}  verdict")
    print("-" * 78)
    for r in rows:
        mark = "**" if r["significant"] else "  "
        print(
            f"{r['tag']:24s} {r['accuracy']:6.1f}% {r['delta_points']:+6.1f} "
            f"{r['helped']:5d} {r['hurt']:5d} {r['p_value']:8.4f}  "
            f"{mark}{'significant' if r['significant'] else 'not distinguishable'}"
        )
    print(f"\nwrote {RESULTS / 'comparisons.json'}")


if __name__ == "__main__":
    main()
