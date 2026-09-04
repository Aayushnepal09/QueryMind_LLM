"""Build results/review_queue.json from a trace file, for the review UI.

    uv run python scripts/build_queue.py results/traces/k3-eval200.json

Applies the same router used in the API, so the queue contains exactly what the
system would route to a human in production -- not a hand-picked sample.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from analyze import TraceShim  # noqa: E402

from sqlsentinel.confidence import agreement_confidence, extract_features  # noqa: E402
from sqlsentinel.eval.harness import BirdHarness  # noqa: E402
from sqlsentinel.router import Decision, Router  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("traces")
    ap.add_argument("--threshold", type=float, default=0.7)
    ap.add_argument("--bird-root", default="data/bird/dev_20240627")
    ap.add_argument("--out", default="results/review_queue.json")
    args = ap.parse_args()

    harness = BirdHarness(Path(args.bird_root))
    router = Router(threshold=args.threshold)
    traces = [TraceShim(d) for d in json.loads(Path(args.traces).read_text(encoding="utf-8"))]

    queue = []
    for t in traces:
        rec = harness.by_id[t.question_id]
        feats = extract_features(
            t, rec["question"], rec.get("evidence", ""), t.n_tables_in_prompt or 1
        )
        conf = agreement_confidence(feats)
        decision = router.route(
            t.sql,
            conf,
            row_count=t.result_row_count,
            executed_ok=t.executed_ok,
            n_tables=t.n_tables_in_prompt,
        )
        if decision.decision is Decision.REVIEW:
            queue.append(
                {
                    "question_id": t.question_id,
                    "db_id": t.db_id,
                    "question": rec["question"],
                    "evidence": rec.get("evidence", ""),
                    "sql": t.sql,
                    "confidence": round(conf, 4),
                    "n_candidates": t.n_candidates,
                    "reasons": decision.reasons,
                }
            )

    queue.sort(key=lambda d: d["confidence"])
    Path(args.out).write_text(json.dumps(queue, indent=1), encoding="utf-8")
    print(f"{len(queue)} of {len(traces)} routed to review ({100 * len(queue) / len(traces):.0f}%)")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
