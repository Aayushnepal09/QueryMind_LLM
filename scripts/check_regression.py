"""Fail the build when execution accuracy regresses (CLAUDE.md section 8).

Compares the accuracy printed by `python -m sqlsentinel.eval` against the
recorded baseline in results/baseline.json and exits non-zero if it dropped by
more than the allowed margin.

The margin is not arbitrary. On the 50-question CI subset the 95% CI is about
+/-13 points, so a 3-point drop is well inside the noise and this gate cannot
distinguish a real regression from sampling variation at that size. It exists
to catch *breakage* -- a prompt change that empties predictions, a parser
returning "", a provider misconfiguration -- which shows up as a large drop.
Treat it as a smoke alarm, not a measurement.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_FILE = REPO_ROOT / "results" / "baseline.json"
MAX_DROP_POINTS = 3.0


def parse_accuracy(text: str) -> float:
    m = re.search(r"EX\s+([\d.]+)%", text)
    if not m:
        raise SystemExit(f"could not find an accuracy in the eval output:\n{text}")
    return float(m.group(1))


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: check_regression.py <eval-output-file>")

    current = parse_accuracy(Path(sys.argv[1]).read_text(encoding="utf-8"))

    if not BASELINE_FILE.exists():
        print(f"No baseline recorded yet; writing {current:.1f}% as the baseline.")
        BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE_FILE.write_text(
            json.dumps({"split": "dev_50", "ex_accuracy": current}, indent=2),
            encoding="utf-8",
        )
        return 0

    baseline = json.loads(BASELINE_FILE.read_text(encoding="utf-8"))["ex_accuracy"]
    drop = baseline - current

    print(f"baseline {baseline:.1f}%  current {current:.1f}%  delta {-drop:+.1f}pp")

    if drop > MAX_DROP_POINTS:
        print(
            f"::error::Execution accuracy fell {drop:.1f} points "
            f"(limit {MAX_DROP_POINTS}). Blocking the merge."
        )
        return 1

    print("Within tolerance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
