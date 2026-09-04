"""Wrapper around BIRD's official execution-accuracy script.

CLAUDE.md section 4 is explicit: wrap the official script, do not reimplement
it. Result-set comparison has ordering and type edge cases the official script
already handles, and a home-grown metric would not be comparable to anything.

The official script (third_party/evaluation.py) has a fussy contract:

  * --predicted_sql_path and --ground_truth_path are DIRECTORY PREFIXES, not
    files. It appends 'predict_dev.json' and 'dev_gold.sql' respectively.
  * Predictions are a dict keyed by stringified index, values formatted as
    "<sql>\t----- bird -----\t<db_id>".
  * Gold is one "<sql>\t<db_id>" per line.
  * --diff_json_path is a dev.json-shaped file supplying difficulty labels.
  * All three are INDEX-ALIGNED positionally. Evaluating a subset therefore
    means materialising a consistent, renumbered slice of all three.

This module builds that slice in a temp directory, shells out, and parses the
result back into a structured object.
"""

from __future__ import annotations

import json
import math
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
OFFICIAL_SCRIPT = REPO_ROOT / "third_party" / "evaluation.py"
DELIM = "\t----- bird -----\t"


@dataclass
class EvalResult:
    """Accuracy broken down by BIRD's difficulty labels."""

    n: int
    accuracy: float  # percent, 0-100
    simple: float
    moderate: float
    challenging: float
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def ci95(self) -> float:
        """Half-width of the 95% Wilson interval, in percentage points.

        Reported alongside every accuracy number. At n=500 this is ~4.4pp;
        quoting a bare accuracy without it invites claiming deltas that are
        inside the noise.
        """
        if self.n == 0:
            return 0.0
        p, z = self.accuracy / 100, 1.96
        denom = 1 + z**2 / self.n
        margin = z * math.sqrt(p * (1 - p) / self.n + z**2 / (4 * self.n**2)) / denom
        return margin * 100

    def __str__(self) -> str:
        return (
            f"EX {self.accuracy:.1f}% +/- {self.ci95:.1f} (n={self.n})  "
            f"simple {self.simple:.1f} | moderate {self.moderate:.1f} | "
            f"challenging {self.challenging:.1f}"
        )


class BirdHarness:
    def __init__(self, bird_root: Path):
        self.bird_root = Path(bird_root)
        self.dev_json = self.bird_root / "dev.json"
        self.gold_sql = self.bird_root / "dev.sql"
        self.db_root = self.bird_root / "dev_databases"

        for p in (self.dev_json, self.gold_sql, self.db_root):
            if not p.exists():
                raise FileNotFoundError(f"BIRD dev set incomplete: {p} not found")

        self.records = json.loads(self.dev_json.read_text(encoding="utf-8"))
        self.by_id = {r["question_id"]: r for r in self.records}
        # gold is positionally aligned with dev.json, not keyed
        self.gold_lines = self.gold_sql.read_text(encoding="utf-8").splitlines()

    def questions(self, question_ids: list[int]) -> list[dict]:
        return [self.by_id[i] for i in question_ids]

    def evaluate(
        self,
        predictions: dict[int, str],
        question_ids: list[int],
        num_cpus: int = 4,
        timeout: float = 30.0,
    ) -> EvalResult:
        """Score `predictions` (question_id -> SQL) over `question_ids`."""
        missing = set(question_ids) - set(predictions)
        if missing:
            raise ValueError(f"{len(missing)} predictions missing, e.g. {sorted(missing)[:5]}")

        # The official script computes per-difficulty accuracy by dividing by the
        # size of each difficulty bucket, with no empty-bucket guard -- an
        # unstratified slice missing one difficulty crashes it with an opaque
        # ZeroDivisionError. Fail early and legibly instead. Always use
        # subsets.stratified_subsample() rather than truncating a split.
        present = {self.by_id[q]["difficulty"] for q in question_ids}
        if missing_diff := {"simple", "moderate", "challenging"} - present:
            raise ValueError(
                f"slice has no {'/'.join(sorted(missing_diff))} questions; the official "
                f"BIRD script divides by difficulty-bucket size and would crash. "
                f"Use a stratified subsample."
            )

        with tempfile.TemporaryDirectory() as td:
            work = Path(td)

            # Renumber to a dense 0..n-1 index; all three files must agree.
            pred_obj, gold_out, diff_out = {}, [], []
            for new_idx, qid in enumerate(question_ids):
                rec = self.by_id[qid]
                sql = " ".join(predictions[qid].split()) or "SELECT 1"
                pred_obj[str(new_idx)] = f"{sql}{DELIM}{rec['db_id']}"
                gold_out.append(self.gold_lines[qid])
                diff_out.append(rec)

            (work / "predict_dev.json").write_text(json.dumps(pred_obj), encoding="utf-8")
            (work / "dev_gold.sql").write_text("\n".join(gold_out) + "\n", encoding="utf-8")
            (work / "dev_diff.json").write_text(json.dumps(diff_out), encoding="utf-8")

            # trailing separator: the script concatenates rather than joins
            prefix = f"{work.as_posix()}/"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(OFFICIAL_SCRIPT),
                    "--predicted_sql_path",
                    prefix,
                    "--ground_truth_path",
                    prefix,
                    "--db_root_path",
                    f"{self.db_root.as_posix()}/",
                    "--data_mode",
                    "dev",
                    "--diff_json_path",
                    str(work / "dev_diff.json"),
                    "--num_cpus",
                    str(num_cpus),
                    "--meta_time_out",
                    str(timeout),
                ],
                capture_output=True,
                text=True,
            )

        if proc.returncode != 0:
            raise RuntimeError(f"official BIRD eval failed:\n{proc.stdout}\n{proc.stderr}")
        return self._parse(proc.stdout, len(question_ids))

    @staticmethod
    def _parse(stdout: str, n: int) -> EvalResult:
        counts = re.search(r"^count\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)", stdout, re.M)
        acc = re.search(r"^accuracy\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", stdout, re.M)
        if not acc:
            raise RuntimeError(f"could not parse eval output:\n{stdout}")

        s, m, c, total = (float(x) for x in acc.groups())
        cd = {}
        if counts:
            cs, cm, cc, ct = (int(x) for x in counts.groups())
            cd = {"simple": cs, "moderate": cm, "challenging": cc, "total": ct}
        return EvalResult(n=n, accuracy=total, simple=s, moderate=m, challenging=c, counts=cd)
