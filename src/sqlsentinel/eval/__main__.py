"""CLI entry point: `python -m sqlsentinel.eval`.

Phase 0 gate (CLAUDE.md section 8): one command produces an accuracy number.
Phase 1 swaps --predictor stub for a real agent; nothing else here changes.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from sqlsentinel.agent import BaselineAgent
from sqlsentinel.eval.harness import BirdHarness
from sqlsentinel.eval.subsets import build_splits, stratified_subsample

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[3]
SPLITS_FILE = REPO_ROOT / "results" / "splits.json"


def stub_predictor(questions: list[dict]) -> dict[int, str]:
    """Always emits `SELECT 1`. Should score ~0% and must not crash.

    This is the harness's own smoke test: it proves the plumbing end to end
    (subset -> prediction file -> official script -> parsed number) without
    any model in the loop.
    """
    return {q["question_id"]: "SELECT 1" for q in questions}


PREDICTORS = {"stub": stub_predictor, "baseline": None}  # baseline built at runtime


def load_split(name: str, bird_root: Path) -> list[int]:
    if SPLITS_FILE.exists():
        data = json.loads(SPLITS_FILE.read_text(encoding="utf-8"))
        if name in data:
            return data[name]
    splits = build_splits(bird_root / "dev.json")
    if name not in splits:
        raise SystemExit(f"unknown split '{name}'")
    return splits[name]


def main() -> None:
    ap = argparse.ArgumentParser(prog="sqlsentinel.eval")
    ap.add_argument("--split", default="dev_50",
                    choices=["dev_50", "eval_500", "calib", "full"],
                    help="which committed split to evaluate (default: dev_50)")
    ap.add_argument("--subset", type=int, default=None,
                    help="stratified subsample of N questions from the split")
    ap.add_argument("--predictor", default="stub", choices=sorted(PREDICTORS))
    ap.add_argument("--provider", default=None, choices=["ollama", "gemini"],
                    help="LLM provider (default: SQLSENTINEL_PROVIDER)")
    ap.add_argument("--workers", type=int, default=1,
                    help="concurrent generations; keep at 1 for ollama")
    ap.add_argument("--no-evidence", action="store_true",
                    help="ablate BIRD's evidence field")
    ap.add_argument("--bird-root", default=os.getenv("BIRD_DEV_ROOT", "data/bird/dev_20240627"))
    ap.add_argument("--num-cpus", type=int, default=4)
    ap.add_argument("--no-mlflow", action="store_true")
    args = ap.parse_args()

    bird_root = Path(args.bird_root)
    harness = BirdHarness(bird_root)

    if args.split == "full":
        qids = [r["question_id"] for r in harness.records]
    else:
        qids = load_split(args.split, bird_root)
    if args.subset:
        qids = stratified_subsample(harness.questions(qids), args.subset)

    questions = harness.questions(qids)
    print(f"predictor={args.predictor} split={args.split} n={len(qids)}")

    agent = None
    t0 = time.time()
    if args.predictor == "stub":
        predictions = stub_predictor(questions)
    else:
        from sqlsentinel.llm import get_client

        client = get_client(args.provider)
        print(f"provider={client.provider} model={client.model} workers={args.workers}")
        agent = BaselineAgent(
            client=client,
            db_root=bird_root / "dev_databases",
            use_evidence=not args.no_evidence,
        )
        predictions = agent.predict(questions, workers=args.workers)
    gen_s = time.time() - t0

    t1 = time.time()
    result = harness.evaluate(predictions, qids, num_cpus=args.num_cpus)
    eval_s = time.time() - t1

    print(f"\n{result}")
    print(f"generation {gen_s:.1f}s | scoring {eval_s:.1f}s")

    if not args.no_mlflow:
        import mlflow

        # MLflow deprecated the plain-file store; use the local sqlite backend.
        # Still zero-infrastructure, still gitignored, but not in maintenance mode.
        mlflow.set_tracking_uri(f"sqlite:///{(REPO_ROOT / 'mlflow.db').as_posix()}")
        mlflow.set_experiment("sqlsentinel")
        with mlflow.start_run(run_name=f"{args.predictor}-{args.split}-n{len(qids)}"):
            mlflow.log_params({
                "predictor": args.predictor,
                "provider": getattr(agent.client, "provider", "none") if agent else "none",
                "model": getattr(agent.client, "model", "none") if agent else "none",
                "use_evidence": bool(agent.use_evidence) if agent else False,
                "split": args.split,
                "n": len(qids),
                "bird_version": bird_root.name,
            })
            mlflow.log_metrics({
                "ex_accuracy": result.accuracy,
                "ex_ci95": result.ci95,
                "ex_simple": result.simple,
                "ex_moderate": result.moderate,
                "ex_challenging": result.challenging,
                "generation_seconds": gen_s,
                "scoring_seconds": eval_s,
                **({f"agent_{k}": v for k, v in agent.summary().items()} if agent else {}),
            })
        print("logged to MLflow")


if __name__ == "__main__":
    main()
