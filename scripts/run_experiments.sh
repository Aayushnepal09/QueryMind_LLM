#!/usr/bin/env bash
# Sequential experiment sweep.
#
# Runs one at a time on purpose: the local model has one GPU, so concurrent
# runs thrash rather than parallelise. Every call is cached, so re-running this
# script after an interruption resumes almost instantly rather than repeating
# work already paid for.
set -u

cd "$(dirname "$0")/.." || exit 1
mkdir -p results/traces logs

run() {
  local tag="$1"; shift
  if [ -f "results/traces/${tag}.json" ]; then
    echo "[skip] $tag (traces already present)"
    return
  fi
  echo "[run ] $tag :: $*"
  uv run python -m sqlsentinel.eval \
    --predictor agent --workers 1 --tag "$tag" \
    --dump-traces "results/traces/${tag}.json" "$@" \
    > "logs/${tag}.log" 2>&1
  echo "[done] $tag :: $(grep -aoE 'EX [0-9.]+% \+/- [0-9.]+ \(n=[0-9]+\)' "logs/${tag}.log" | head -1)"
}

# ---- Phase 2: per-technique ablations on the fast dev loop
run baseline-dev50    --split dev_50 --provider ollama
run fewshot3-dev50    --split dev_50 --provider ollama --few-shot 3
run pruned-dev50      --split dev_50 --provider ollama --prune-schema
run selfcorrect-dev50 --split dev_50 --provider ollama --max-corrections 2
run noevidence-dev50  --split dev_50 --provider ollama --no-evidence

# ---- combined best-guess configuration
run combined-dev50    --split dev_50 --provider ollama --few-shot 3 --max-corrections 2

# ---- Phase 3: k-sample runs for the confidence signal.
# calib slice trains the model, eval slice reports it. Disjoint by construction.
run k3-calib200 --split calib    --subset 200 --provider ollama --k 3 --max-corrections 2 --few-shot 3
run k3-eval200  --split eval_500 --subset 200 --provider ollama --k 3 --max-corrections 2 --few-shot 3

# ---- Phase 1/5: the headline before/after on the full reporting split
run baseline-eval500 --split eval_500 --provider ollama
run final-eval500    --split eval_500 --provider ollama --few-shot 3 --max-corrections 2

echo "ALL EXPERIMENTS COMPLETE"
