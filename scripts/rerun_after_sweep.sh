#!/usr/bin/env bash
# Waits for the main sweep to finish, then re-runs the calibration slice with
# the retrieval leakage guard in place. Kept separate from run_experiments.sh
# because bash reads a running script incrementally -- editing one mid-run
# corrupts execution.
set -u
cd "$(dirname "$0")/.." || exit 1
SWEEP="$1"

until grep -aq "ALL EXPERIMENTS COMPLETE" "$SWEEP" 2>/dev/null; do sleep 60; done
echo "[chain] main sweep finished; re-running k3-calib200 clean"

uv run python -m sqlsentinel.eval \
  --split calib --subset 200 --provider ollama --workers 1 \
  --k 3 --max-corrections 2 --few-shot 3 \
  --tag k3-calib200-clean \
  --dump-traces results/traces/k3-calib200.json > logs/k3-calib200-clean.log 2>&1

echo "[chain] done :: $(grep -aoE 'EX [0-9.]+% \+/- [0-9.]+ \(n=[0-9]+\)' logs/k3-calib200-clean.log | head -1)"
echo "CHAIN COMPLETE"
