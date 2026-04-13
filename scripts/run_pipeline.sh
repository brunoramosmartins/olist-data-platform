#!/usr/bin/env bash
# Full end-to-end pipeline: raw load → dbt → features export → train → predict →
# fct_predictions → monitor → retrain (if triggered).
# Run from repository root: bash scripts/run_pipeline.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

step() {
  echo ""
  echo "=== $1 ==="
}

run_timed() {
  local label="$1"
  shift
  local start end elapsed
  start="$(date +%s)"
  "$@"
  end="$(date +%s)"
  elapsed=$((end - start))
  echo "(timing) ${label}: ${elapsed}s"
}

step "Load raw CSVs into DuckDB"
run_timed "load-data" make load-data

step "dbt build (full project)"
run_timed "dbt-build" make dbt-build

step "Export ML features Parquet"
run_timed "export-features" make export-features

step "Train baseline model"
run_timed "ml-train" make ml-train

step "Batch predictions → DuckDB ml_predictions"
run_timed "ml-predict" make ml-predict

step "dbt: refresh fct_predictions"
run_timed "dbt-fct_predictions" bash -c 'cd dbt_project && dbt build --select "+fct_predictions"'

step "Monitoring report"
run_timed "ml-monitor" python ml/monitor.py

step "Retrain (if triggers fire)"
run_timed "ml-retrain" python ml/retrain.py

echo ""
echo "=== Pipeline complete ==="
