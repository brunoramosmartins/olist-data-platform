# Runbook

## Prerequisites

- Python 3.11+
- [Kaggle CLI](https://github.com/Kaggle/kaggle-api) configured with API token

## Initial Setup

```bash
# 1. Clone the repository
git clone https://github.com/<your-user>/olist-data-platform.git
cd olist-data-platform

# 2. Set up the environment
bash scripts/setup_env.sh

# 3. Activate the virtual environment
source .venv/bin/activate

# 4. Download the Olist dataset
bash scripts/download_data.sh
```

## Running the Pipeline

### One command (recommended)

From the repo root (bash available — **Git Bash** / **macOS** / **Linux**):

```bash
make pipeline
```

This runs `scripts/run_pipeline.sh`, which executes in order:

1. `make load-data` — CSVs from `data/raw/` into DuckDB  
2. `make dbt-build` — full dbt project  
3. `make export-features` — `dbt build --select "+fct_order_features"` + `scripts/export_features.py`  
4. `make ml-train` — `ml/train.py`  
5. `make ml-predict` — `ml/predict.py`  
6. `dbt build --select "+fct_predictions"` — refresh the predictions mart  
7. `python ml/monitor.py` — monitoring + drift JSON/MD  
8. `python ml/retrain.py` — triggers, optional refit + comparison + promotion  

Each major step prints a **wall-clock timing** line `(timing) …: Ns` for rough benchmarking.

### Individual targets

```bash
make dbt-build # Build all dbt models
make dbt-test        # Run dbt tests only
make export-features # Feature mart + Parquet export
make ml-train        # Train the ML model
make ml-predict      # Run batch predictions
make ml-evaluate     # Test ROC-AUC from current_model.yaml
```

### Without `run_pipeline.sh` (manual)

From repository root, after activating the venv:

```bash
make load-data
cd dbt_project && dbt build && cd ..
make export-features
python ml/train.py
python ml/predict.py
cd dbt_project && dbt build --select "+fct_predictions"
python ml/monitor.py
python ml/retrain.py
```

**Inference (Phase 7):** `python ml/predict.py` reads `ml/current_model.yaml`, loads the joblib pipeline, scores `data/ml/features.parquet`, writes `ml/predictions/predictions_{version}_{date}.parquet` and replaces DuckDB table `main.ml_predictions`. Then build `fct_predictions` so predictions join `fact_orders` in dbt.

### Example: actuals vs predictions (DuckDB / dbt)

After `predict.py` and `dbt build --select fct_predictions`:

```sql
SELECT fp.order_date,
    fp.actual_is_delayed,
    fp.predicted_class,
    fp.predicted_probability,
    fp.model_version
FROM fct_predictions fp
WHERE fp.actual_is_delayed IS NOT NULL
LIMIT 20;
```

Use this for calibration, error analysis, and monitoring delayed vs predicted-delayed rates by date.

## Monitoring (Phase 8)

After predictions exist (`ml/predict.py`) and `fct_predictions` is built, run:

```bash
python ml/monitor.py
```

This reads `fct_predictions` from DuckDB (scored rows with non-null `actual_is_delayed`), computes **monthly** technical metrics, **precision@k** (top decile by default) and **cost-weighted** FP/FN totals using `ml/config.yaml`, compares ROC-AUC to the baseline in `ml/current_model.yaml` (or `monitoring.baseline_roc_auc`), writes `ml/reports/monitoring_{date}.json`, runs **drift** (PSI + categorical shifts) vs `ml/data_snapshots/train_v1.parquet` and `data/ml/features.parquet`, writes `ml/reports/drift_{date}.md`, and replaces `main.ml_monitoring` with a JSON payload snapshot.

**Prerequisites:** training snapshot (e.g. `train_v1.parquet` from `python ml/train.py`, or the path in `train_snapshot` inside `current_model.yaml`), current `features.parquet`, and populated `fct_predictions`.

## Retraining (Phase 9)

After monitoring has produced at least one `ml/reports/monitoring_*.json`:

```bash
python ml/retrain.py
```

Triggers are documented in [ml_design.md](ml_design.md). Use `python ml/retrain.py --force` to refit regardless of triggers (still applies the promotion rule: **test ROC-AUC must improve** to update `current_model.yaml`).

## Benchmarks (illustrative)

Times vary by CPU, disk, and dataset size. The following are **typical** on a mid-range laptop after data is cached locally:

| Step | Order of magnitude |
| --- | --- |
| `dbt build` (full project) | ~1–4 min |
| `make export-features` | ~30–90 s (includes targeted dbt + Parquet write) |
| `python ml/train.py` | ~5–30 s |
| `python ml/predict.py` | ~5–20 s |
| `dbt build --select "+fct_predictions"` | ~10–40 s |
| `python ml/monitor.py` | ~5–30 s |
| `python ml/retrain.py` (when triggered) | ~10–60 s |

Use the `(timing) …` lines from `scripts/run_pipeline.sh` on your machine as the ground truth.

## Verifying the Setup

```bash
# Check dbt connection
cd dbt_project && dbt debug

# Check dbt dependencies
cd dbt_project && dbt deps

# Compile models (no execution)
cd dbt_project && dbt compile
```

## Cleaning Up

```bash
make clean    # Remove dbt artifacts, DuckDB files, and __pycache__
```
