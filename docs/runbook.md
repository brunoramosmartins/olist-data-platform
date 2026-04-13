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

```bash
# Full pipeline (dbt build → predict → monitor → retrain)
make pipeline

# Individual steps
make dbt-build      # Build all dbt models
make dbt-test       # Run dbt tests only
make ml-train       # Train the ML model
make ml-predict     # Run batch predictions
```

Without `make` (from repository root, after activating the venv):

```bash
cd dbt_project && dbt build
cd ..
python scripts/export_features.py
python ml/train.py
python ml/predict.py
cd dbt_project && dbt build --select "+fct_predictions"
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
