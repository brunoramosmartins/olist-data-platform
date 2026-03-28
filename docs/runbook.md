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
