# Olist Data Platform

End-to-end **data + ML** platform for the **Olist Brazilian e-commerce** dataset: raw CSVs land in DuckDB, **dbt** builds staging → intermediate → marts → metrics → **`fct_order_features`**, a Parquet export feeds **scikit-learn** training and batch scoring, and **monitoring + retraining** close the loop on delivery-delay prediction (`is_delayed`).

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Data engine | DuckDB | Analytical database, zero infrastructure |
| Transformation | dbt-core + dbt-duckdb | SQL-based data modeling, testing, docs |
| ML | scikit-learn | Training, evaluation, inference |
| Data format | Parquet | Boundary between dbt and ML pipelines |
| Orchestration | Makefile + bash | Pipeline execution, no external scheduler |
| Language | Python 3.11+ | ML scripts, data loading |
| CI | GitHub Actions | dbt tests + ML validation checks |

## Architecture

```
CSV (Olist) -> DuckDB raw -> dbt (stg -> int -> marts -> metrics -> fct_order_features)
                                    |
                         data/ml/features.parquet
                                    |
 ml/train.py -> model_v*.joblib + current_model.yaml + train_v*.parquet
                                    |
              ml/predict.py -> ml_predictions (DuckDB) -> dbt fct_predictions
                                    |
              ml/monitor.py -> monitoring_*.json + drift_*.md + ml_monitoring
                                    |
              ml/retrain.py -> comparison_*.md + optional promotion
```

## Directory Structure

```
olist-data-platform/
├── .github/              # Issue templates, PR template, CI workflows
├── data/
│   ├── raw/              # Olist CSVs (gitignored)
│   └── ml/               # Parquet boundary layer (gitignored)
├── dbt_project/
│   ├── models/
│   │   ├── staging/      # stg_* — clean, cast, rename
│   │   ├── intermediate/ # int_* — business rules, enrichment
│   │   ├── marts/        # fact/dim tables
│   │   ├── metrics/      # GMV, revenue, delay rate
│   │   └── features/     # ML feature layer
│   ├── macros/
│   ├── seeds/
│   └── tests/
├── ml/
│   ├── models/           # Serialized model artifacts (*.joblib; gitignored)
│   ├── predictions/      # Versioned prediction outputs
│   ├── reports/          # Monitoring, evaluation, comparison reports
│   ├── data_snapshots/   # Training data snapshots (reproducibility)
│   ├── state/            # Retrain calendar reference (optional)
│   └── notebooks/        # EDA and analysis
├── docs/                 # Business definitions, ML design, runbook
├── scripts/              # setup, download, load_raw_data, run_pipeline.sh, …
├── Makefile              # Pipeline orchestration
├── pyproject.toml        # Python dependencies
└── CHANGELOG.md
```

## Quick Start

```bash
# 1. Dependencies + dbt packages (Python 3.11+)
make install

# 2. Raw data (Kaggle CLI) — see scripts/download_data.sh
bash scripts/download_data.sh

# 3. Full cycle: load → dbt → features → train → predict → fct_predictions → monitor → retrain
make pipeline
```

On **Windows**, use **Git Bash** or **WSL** so `make pipeline` can run `bash scripts/run_pipeline.sh`. Alternatively, run the steps in [docs/runbook.md](docs/runbook.md) manually.

See [docs/runbook.md](docs/runbook.md) for detailed instructions and benchmark timings.

## Example: predictions vs actuals (DuckDB)

After `make pipeline` (or predict + `dbt build --select +fct_predictions`):

```sql
SELECT order_date,
       actual_is_delayed,
       predicted_class,
       predicted_probability,
       model_version
FROM fct_predictions
WHERE actual_is_delayed IS NOT NULL
ORDER BY order_date DESC
LIMIT 25;
```

## Documentation

- [Business Definitions](docs/definitions.md) — Valid order, delay, ML scope
- [ML Design](docs/ml_design.md) — Problem statement, features, evaluation plan
- [Metric Definitions](docs/metrics.md) — GMV, revenue, delay rate
- [Runbook](docs/runbook.md) — How to set up and run the pipeline

## Roadmap

| Phase | Description | Tag |
|---|---|---|
| 0 | Setup & Alignment | `v0.1-setup` |
| 1 | Staging (Reliable Data) | `v0.2-staging` |
| 2 | Intermediate (Business Rules) | `v0.3-intermediate` |
| 3 | Marts (Analytical Model) | `v0.4-marts` |
| 4 | Metrics Layer | `v0.5-metrics` |
| 5 | Feature Layer (Bridge to ML) | `v0.6-features` |
| 6 | Model Training | `v0.7-training` |
| 7 | Inference Pipeline | `v0.8-inference` |
| 8 | Monitoring (Feedback Loop) | `v0.9-monitoring` |
| 9 | Retraining (continuous cycle; CI ML checks) | `v1.0.0` |

## License

MIT
