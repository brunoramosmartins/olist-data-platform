# Olist Data Platform

End-to-end data system that transforms raw e-commerce data into reliable metrics and monitored delivery-delay predictions.

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
CSV (Olist) → stg_* → int_* → fact/dim → metrics
                                    ↓
                              features → .parquet → train → predict → monitor → retrain
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
│   ├── models/           # Serialized model artifacts
│   ├── predictions/      # Versioned prediction outputs
│   ├── reports/          # Monitoring and evaluation reports
│   ├── data_snapshots/   # Training data snapshots
│   └── notebooks/        # EDA and analysis
├── docs/                 # Business definitions, ML design, runbook
├── scripts/              # Setup, download, and utility scripts
├── Makefile              # Pipeline orchestration
├── pyproject.toml        # Python dependencies
└── CHANGELOG.md
```

## Quick Start

```bash
# 1. Set up environment
bash scripts/setup_env.sh

# 2. Activate venv
source .venv/bin/activate

# 3. Download Olist dataset (requires Kaggle CLI)
bash scripts/download_data.sh

# 4. Run the full pipeline
make pipeline
```

See [docs/runbook.md](docs/runbook.md) for detailed instructions.

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
| 9 | Retraining (Continuous Cycle) | `v1.0.0` |

## License

MIT
