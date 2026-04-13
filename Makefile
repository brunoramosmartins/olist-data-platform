.PHONY: install load-data dbt-deps dbt-build dbt-test export-features ml-train ml-predict ml-evaluate pipeline clean

# ---------- Environment ----------

install:
	python -m pip install --upgrade pip
	pip install -e ".[dev]"
	$(MAKE) dbt-deps

# ---------- Data ----------

load-data:
	python scripts/load_raw_data.py

# ---------- dbt ----------

dbt-deps:
	cd dbt_project && dbt deps

dbt-build:
	cd dbt_project && dbt build

dbt-test:
	cd dbt_project && dbt test

export-features:
	cd dbt_project && dbt build --select "+fct_order_features"
	python scripts/export_features.py

# ---------- ML ----------

ml-train:
	python ml/train.py

ml-predict:
	python ml/predict.py

ml-evaluate:
	python ml/evaluate.py

# ---------- Full Pipeline ----------
# Order: load-data → dbt-build → export-features → train → predict → fct_predictions → monitor → retrain

pipeline:
	bash scripts/run_pipeline.sh

# ---------- Utilities ----------

clean:
	rm -rf dbt_project/target dbt_project/dbt_packages dbt_project/logs
	rm -f data/*.duckdb data/*.duckdb.wal
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
