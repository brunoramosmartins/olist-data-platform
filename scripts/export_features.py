"""
export_features.py — Export dbt model fct_order_features to Parquet for ML.

Run from the repository root after the model exists in DuckDB, e.g.:

    cd dbt_project && dbt build --select "+fct_order_features"
    cd .. && python scripts/export_features.py

Or: make export-features
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "olist.duckdb"
OUT_PATH = ROOT / "data" / "ml" / "features.parquet"


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f">>> Connecting to {DB_PATH}...")
    con = duckdb.connect(str(DB_PATH))
    out_sql = str(OUT_PATH.resolve()).replace("\\", "/")
    print(f">>> COPY fct_order_features -> {OUT_PATH}")
    con.execute(
        f"COPY (SELECT * FROM main.fct_order_features) TO '{out_sql}' (FORMAT PARQUET)"
    )
    con.close()
    print(">>> Done.")


if __name__ == "__main__":
    main()
