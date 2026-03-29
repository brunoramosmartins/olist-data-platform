"""
load_raw_data.py — Load Olist CSVs into DuckDB as raw tables.

This script reads all CSVs from data/raw/ and creates corresponding
tables in the DuckDB database used by dbt. Run this before `dbt build`.

Usage:
    python scripts/load_raw_data.py
"""

import duckdb
from pathlib import Path

RAW_DIR = Path("data/raw")
DB_PATH = Path("data/olist.duckdb")

CSV_FILES = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_customers_dataset",
    "olist_sellers_dataset",
    "olist_products_dataset",
    "olist_geolocation_dataset",
]


def main():
    print(f">>> Connecting to {DB_PATH}...")
    con = duckdb.connect(str(DB_PATH))

    for table_name in CSV_FILES:
        csv_path = RAW_DIR / f"{table_name}.csv"
        if not csv_path.exists():
            print(f"  [SKIP] {csv_path} not found")
            continue

        print(f"  Loading {table_name}...")
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=true)
        """)
        row_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        print(f"    -> {row_count:,} rows")

    con.close()
    print("\n>>> Done. All tables loaded into DuckDB.")


if __name__ == "__main__":
    main()
