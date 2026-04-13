"""
Batch inference: load active model from ml/current_model.yaml, score features Parquet,
write versioned Parquet + DuckDB table main.ml_predictions.

Usage (from repo root):
    python ml/predict.py
    python ml/predict.py --dry-run
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import joblib
import numpy as np
import pandas as pd
import yaml

from feature_config import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    feature_columns,
    validate_expected_columns,
)

ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PATH = ROOT / "ml" / "current_model.yaml"
DB_PATH = ROOT / "data" / "olist.duckdb"
PREDICTIONS_DIR = ROOT / "ml" / "predictions"


def _load_registry() -> dict:
    if not REGISTRY_PATH.is_file():
        raise FileNotFoundError(
            f"Missing {REGISTRY_PATH}. Train a model first (python ml/train.py)."
        )
    with open(REGISTRY_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not data or "active_model" not in data:
        raise ValueError(f"Invalid registry YAML: {REGISTRY_PATH}")
    return data["active_model"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch scoring from features Parquet.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run load + transform + predict; do not write Parquet or DuckDB.",
    )
    parser.add_argument(
        "--skip-if-missing",
        action="store_true",
        help="Exit 0 if registry, model, or features are missing (CI without artifacts).",
    )
    args = parser.parse_args()

    if args.skip_if_missing and not REGISTRY_PATH.is_file():
        print("predict: skip (no registry)")
        return

    active = _load_registry()
    model_rel = Path(active["path"])
    model_path = ROOT / model_rel
    if not model_path.is_file():
        if args.skip_if_missing:
            print("predict: skip (no model artifact)")
            return
        raise FileNotFoundError(f"Model artifact not found: {model_path}")

    features_rel = Path(active["features_parquet"])
    features_path = ROOT / features_rel
    if not features_path.is_file():
        if args.skip_if_missing:
            print("predict: skip (no features parquet)")
            return
        raise FileNotFoundError(
            f"Features parquet not found: {features_path}. Run export_features first."
        )

    version = str(active["version"])

    pipeline = joblib.load(model_path)
    df = pd.read_parquet(features_path)
    if "order_id" not in df.columns:
        raise ValueError("features parquet must include order_id")

    validate_expected_columns(list(df.columns))
    cols = feature_columns(list(df.columns))
    num_cols = [c for c in NUMERIC_FEATURES if c in cols]
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in cols]

    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in cat_cols:
        df[c] = df[c].fillna("missing").astype(str)

    X = df[cols]
    proba = pipeline.predict_proba(X)[:, 1]
    pred_class = (proba >= 0.5).astype(np.int64)

    ts = datetime.now(timezone.utc)
    out = pd.DataFrame(
        {
            "order_id": df["order_id"].astype(str),
            "predicted_probability": proba.astype(float),
            "predicted_class": pred_class,
            "model_version": version,
            "prediction_timestamp": ts,
        }
    )

    if len(out) != len(df):
        raise RuntimeError("Internal error: prediction row count mismatch.")

    if args.dry_run:
        print(
            f"dry-run OK: scored {len(out)} rows (model {version}); skipping writes."
        )
        return

    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    day = date.today().isoformat()
    safe_ver = version.replace("/", "_").replace("\\", "_")
    parquet_out = PREDICTIONS_DIR / f"predictions_{safe_ver}_{day}.parquet"
    out.to_parquet(parquet_out, index=False)
    print(f"Wrote predictions parquet: {parquet_out}")

    print(f"Writing DuckDB table main.ml_predictions ({len(out)} rows)…")
    con = duckdb.connect(str(DB_PATH))
    con.register("pred_out", out)
    con.execute("CREATE OR REPLACE TABLE ml_predictions AS SELECT * FROM pred_out")
    con.close()
    print("Done. Run dbt build --select +fct_predictions to refresh the bridge model.")


if __name__ == "__main__":
    main()
