"""
Evaluate active model on the held-out test slice from `ml/current_model.yaml`.

Usage:
    python ml/evaluate.py
    python ml/evaluate.py --check-minimum-auc 0.55
    python ml/evaluate.py --check-minimum-auc 0.55 --skip-if-missing
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
import pandas as pd
import yaml

import train as train_mod
from feature_config import TARGET, feature_columns, validate_expected_columns

ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PATH = ROOT / "ml" / "current_model.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate active model test ROC-AUC.")
    parser.add_argument(
        "--check-minimum-auc",
        type=float,
        default=None,
        help="Exit with code 1 if test ROC-AUC is below this value.",
    )
    parser.add_argument(
        "--skip-if-missing",
        action="store_true",
        help="Exit 0 if features, registry, or model artifact are missing (e.g. CI without artifacts).",
    )
    args = parser.parse_args()

    features_path = ROOT / "data" / "ml" / "features.parquet"
    if args.skip_if_missing and (
        not features_path.is_file() or not REGISTRY_PATH.is_file()
    ):
        print("evaluate: skip (missing features or registry)")
        return
    if not REGISTRY_PATH.is_file():
        raise FileNotFoundError(f"Missing {REGISTRY_PATH}")
    if not features_path.is_file():
        raise FileNotFoundError(f"Missing {features_path}")

    with open(REGISTRY_PATH, encoding="utf-8") as f:
        reg = yaml.safe_load(f) or {}
    active = reg.get("active_model") or {}
    model_path = ROOT / Path(str(active["path"]))
    test_start = pd.Timestamp(str(active["test_start"]))

    if args.skip_if_missing and not model_path.is_file():
        print("evaluate: skip (model artifact missing)")
        return

    if not model_path.is_file():
        raise FileNotFoundError(f"Model not found: {model_path}")

    df = pd.read_parquet(features_path)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    if TARGET not in df.columns:
        raise ValueError(f"Parquet must contain `{TARGET}`.")
    df = df.loc[df[TARGET].notna()].copy()
    df[TARGET] = df[TARGET].astype(int)

    test_mask = df["order_purchase_timestamp"] >= test_start
    if int(test_mask.sum()) == 0:
        raise RuntimeError("No rows in test split; check features date range vs registry test_start.")

    validate_expected_columns(list(df.columns))
    cols = feature_columns(list(df.columns))
    pipeline = joblib.load(model_path)
    auc = train_mod.roc_auc_on_mask(pipeline, df, test_mask, cols, TARGET)
    print(f"Test ROC-AUC: {auc if auc is not None else 'N/A'}")

    if args.check_minimum_auc is not None:
        if auc is None:
            print("evaluate: cannot compare minimum AUC (single class or undefined).", file=sys.stderr)
            sys.exit(1)
        if auc < args.check_minimum_auc:
            print(
                f"evaluate: ROC-AUC {auc:.6f} < minimum {args.check_minimum_auc:.6f}",
                file=sys.stderr,
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
