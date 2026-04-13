"""
Shared feature matrix definition for train / predict (keep in sync with fct_order_features).
"""

from __future__ import annotations

TARGET = "is_delayed"
DROP_FROM_X = ["order_id", "order_purchase_timestamp", "seller_id"]

NUMERIC_FEATURES = [
    "order_day_of_week",
    "order_hour",
    "order_month",
    "total_payment_value",
    "payment_installments",
    "payment_type_count",
    "product_weight_g",
    "product_volume_cm3",
    "customer_seller_same_state",
    "seller_avg_delivery_days_historical",
    "freight_value",
    "freight_ratio",
    "estimated_delivery_days",
]
CATEGORICAL_FEATURES = [
    "product_category",
    "customer_state",
    "seller_state",
]


def feature_columns(df_columns: list[str]) -> list[str]:
    """Columns passed to the sklearn pipeline (excludes keys, timestamp, seller_id, target)."""
    return [c for c in df_columns if c not in DROP_FROM_X and c != TARGET]


def validate_expected_columns(df_columns: list[str]) -> None:
    missing = [
        c for c in NUMERIC_FEATURES + CATEGORICAL_FEATURES if c not in df_columns
    ]
    if missing:
        raise ValueError(f"Features parquet missing expected columns: {missing}")
