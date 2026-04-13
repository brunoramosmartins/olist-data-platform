"""
Train baseline logistic regression for delivery-delay prediction (Phase 6).

Reads `data/ml/features.parquet` (export from dbt). Temporal split only — no DuckDB.

Usage:
    python ml/train.py
    make ml-train
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from feature_config import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    TARGET,
    feature_columns,
    validate_expected_columns,
)

ROOT = Path(__file__).resolve().parent.parent
FEATURES_PATH = ROOT / "data" / "ml" / "features.parquet"
MODEL_DIR = ROOT / "ml" / "models"
MODEL_PATH = MODEL_DIR / "model_v1_logreg.joblib"
YAML_PATH = ROOT / "ml" / "current_model.yaml"
SNAPSHOT_DIR = ROOT / "ml" / "data_snapshots"
SNAPSHOT_PATH = SNAPSHOT_DIR / "train_v1.parquet"
REPORT_DIR = ROOT / "ml" / "reports"
EVAL_REPORT_PATH = REPORT_DIR / "evaluation_v1.md"

# Temporal boundaries (purchase timestamp, naive local time as in source)
TRAIN_END = pd.Timestamp("2018-04-01")
VAL_END = pd.Timestamp("2018-07-01")


@dataclass
class SplitMetrics:
    roc_auc: float | None
    precision: float
    recall: float
    f1: float
    confusion: np.ndarray


def _print_class_balance(y: pd.Series, label: str) -> None:
    vc = y.value_counts(dropna=False).sort_index()
    total = len(y)
    print(f"  {label} (n={total}):")
    for k, v in vc.items():
        pct = 100.0 * v / total if total else 0.0
        print(f"    is_delayed={k}: {v} ({pct:.1f}%)")


def _build_pipeline(numeric_cols: list[str], categorical_cols: list[str]) -> Pipeline:
    # Encoding: one-hot with frequency cap for high-cardinality categoricals (vs ordinal:
    # unsuitable for linear model without monotonic meaning).
    preprocess = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_cols,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        (
                            "imputer",
                            SimpleImputer(strategy="constant", fill_value="missing"),
                        ),
                        (
                            "ohe",
                            OneHotEncoder(
                                handle_unknown="ignore",
                                sparse_output=False,
                                max_categories=40,
                            ),
                        ),
                    ]
                ),
                categorical_cols,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocess", preprocess),
            (
                "clf",
                LogisticRegression(max_iter=2000, random_state=42),
            ),
        ]
    )


def _evaluate(model: Pipeline, X: pd.DataFrame, y: pd.Series) -> SplitMetrics:
    proba = model.predict_proba(X)[:, 1]
    pred = model.predict(X)
    try:
        roc = float(roc_auc_score(y, proba))
    except ValueError:
        roc = None
    return SplitMetrics(
        roc_auc=roc,
        precision=float(precision_score(y, pred, zero_division=0)),
        recall=float(recall_score(y, pred, zero_division=0)),
        f1=float(f1_score(y, pred, zero_division=0)),
        confusion=confusion_matrix(y, pred),
    )


def _top_coef_table(model: Pipeline, n: int = 25) -> str:
    clf = model.named_steps["clf"]
    preprocess = model.named_steps["preprocess"]
    names = preprocess.get_feature_names_out()
    coef = clf.coef_.ravel()
    order = np.argsort(np.abs(coef))[::-1][:n]
    lines = ["| Rank | Feature | Coefficient |", "| --- | --- | --- |"]
    for i, idx in enumerate(order, start=1):
        lines.append(f"| {i} | `{names[idx]}` | {coef[idx]:.4f} |")
    return "\n".join(lines)


def _write_yaml(
    metrics_val: SplitMetrics,
    metrics_test: SplitMetrics,
    n_train: int,
    n_val: int,
    n_test: int,
) -> None:
    trained_on = date.today().isoformat()
    lines = [
        "active_model:",
        '  version: "v1"',
        '  algorithm: "logistic_regression"',
        '  path: "ml/models/model_v1_logreg.joblib"',
        f'  trained_on: "{trained_on}"',
        '  train_cutoff: "2018-04-01"',
        '  validation_end: "2018-06-30"',
        '  test_start: "2018-07-01"',
        '  features_parquet: "data/ml/features.parquet"',
        '  train_snapshot: "ml/data_snapshots/train_v1.parquet"',
        "  splits:",
        f"    train_rows: {n_train}",
        f"    validation_rows: {n_val}",
        f"    test_rows: {n_test}",
        "  metrics:",
        f"    roc_auc_val: {metrics_val.roc_auc if metrics_val.roc_auc is not None else 'null'}",
        f"    roc_auc_test: {metrics_test.roc_auc if metrics_test.roc_auc is not None else 'null'}",
        f"    precision_val: {metrics_val.precision:.6f}",
        f"    recall_val: {metrics_val.recall:.6f}",
        f"    f1_val: {metrics_val.f1:.6f}",
        f"    precision_test: {metrics_test.precision:.6f}",
        f"    recall_test: {metrics_test.recall:.6f}",
        f"    f1_test: {metrics_test.f1:.6f}",
    ]
    YAML_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt_cm2x2(cm: np.ndarray) -> tuple[int, int, int, int]:
    if cm.shape != (2, 2):
        raise ValueError(f"Expected 2x2 confusion matrix, got shape {cm.shape}")
    return int(cm[0, 0]), int(cm[0, 1]), int(cm[1, 0]), int(cm[1, 1])


def _write_report(
    metrics_val: SplitMetrics,
    metrics_test: SplitMetrics,
    model: Pipeline,
    n_train: int,
    n_val: int,
    n_test: int,
) -> None:
    cm_val = metrics_val.confusion
    cm_test = metrics_test.confusion
    v00, v01, v10, v11 = _fmt_cm2x2(cm_val)
    t00, t01, t10, t11 = _fmt_cm2x2(cm_test)
    body = f"""# Evaluation report — v1 Logistic Regression

Generated: `{datetime.now(timezone.utc).isoformat()}` (UTC)

## Model

- **Algorithm:** Logistic Regression (default hyperparameters aside from `max_iter=2000`, `random_state=42`).
- **Target:** `is_delayed` (1 = late vs estimated delivery, 0 = on-time), rows with non-null label only.
- **Preprocessing:** `ColumnTransformer` — numeric: median imputation + `StandardScaler`; categorical: constant `"missing"` + one-hot (`max_categories=40`, `handle_unknown='ignore'`).
- **Excluded from X:** `order_id`, `order_purchase_timestamp`, `seller_id` (identifier / leakage risk for linear baseline).

## Temporal split (purchase timestamp)

| Split | Rule | Rows |
| --- | --- | --- |
| Train | `<2018-04-01` | {n_train} |
| Validation | `2018-04-01` to `< 2018-07-01` | {n_val} |
| Test | `>= 2018-07-01` | {n_test} |

## Metrics

### Validation (used for monitoring; no hyperparameter search in v1)

| Metric | Value |
| --- | --- |
| ROC-AUC | {metrics_val.roc_auc if metrics_val.roc_auc is not None else "N/A (single class?)"} |
| Precision | {metrics_val.precision:.4f} |
| Recall | {metrics_val.recall:.4f} |
| F1 | {metrics_val.f1:.4f} |

**Confusion matrix (validation)** — rows=true, cols=predicted:

| | Pred 0 | Pred 1 |
| --- | --- | --- |
| True 0 | {v00} | {v01} |
| True 1 | {v10} | {v11} |

### Test (held-out)

| Metric | Value |
| --- | --- |
| ROC-AUC | {metrics_test.roc_auc if metrics_test.roc_auc is not None else "N/A"} |
| Precision | {metrics_test.precision:.4f} |
| Recall | {metrics_test.recall:.4f} |
| F1 | {metrics_test.f1:.4f} |

**Confusion matrix (test)**

| | Pred 0 | Pred 1 |
| --- | --- | --- |
| True 0 | {t00} | {t01} |
| True 1 | {t10} | {t11} |

## Feature importance (linear coefficients, top 25 by |coef|)

{_top_coef_table(model)}

## Limitations

- Baseline linear model; interactions and non-linearities not captured.
- One-hot with `max_categories` collapses rare categories — some signal loss.
- `seller_id` dropped; seller-specific effects only via state + historical delivery average.
- Class imbalance may skew precision/recall; consider `class_weight` or resampling in later versions.
- Test performance is a single temporal slice; drift not assessed here.

---
*Regenerate this file with `python ml/train.py` after updating `data/ml/features.parquet`.*
"""
    EVAL_REPORT_PATH.write_text(body, encoding="utf-8")


def prepare_features_for_sklearn(df_sub: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Match inference preprocessing (numeric float64, categorical 'missing' string)."""
    d = df_sub.loc[:, feature_cols].copy()
    num_cols = [c for c in NUMERIC_FEATURES if c in feature_cols]
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in feature_cols]
    for c in num_cols:
        d[c] = pd.to_numeric(d[c], errors="coerce").astype("float64")
    for c in cat_cols:
        d[c] = d[c].fillna("missing").astype(str)
    return d


def fit_pipeline_on_mask(
    df: pd.DataFrame,
    train_mask: pd.Series,
    feature_cols: list[str],
    target_col: str,
) -> Pipeline:
    validate_expected_columns(list(df.columns))
    num_cols = [c for c in NUMERIC_FEATURES if c in feature_cols]
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in feature_cols]
    d_train = prepare_features_for_sklearn(df.loc[train_mask], feature_cols)
    y_train = df.loc[train_mask, target_col].astype(int)
    pipeline = _build_pipeline(num_cols, cat_cols)
    pipeline.fit(d_train, y_train)
    return pipeline


def roc_auc_on_mask(
    pipeline: Pipeline,
    df: pd.DataFrame,
    mask: pd.Series,
    feature_cols: list[str],
    target_col: str,
) -> float | None:
    d = prepare_features_for_sklearn(df.loc[mask], feature_cols)
    y = df.loc[mask, target_col].astype(int).to_numpy()
    if len(np.unique(y)) < 2:
        return None
    proba = pipeline.predict_proba(d)[:, 1]
    try:
        return float(roc_auc_score(y, proba))
    except ValueError:
        return None


def main() -> None:
    if not FEATURES_PATH.is_file():
        raise FileNotFoundError(
            f"Missing {FEATURES_PATH}. Run: make export-features (or dbt export + scripts/export_features.py)."
        )

    df = pd.read_parquet(FEATURES_PATH)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])

    if TARGET not in df.columns:
        raise ValueError(f"Parquet must contain target column `{TARGET}`.")

    df = df.loc[df[TARGET].notna()].copy()
    df[TARGET] = df[TARGET].astype(int)

    ts = df["order_purchase_timestamp"]
    train_mask = ts < TRAIN_END
    val_mask = (ts >= TRAIN_END) & (ts < VAL_END)
    test_mask = ts >= VAL_END

    n_train, n_val, n_test = int(train_mask.sum()), int(val_mask.sum()), int(test_mask.sum())
    print("Temporal split:")
    print(f"  Train (< {TRAIN_END.date()}): {n_train}")
    print(f"  Validation ([{TRAIN_END.date()}, {VAL_END.date()})): {n_val}")
    print(f"  Test (>= {VAL_END.date()}): {n_test}")

    if n_train == 0 or n_val == 0 or n_test == 0:
        raise RuntimeError(
            "One or more splits are empty. Check feature Parquet date range vs cutoffs."
        )

    print("Class balance:")
    _print_class_balance(df.loc[train_mask, TARGET], "Train")
    _print_class_balance(df.loc[val_mask, TARGET], "Validation")
    _print_class_balance(df.loc[test_mask, TARGET], "Test")

    validate_expected_columns(list(df.columns))
    feature_cols = feature_columns(list(df.columns))

    num_cols = [c for c in NUMERIC_FEATURES if c in feature_cols]
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in feature_cols]
    for c in cat_cols:
        df[c] = df[c].astype("string").fillna(pd.NA)

    X_train = df.loc[train_mask, feature_cols]
    y_train = df.loc[train_mask, TARGET]
    X_val = df.loc[val_mask, feature_cols]
    y_val = df.loc[val_mask, TARGET]
    X_test = df.loc[test_mask, feature_cols]
    y_test = df.loc[test_mask, TARGET]

    train_snapshot = df.loc[train_mask].copy()
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    train_snapshot.to_parquet(SNAPSHOT_PATH, index=False)
    print(f"Wrote training snapshot: {SNAPSHOT_PATH}")

    pipeline = _build_pipeline(num_cols, cat_cols)
    print("Fitting pipeline on train…")
    pipeline.fit(X_train, y_train)

    metrics_val = _evaluate(pipeline, X_val, y_val)
    metrics_test = _evaluate(pipeline, X_test, y_test)

    joblib.dump(pipeline, MODEL_PATH)
    print(f"Saved model: {MODEL_PATH}")

    _write_yaml(metrics_val, metrics_test, n_train, n_val, n_test)
    print(f"Wrote registry: {YAML_PATH}")

    _write_report(metrics_val, metrics_test, pipeline, n_train, n_val, n_test)
    print(f"Wrote report: {EVAL_REPORT_PATH}")

    print("Done.")


if __name__ == "__main__":
    main()
