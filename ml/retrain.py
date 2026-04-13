"""
Retraining orchestrator (Phase 9): read latest monitoring report, evaluate triggers,
optionally retrain with an extended temporal window, compare to the active model,
promote `ml/current_model.yaml` only if test ROC-AUC improves.

Usage (repo root):
    python ml/retrain.py
    python ml/retrain.py --force
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import yaml
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

import train as train_mod
from feature_config import CATEGORICAL_FEATURES, TARGET, feature_columns, validate_expected_columns

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "ml" / "config.yaml"
REGISTRY_PATH = ROOT / "ml" / "current_model.yaml"
FEATURES_PATH = ROOT / "data" / "ml" / "features.parquet"
REPORT_DIR = ROOT / "ml" / "reports"
STATE_PATH = ROOT / "ml" / "state" / "retrain_state.json"
MODEL_DIR = ROOT / "ml" / "models"
SNAPSHOT_DIR = ROOT / "ml" / "data_snapshots"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("retrain")


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _latest_monitoring_json() -> Path | None:
    paths = sorted(REPORT_DIR.glob("monitoring_*.json"))
    return paths[-1] if paths else None


def _parse_version(v: str) -> int:
    s = str(v).strip().lower()
    if s.startswith("v"):
        s = s[1:]
    return int(s)


def _max_consecutive_alerts(windows: list[dict[str, Any]]) -> int:
    """windows sorted by calendar period; count longest run of roc_auc_alert True."""
    best = cur = 0
    for w in windows:
        if w.get("roc_auc_alert"):
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def _count_high_psi_numeric(drift: dict[str, Any], threshold: float) -> int:
    n = 0
    for row in drift.get("numeric", []):
        psi = row.get("psi")
        if isinstance(psi, (float, int)) and not (isinstance(psi, float) and np.isnan(psi)):
            if float(psi) >= threshold:
                n += 1
    return n


def _months_between(iso_a: str, iso_b: str) -> float:
    """Approximate calendar months between date-like ISO strings (a earlier than b)."""
    da = datetime.fromisoformat(iso_a[:10]).date()
    db = datetime.fromisoformat(iso_b[:10]).date()
    return (db.year - da.year) * 12 + (db.month - da.month)


def _load_retrain_state() -> dict[str, Any]:
    if not STATE_PATH.is_file():
        return {}
    with open(STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


def _save_retrain_state(payload: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _metric_bundle(
    pipeline: Any,
    df: pd.DataFrame,
    mask: pd.Series,
    feature_cols: list[str],
    target_col: str,
) -> dict[str, Any]:
    X = train_mod.prepare_features_for_sklearn(df.loc[mask], feature_cols)
    y = df.loc[mask, target_col].astype(int)
    proba = pipeline.predict_proba(X)[:, 1]
    pred = pipeline.predict(X)
    roc: float | None
    try:
        roc = float(roc_auc_score(y, proba)) if len(np.unique(y.to_numpy())) > 1 else None
    except ValueError:
        roc = None
    return {
        "roc_auc": roc,
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
    }


def _write_registry(
    *,
    version: str,
    model_rel: str,
    train_snapshot_rel: str,
    train_cutoff: str,
    validation_end: str,
    test_start: str,
    n_train: int,
    n_val: int,
    n_test: int,
    metrics_val: dict[str, Any],
    metrics_test: dict[str, Any],
) -> None:
    trained_on = date.today().isoformat()
    lines = [
        "active_model:",
        f'  version: "{version}"',
        '  algorithm: "logistic_regression"',
        f'  path: "{model_rel}"',
        f'  trained_on: "{trained_on}"',
        f'  train_cutoff: "{train_cutoff}"',
        f'  validation_end: "{validation_end}"',
        f'  test_start: "{test_start}"',
        '  features_parquet: "data/ml/features.parquet"',
        f'  train_snapshot: "{train_snapshot_rel}"',
        "  splits:",
        f"    train_rows: {n_train}",
        f"    validation_rows: {n_val}",
        f"    test_rows: {n_test}",
        "  metrics:",
        f"    roc_auc_val: {metrics_val['roc_auc'] if metrics_val['roc_auc'] is not None else 'null'}",
        f"    roc_auc_test: {metrics_test['roc_auc'] if metrics_test['roc_auc'] is not None else 'null'}",
        f"    precision_val: {metrics_val['precision']:.6f}",
        f"    recall_val: {metrics_val['recall']:.6f}",
        f"    f1_val: {metrics_val['f1']:.6f}",
        f"    precision_test: {metrics_test['precision']:.6f}",
        f"    recall_test: {metrics_test['recall']:.6f}",
        f"    f1_test: {metrics_test['f1']:.6f}",
    ]
    REGISTRY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_comparison_report(
    path: Path,
    old_ver: str,
    new_ver: str,
    old_m: dict[str, Any],
    new_m: dict[str, Any],
    promoted: bool,
) -> None:
    def row(metric: str, o: Any, n: Any) -> str:
        if metric == "ROC-AUC":
            o_s = f"{o:.6f}" if o is not None else "N/A"
            n_s = f"{n:.6f}" if n is not None else "N/A"
            if o is not None and n is not None:
                d_s = f"{n - o:+.6f}"
            else:
                d_s = "N/A"
        else:
            o_s, n_s = f"{o:.6f}", f"{n:.6f}"
            d_s = f"{n - o:+.6f}"
        return f"| {metric} | {o_s} | {n_s} | {d_s} |"

    body = "\n".join(
        [
            f"# Model comparison — {old_ver} vs {new_ver}",
            "",
            f"Generated: `{datetime.now(timezone.utc).isoformat()}` (UTC)",
            "",
            f"**Promotion:** {'yes — `current_model.yaml` updated' if promoted else 'no — kept active model'}",
            "",
            "## Test set (same temporal slice as registry `test_start`)",
            "",
            "| Metric | old | new | delta |",
            "| --- | --- | --- | --- |",
            row("ROC-AUC", old_m.get("roc_auc"), new_m.get("roc_auc")),
            row("Precision", old_m["precision"], new_m["precision"]),
            row("Recall", old_m["recall"], new_m["recall"]),
            row("F1", old_m["f1"], new_m["f1"]),
            "",
            "**Rule:** promote only if new **ROC-AUC** is strictly greater than old.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def evaluate_triggers(
    monitoring: dict[str, Any],
    drift: dict[str, Any],
    cfg: dict[str, Any],
    last_retrain_ref_iso: str,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    rcfg = cfg.get("retrain", {})
    consec_need = int(rcfg.get("performance_consecutive_windows", 2))
    psi_thr = float(rcfg.get("psi_retrain_threshold", 0.2))
    psi_count_need = int(rcfg.get("psi_feature_count_trigger", 3))
    cal_months = int(rcfg.get("calendar_months", 3))

    windows = list(monitoring.get("monthly_windows") or [])
    windows_sorted = sorted(windows, key=lambda w: str(w.get("period", "")))
    if _max_consecutive_alerts(windows_sorted) >= consec_need:
        reasons.append(
            f"performance: {consec_need}+ consecutive monthly windows with roc_auc_alert "
            "(ROC-AUC < baseline - roc_auc_alert_delta)"
        )

    if "error" not in drift:
        hi = _count_high_psi_numeric(drift, psi_thr)
        if hi >= psi_count_need:
            reasons.append(f"drift: {hi} numeric features with PSI >= {psi_thr}")

    today = date.today().isoformat()
    if _months_between(last_retrain_ref_iso[:10], today) >= cal_months:
        reasons.append(
            f"calendar: >={cal_months} months since last retrain reference ({last_retrain_ref_iso[:10]})"
        )

    return bool(reasons), reasons


def main() -> None:
    parser = argparse.ArgumentParser(description="Retrain orchestrator (Phase 9).")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore trigger evaluation and retrain anyway.",
    )
    args = parser.parse_args()

    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"Missing {CONFIG_PATH}")
    cfg = _load_yaml(CONFIG_PATH)

    if not REGISTRY_PATH.is_file():
        raise FileNotFoundError(f"Missing {REGISTRY_PATH}. Run python ml/train.py first.")
    registry = _load_yaml(REGISTRY_PATH)
    active = registry["active_model"]
    old_ver = str(active["version"])
    old_path = ROOT / str(active["path"])

    state = _load_retrain_state()
    ref_iso = state.get("last_retrain_iso") or str(active.get("trained_on", "2000-01-01"))

    mon_path = _latest_monitoring_json()
    if mon_path is None or not mon_path.is_file():
        log.warning("No monitoring JSON in %s — cannot evaluate triggers. Exiting.", REPORT_DIR)
        if not args.force:
            return
        monitoring: dict[str, Any] = {}
        drift: dict[str, Any] = {}
    else:
        with open(mon_path, encoding="utf-8") as f:
            monitoring = json.load(f)
        drift = monitoring.get("drift") or {}

    triggered, reasons = evaluate_triggers(monitoring, drift, cfg, ref_iso)
    if args.force:
        triggered = True
        reasons = ["--force"]

    if not triggered:
        log.info("Retrain not triggered (performance / drift / calendar). Clean exit.")
        return

    for r in reasons:
        log.info("Trigger: %s", r)

    if not FEATURES_PATH.is_file():
        raise FileNotFoundError(f"Missing {FEATURES_PATH}. Run export-features first.")

    df = pd.read_parquet(FEATURES_PATH)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    if TARGET not in df.columns:
        raise ValueError(f"Parquet must contain `{TARGET}`.")
    df = df.loc[df[TARGET].notna()].copy()
    df[TARGET] = df[TARGET].astype(int)

    rcfg = cfg.get("retrain", {})
    ext_end = pd.Timestamp(str(rcfg.get("extended_train_end", "2018-06-01")))
    test_start = pd.Timestamp(str(active["test_start"]))

    if ext_end >= test_start:
        raise ValueError(
            f"retrain.extended_train_end ({ext_end.date()}) must be before test_start ({test_start.date()})."
        )

    train_mask = df["order_purchase_timestamp"] < ext_end
    val_mask = (df["order_purchase_timestamp"] >= ext_end) & (
        df["order_purchase_timestamp"] < test_start
    )
    test_mask = df["order_purchase_timestamp"] >= test_start

    n_train, n_val, n_test = int(train_mask.sum()), int(val_mask.sum()), int(test_mask.sum())
    if n_train == 0 or n_test == 0:
        raise RuntimeError("Retrain split produced empty train or test. Check dates and Parquet range.")
    if n_val == 0:
        log.warning("Validation split empty (extended_train_end touches test_start). Metrics val will be weak.")

    validate_expected_columns(list(df.columns))
    feature_cols = feature_columns(list(df.columns))
    for c in CATEGORICAL_FEATURES:
        if c in df.columns:
            df[c] = df[c].astype("string").fillna(pd.NA)

    new_num = _parse_version(old_ver) + 1
    new_ver = f"v{new_num}"
    model_basename = f"model_{new_ver}_logreg.joblib"
    model_rel = f"ml/models/{model_basename}"
    snapshot_basename = f"train_{new_ver}.parquet"
    snapshot_rel = f"ml/data_snapshots/{snapshot_basename}"

    log.info("Fitting new model %s (train n=%s, val n=%s, test n=%s)…", new_ver, n_train, n_val, n_test)
    new_pipeline = train_mod.fit_pipeline_on_mask(df, train_mask, feature_cols, TARGET)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    out_model = ROOT / model_rel
    train_snapshot = df.loc[train_mask].copy()
    train_snapshot.to_parquet(SNAPSHOT_DIR / snapshot_basename, index=False)
    joblib.dump(new_pipeline, out_model)
    log.info("Saved model %s and training snapshot %s", out_model, snapshot_rel)

    new_val_metrics = _metric_bundle(new_pipeline, df, val_mask, feature_cols, TARGET)
    new_test_metrics = _metric_bundle(new_pipeline, df, test_mask, feature_cols, TARGET)

    if not old_path.is_file():
        log.warning("Active model artifact missing at %s — promoting new model by default.", old_path)
        promoted = True
        old_test_metrics = {"roc_auc": None, "precision": 0.0, "recall": 0.0, "f1": 0.0}
    else:
        old_pipeline = joblib.load(old_path)
        old_test_metrics = _metric_bundle(old_pipeline, df, test_mask, feature_cols, TARGET)
        o_roc = old_test_metrics.get("roc_auc")
        n_roc = new_test_metrics.get("roc_auc")
        promoted = (
            n_roc is not None
            and (o_roc is None or (n_roc is not None and n_roc > o_roc))
        )

    val_end_date = (test_start - pd.Timedelta(days=1)).date().isoformat()
    comparison_path = REPORT_DIR / f"comparison_{old_ver}_vs_{new_ver}.md"
    _write_comparison_report(
        comparison_path,
        old_ver,
        new_ver,
        old_test_metrics,
        new_test_metrics,
        promoted,
    )
    log.info("Wrote comparison report %s", comparison_path)

    if promoted:
        _write_registry(
            version=new_ver,
            model_rel=model_rel,
            train_snapshot_rel=snapshot_rel,
            train_cutoff=ext_end.date().isoformat(),
            validation_end=val_end_date,
            test_start=test_start.date().isoformat(),
            n_train=n_train,
            n_val=n_val,
            n_test=n_test,
            metrics_val=new_val_metrics,
            metrics_test=new_test_metrics,
        )
        n_auc = new_test_metrics.get("roc_auc")
        o_auc = old_test_metrics.get("roc_auc")
        log.info(
            "Promoted %s in %s (test ROC-AUC %s vs previous %s).",
            new_ver,
            REGISTRY_PATH,
            f"{n_auc:.6f}" if n_auc is not None else "N/A",
            f"{o_auc:.6f}" if o_auc is not None else "N/A",
        )
    else:
        log.info(
            "Retained active model %s (new test ROC-AUC not better: new=%s old=%s).",
            old_ver,
            new_test_metrics.get("roc_auc"),
            old_test_metrics.get("roc_auc"),
        )

    state["last_retrain_iso"] = datetime.now(timezone.utc).isoformat()
    _save_retrain_state(state)


if __name__ == "__main__":
    main()
