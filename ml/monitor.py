"""
Model monitoring: performance vs actuals (monthly), business metrics, drift vs training snapshot.

Reads:
 - DuckDB `fct_predictions` (predictions + actual_is_delayed + order_date)
  - `data/ml/features.parquet` (current feature distribution)
  - `ml/data_snapshots/train_v1.parquet` (baseline distribution for drift)
  - `ml/config.yaml` (costs, thresholds)
  - `ml/current_model.yaml` (optional baseline ROC-AUC)

Writes:
  - `ml/reports/monitoring_{date}.json`
  - `ml/reports/drift_{date}.md`
  - DuckDB table `main.ml_monitoring` (latest run payload)

Usage (repo root):
    python ml/monitor.py
"""

from __future__ import annotations

import json
import warnings
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import yaml
from sklearn.exceptions import UndefinedMetricWarning
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "olist.duckdb"
CONFIG_PATH = ROOT / "ml" / "config.yaml"
REGISTRY_PATH = ROOT / "ml" / "current_model.yaml"
FEATURES_PATH = ROOT / "data" / "ml" / "features.parquet"
TRAIN_SNAPSHOT_PATH = ROOT / "ml" / "data_snapshots" / "train_v1.parquet"
REPORT_DIR = ROOT / "ml" / "reports"


@dataclass
class WindowMetrics:
    period: str
    n_rows: int
    roc_auc: float | None
    precision: float
    recall: float
    f1: float
    roc_auc_alert: bool


def _load_config() -> dict[str, Any]:
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"Missing {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _baseline_roc_auc(cfg: dict[str, Any]) -> float:
    raw = cfg.get("monitoring", {}).get("baseline_roc_auc")
    if raw is not None:
        return float(raw)
    if not REGISTRY_PATH.is_file():
        raise FileNotFoundError(
            f"Set monitoring.baseline_roc_auc in config or add {REGISTRY_PATH}"
        )
    with open(REGISTRY_PATH, encoding="utf-8") as f:
        reg = yaml.safe_load(f)
    return float(reg["active_model"]["metrics"]["roc_auc_val"])


def _load_scored_frame() -> pd.DataFrame:
    if not DB_PATH.is_file():
        raise FileNotFoundError(f"Missing {DB_PATH}. Run dbt + ml/predict.py first.")
    con = duckdb.connect(str(DB_PATH))
    df = con.execute(
        """
        SELECT
            order_id,
            predicted_probability,
            predicted_class,
            order_date,
            actual_is_delayed
        FROM fct_predictions
        WHERE actual_is_delayed IS NOT NULL
          AND order_date IS NOT NULL
        """
    ).df()
    con.close()
    if df.empty:
        raise RuntimeError("No scored rows in fct_predictions (need actual_is_delayed + order_date).")
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["actual_is_delayed"] = df["actual_is_delayed"].astype(int)
    df["predicted_class"] = df["predicted_class"].astype(int)
    return df


def _safe_roc_auc(y_true: np.ndarray, proba: np.ndarray) -> float | None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UndefinedMetricWarning)
        try:
            return float(roc_auc_score(y_true, proba))
        except ValueError:
            return None


def _monthly_metrics(df: pd.DataFrame, baseline_roc: float, alert_delta: float) -> list[WindowMetrics]:
    df = df.copy()
    df["period"] = df["order_date"].dt.to_period("M").astype(str)
    out: list[WindowMetrics] = []
    for period, g in df.groupby("period", sort=True):
        y = g["actual_is_delayed"].to_numpy()
        pred = g["predicted_class"].to_numpy()
        proba = g["predicted_probability"].to_numpy(dtype=float)
        roc = _safe_roc_auc(y, proba)
        alert = roc is not None and roc < baseline_roc - alert_delta
        out.append(
            WindowMetrics(
                period=str(period),
                n_rows=int(len(g)),
                roc_auc=roc,
                precision=float(precision_score(y, pred, zero_division=0)),
                recall=float(recall_score(y, pred, zero_division=0)),
                f1=float(f1_score(y, pred, zero_division=0)),
                roc_auc_alert=alert,
            )
        )
    return out


def _precision_at_k(df: pd.DataFrame, k_fraction: float) -> float:
    """Among the top k_fraction highest-risk orders by predicted_probability, fraction actually delayed."""
    n = len(df)
    if n == 0:
        return 0.0
    k = max(1, int(np.ceil(k_fraction * n)))
    top = df.nlargest(k, "predicted_probability")
    return float(top["actual_is_delayed"].mean())


def _cost_analysis(
    y_true: np.ndarray, y_pred: np.ndarray, fp_cost: float, fn_cost: float
) -> dict[str, Any]:
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    tn, fp, fn, tp = (int(x) for x in cm.ravel())
    total = fp * fp_cost + fn * fn_cost
    return {
        "true_negatives": int(tn),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_positives": int(tp),
        "estimated_cost_false_positives": float(fp * fp_cost),
        "estimated_cost_false_negatives": float(fn * fn_cost),
        "estimated_total_cost": float(total),
        "cost_ratio_fn_to_fp": float(fn_cost / fp_cost) if fp_cost else None,
    }


def _psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """Population Stability Index; expected = baseline sample, actual = current sample."""
    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]
    if len(expected) < bins or len(actual) < 10:
        return float("nan")
    qs = np.unique(np.percentile(expected, np.linspace(0, 100, bins + 1)))
    if len(qs) < 3:
        return float("nan")
    e_counts, edges = np.histogram(expected, bins=qs)
    a_counts, _ = np.histogram(actual, bins=edges)
    e_pct = e_counts / max(e_counts.sum(), 1)
    a_pct = a_counts / max(a_counts.sum(), 1)
    eps = 1e-6
    e_pct = np.clip(e_pct, eps, 1.0)
    a_pct = np.clip(a_pct, eps, 1.0)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def _psi_status(psi: float, ok_max: float, warn_max: float) -> str:
    if np.isnan(psi):
        return "unknown"
    if psi < ok_max:
        return "ok"
    if psi < warn_max:
        return "warning"
    return "alert"


def _categorical_drift(
    base: pd.Series,
    cur: pd.Series,
    warn_diff: float,
    alert_diff: float,
) -> dict[str, Any]:
    base = base.fillna("__NULL__").astype(str)
    cur = cur.fillna("__NULL__").astype(str)
    all_cats = pd.Index(sorted(set(base.unique()) | set(cur.unique())))
    p_base = base.value_counts(normalize=True).reindex(all_cats, fill_value=0.0)
    p_cur = cur.value_counts(normalize=True).reindex(all_cats, fill_value=0.0)
    diff = (p_cur - p_base).abs()
    worst_cat = diff.idxmax() if len(diff) else None
    max_diff = float(diff.max()) if len(diff) else 0.0
    if max_diff >= alert_diff:
        status = "alert"
    elif max_diff >= warn_diff:
        status = "warning"
    else:
        status = "ok"
    top_shifts = diff.nlargest(5)
    shifts = [
        {"category": str(idx), "baseline_share": float(p_base.loc[idx]), "current_share": float(p_cur.loc[idx]), "abs_diff": float(val)}
        for idx, val in top_shifts.items()
    ]
    return {
        "status": status,
        "max_abs_share_diff": max_diff,
        "worst_category": str(worst_cat) if worst_cat is not None else None,
        "top_shifts": shifts,
    }


def _run_drift(cfg: dict[str, Any]) -> dict[str, Any]:
    drift_cfg = cfg.get("drift", {})
    bins = int(drift_cfg.get("psi_bins", 10))
    ok_max = float(drift_cfg.get("psi_ok_max", 0.1))
    warn_max = float(drift_cfg.get("psi_warning_max", 0.2))
    cat_warn = float(drift_cfg.get("categorical_max_share_diff_warning", 0.1))
    cat_alert = float(drift_cfg.get("categorical_max_share_diff_alert", 0.2))

    if not TRAIN_SNAPSHOT_PATH.is_file():
        return {"error": f"Missing baseline snapshot {TRAIN_SNAPSHOT_PATH}"}
    if not FEATURES_PATH.is_file():
        return {"error": f"Missing current features {FEATURES_PATH}"}

    base_df = pd.read_parquet(TRAIN_SNAPSHOT_PATH)
    cur_df = pd.read_parquet(FEATURES_PATH)

    from feature_config import CATEGORICAL_FEATURES, NUMERIC_FEATURES

    numeric_results = []
    overall_numeric = "ok"
    for col in NUMERIC_FEATURES:
        if col not in base_df.columns or col not in cur_df.columns:
            continue
        b = pd.to_numeric(base_df[col], errors="coerce").to_numpy(dtype=float)
        c = pd.to_numeric(cur_df[col], errors="coerce").to_numpy(dtype=float)
        psi = _psi(b, c, bins=bins)
        st = _psi_status(psi, ok_max, warn_max)
        if st == "alert":
            overall_numeric = "alert"
        elif st == "warning" and overall_numeric == "ok":
            overall_numeric = "warning"
        numeric_results.append(
            {"feature": col, "psi": psi, "status": st}
        )

    cat_results = []
    overall_cat = "ok"
    for col in CATEGORICAL_FEATURES:
        if col not in base_df.columns or col not in cur_df.columns:
            continue
        info = _categorical_drift(base_df[col], cur_df[col], cat_warn, cat_alert)
        info["feature"] = col
        cat_results.append(info)
        st = info["status"]
        if st == "alert":
            overall_cat = "alert"
        elif st == "warning" and overall_cat == "ok":
            overall_cat = "warning"

    if overall_numeric == "alert" or overall_cat == "alert":
        overall = "alert"
    elif overall_numeric == "warning" or overall_cat == "warning":
        overall = "warning"
    else:
        overall = "ok"

    return {
        "baseline_path": str(TRAIN_SNAPSHOT_PATH.relative_to(ROOT)),
        "current_path": str(FEATURES_PATH.relative_to(ROOT)),
        "overall_status": overall,
        "numeric": numeric_results,
        "categorical": cat_results,
        "thresholds": {
            "psi_ok_max": ok_max,
            "psi_warning_max": warn_max,
            "categorical_warning_diff": cat_warn,
            "categorical_alert_diff": cat_alert,
        },
    }


def _write_drift_md(drift: dict[str, Any], path: Path) -> None:
    lines = [
        f"# Drift report — {date.today().isoformat()}",
        "",
        f"**Overall status:** `{drift.get('overall_status', 'unknown')}`",
        "",
    ]
    if "error" in drift:
        lines.append(f"**Error:** {drift['error']}")
        path.write_text("\n".join(lines), encoding="utf-8")
        return

    lines.extend(
        [
            f"- **Baseline:** `{drift['baseline_path']}` (training snapshot)",
            f"- **Current:** `{drift['current_path']}` (latest features export)",
            "",
            "## Numeric features (PSI)",
            "",
            "| Feature | PSI | Status |",
            "| --- | --- | --- |",
        ]
    )
    for row in drift.get("numeric", []):
        psi = row.get("psi")
        psi_s = f"{psi:.4f}" if isinstance(psi, float) and not np.isnan(psi) else "n/a"
        lines.append(f"| {row['feature']} | {psi_s} | {row['status']} |")
    lines.extend(["", "## Categorical features (share shift)", ""])
    for block in drift.get("categorical", []):
        lines.append(f"### {block['feature']} — **{block['status']}** (max |Δshare| = {block['max_abs_share_diff']:.4f})")
        lines.append("")
        lines.append("| Category | Baseline | Current | |Δ| |")
        lines.append("| --- | --- | --- | --- |")
        for s in block.get("top_shifts", []):
            lines.append(
                f"| {s['category']} | {s['baseline_share']:.4f} | {s['current_share']:.4f} | {s['abs_diff']:.4f} |"
            )
        lines.append("")
    th = drift.get("thresholds", {})
    lines.extend(
        [
            "## Thresholds",
            "",
            f"- PSI: `< {th.get('psi_ok_max')}` OK, `< {th.get('psi_warning_max')}` warning, else alert.",
            f"- Categorical: max |Δshare| warning ≥ {th.get('categorical_warning_diff')}, alert ≥ {th.get('categorical_alert_diff')}.",
            "",
            "*Run after each pipeline execution (simulated daily).*",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    cfg = _load_config()
    mon = cfg.get("monitoring", {})
    fp_cost = float(mon.get("false_positive_cost", 10))
    fn_cost = float(mon.get("false_negative_cost", 50))
    alert_delta = float(mon.get("roc_auc_alert_delta", 0.05))
    k_frac = float(mon.get("precision_at_k", 0.10))

    baseline_roc = _baseline_roc_auc(cfg)
    df = _load_scored_frame()

    windows = _monthly_metrics(df, baseline_roc, alert_delta)
    y = df["actual_is_delayed"].to_numpy()
    pred = df["predicted_class"].to_numpy()
    proba = df["predicted_probability"].to_numpy(dtype=float)

    overall_roc = _safe_roc_auc(y, proba)
    overall_alert = overall_roc is not None and overall_roc < baseline_roc - alert_delta

    monitoring_payload = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_roc_auc": baseline_roc,
        "roc_auc_alert_delta": alert_delta,
        "overall": {
            "n_rows": int(len(df)),
            "roc_auc": overall_roc,
            "precision": float(precision_score(y, pred, zero_division=0)),
            "recall": float(recall_score(y, pred, zero_division=0)),
            "f1": float(f1_score(y, pred, zero_division=0)),
            "precision_at_k": _precision_at_k(df, k_frac),
            "k_fraction": k_frac,
            "roc_auc_below_baseline_alert": overall_alert,
            "cost_analysis": _cost_analysis(y, pred, fp_cost, fn_cost),
        },
        "monthly_windows": [asdict(w) for w in windows],
        "any_monthly_roc_alert": any(w.roc_auc_alert for w in windows),
    }

    drift = _run_drift(cfg)
    monitoring_payload["drift_summary"] = {
        "overall_status": drift.get("overall_status"),
        "error": drift.get("error"),
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    day = date.today().isoformat()
    json_path = REPORT_DIR / f"monitoring_{day}.json"
    json_path.write_text(json.dumps(monitoring_payload, indent=2), encoding="utf-8")
    print(f"Wrote {json_path}")

    drift_path = REPORT_DIR / f"drift_{day}.md"
    _write_drift_md(drift, drift_path)
    print(f"Wrote {drift_path}")

    db_payload = {
        "monitoring": monitoring_payload,
        "drift": drift,
    }
    run_at = datetime.now(timezone.utc)
    report_json = json.dumps(db_payload, default=str)
    row = pd.DataFrame([{"run_at": run_at, "report_json": report_json}])
    con = duckdb.connect(str(DB_PATH))
    con.register("_monitoring_row", row)
    con.execute("CREATE OR REPLACE TABLE ml_monitoring AS SELECT * FROM _monitoring_row")
    con.close()
    print("Updated DuckDB table main.ml_monitoring")

    if overall_alert or monitoring_payload["any_monthly_roc_alert"]:
        print("WARNING: ROC-AUC below baseline threshold in one or more windows.")
    if drift.get("overall_status") == "alert":
        print("WARNING: Drift status ALERT — review drift_*.md")


if __name__ == "__main__":
    main()
