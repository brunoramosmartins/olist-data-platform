# ML Design Document

## Problem Statement

Predict whether an e-commerce order will be delivered late (after the estimated delivery date) at the time the order is created. This enables proactive logistics interventions and improved customer communication.

---

## Target Variable

| Field | Type | Definition |
|---|---|---|
| `is_delayed` | binary (0/1) | 1 if `order_delivered_customer_date > order_estimated_delivery_date`, else 0 |

See `docs/definitions.md` for the complete delay definition.

---

## Feature Constraints

Features must use **only information available at order creation time** (`order_approved_at`). This prevents future leakage.

**Allowed features (examples):**
- Order metadata: payment type, payment installments, total payment value
- Product attributes: category, weight, dimensions, number of photos
- Seller attributes: city, state, historical delay rate (computed on past data only)
- Customer attributes: city, state
- Temporal: day of week, month, estimated delivery lead time
- Geographic: distance between seller and customer (via geolocation)

**Forbidden features:**
- `order_delivered_customer_date` (target leakage)
- `order_delivered_carrier_date` (post-creation event)
- `review_score` (post-delivery event)
- Any field that only exists after order creation

---

## Evaluation Plan

### Metrics

| Metric | Purpose |
|---|---|
| **F1-score** | Primary metric — balances precision and recall for imbalanced classes |
| **Precision** | Cost of false positives (unnecessary interventions) |
| **Recall** | Cost of false negatives (missed delays) |
| **ROC-AUC** | Threshold-independent discrimination ability |

### Validation Strategy

- **Temporal split:** Train on orders before a cutoff date, test on orders after. No random split — this simulates production behavior
- **No data from the future:** Features for the test set are computed using only data available before each order's creation time

---

## Baseline Approach

1. **Model:** Logistic Regression (interpretable, fast, good baseline)
2. **Second model:** Random Forest or Gradient Boosting (if baseline is insufficient)
3. **Feature engineering:** Handled entirely by dbt (`fct_order_features`) — Python reads a clean Parquet file
4. **Hyperparameter tuning:** Grid search with temporal cross-validation

---

## Data Flow

```
dbt (fct_order_features) ── export ──> data/ml/features.parquet ── read ──> ml/train.py
                                                                             ml/predict.py
```

The Parquet file is the contract between dbt and ML. dbt owns the SQL; Python owns the modeling.

---

## Model Tracking

Model artifacts are tracked via `ml/current_model.yaml` (no MLflow or W&B):

```yaml
version: "v1"
model_path: "ml/models/model_v1.joblib"
trained_at: "2024-01-15T10:30:00"
metrics:
  f1: 0.72
  precision: 0.68
  recall: 0.77
  roc_auc: 0.81
data_snapshot: "ml/data_snapshots/features_v1.parquet"
```
