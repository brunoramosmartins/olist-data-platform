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

Features must use **only information available at order placement time** (`order_purchased_at` for calendar features and window ordering; payment may still be pending, but aggregates reflect what is recorded on the order at build time). `docs/definitions.md` references prediction at `order_approved_at`; this table uses **`order_purchase_timestamp`** for calendar features and window ordering — align with your training policy if you require approval time instead.

**Forbidden (not in `fct_order_features`):**
- Actual delivery timestamps, `delivery_days` / `delay_days` of the **current** order (leakage)
- `review_score` and any post-delivery review fields
- Carrier handoff timestamps after purchase
- Raw high-cardinality text (e.g. full city names) unless hashed — we expose **states** only at order time

**Rejected for this phase (with reasons):**

| Candidate | Reason |
|-----------|--------|
| `review_score` | Observed after delivery |
| `product_photos_qty`, `product_name_length` | Omitted to keep v1 narrow; safe to add later from `dim_products` / staging |
| `seller_city`, `customer_city` | Higher cardinality; states + `customer_seller_same_state` used instead |
| Geolocation distance | Requires extra join and lat/long; not in Phase 5 scope |
| Current-order `delivery_days` | Direct leakage for delay prediction |
| `order_status` at training export | Status can change after purchase; omitted to avoid ambiguous snapshot |

---

## `fct_order_features` — column reference

All columns below are implemented in `dbt_project/models/features/fct_order_features.sql` and documented in `_features__models.yml`.

| Feature (column) | Source / logic | Type | Leakage-safe | Justification |
|------------------|----------------|------|--------------|---------------|
| `order_day_of_week` | `extract(dow from order_purchase_timestamp)` | int | yes | Clock/calendar at purchase |
| `order_hour` | `extract(hour from …)` | int | yes | Same |
| `order_month` | `extract(month from …)` | int | yes | Same |
| `total_payment_value` | `int_orders_enriched` / payments agg | float | yes | Payment plan known at order capture |
| `payment_installments` | `payment_installments_max` | int | yes | Same |
| `payment_type_count` | `payment_method_count` | int | yes | Same |
| `product_weight_g` | Primary line → `dim_products` | float | yes | Product master data |
| `product_volume_cm3` | `length_cm * height_cm * width_cm` | float | yes | Same |
| `product_category` | Primary line → `dim_products` | string | yes | Same |
| `customer_state` | `dim_customers` | string | yes | Customer master |
| `seller_state` | `dim_sellers` | string | yes | Seller master |
| `customer_seller_same_state` | compare states | int (0/1) | yes | Geography at purchase |
| `seller_avg_delivery_days_historical` | `avg(delivery_days) OVER (PARTITION BY seller_id ORDER BY order_purchase_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)` | float | yes | Uses **prior** orders’ realized lead time only; current row excluded |
| `freight_value` | `fact_orders.total_freight_value` | float | yes | Line freight known at checkout |
| `freight_ratio` | `safe_divide(freight_value, total_payment_value)` | float | yes | Derived from same-time fields |
| `estimated_delivery_days` | `int_orders_enriched` | int | yes | Promised lead time shown at purchase |
| `seller_id` | Primary line seller | string | yes* | Identifier; *optional drop for tree models to limit memorization |
| `order_id` | key | string | yes* | Exclude from matrix; join key only |
| `order_purchase_timestamp` | split / ordering | ts | yes | Known at purchase |
| `is_delayed` | label | int / null | **target** | Null unless delivered + both dates (definitions.md) |

Historical averages use **window functions only** (no self-joins). Training scripts should drop `order_id`, `order_purchase_timestamp` (or keep only for splits), and optionally `seller_id`, per model needs.

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
