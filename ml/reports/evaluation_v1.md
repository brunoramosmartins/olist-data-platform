# Evaluation report — v1 Logistic Regression

Generated: `2026-04-13T00:32:24.932380+00:00` (UTC)

## Model

- **Algorithm:** Logistic Regression (default hyperparameters aside from `max_iter=2000`, `random_state=42`).
- **Target:** `is_delayed` (1 = late vs estimated delivery, 0 = on-time), rows with non-null label only.
- **Preprocessing:** `ColumnTransformer` — numeric: median imputation + `StandardScaler`; categorical: constant `"missing"` + one-hot (`max_categories=40`, `handle_unknown='ignore'`).
- **Excluded from X:** `order_id`, `order_purchase_timestamp`, `seller_id` (identifier / leakage risk for linear baseline).

## Temporal split (purchase timestamp)

| Split | Rule | Rows |
| --- | --- | --- |
| Train | `<2018-04-01` | 64320 |
| Validation | `2018-04-01` to `< 2018-07-01` | 19643 |
| Test | `>= 2018-07-01` | 12507 |

## Metrics

### Validation (used for monitoring; no hyperparameter search in v1)

| Metric | Value |
| --- | --- |
| ROC-AUC | 0.7862180979456096 |
| Precision | 0.3636 |
| Recall | 0.0160 |
| F1 | 0.0307 |

**Confusion matrix (validation)** — rows=true, cols=predicted:

| | Pred 0 | Pred 1 |
| --- | --- | --- |
| True 0 | 18615 | 28 |
| True 1 | 984 | 16 |

### Test (held-out)

| Metric | Value |
| --- | --- |
| ROC-AUC | 0.646993164461404 |
| Precision | 0.3043 |
| Recall | 0.0150 |
| F1 | 0.0285 |

**Confusion matrix (test)**

| | Pred 0 | Pred 1 |
| --- | --- | --- |
| True 0 | 11539 | 32 |
| True 1 | 922 | 14 |

## Feature importance (linear coefficients, top 25 by |coef|)

| Rank | Feature | Coefficient |
| --- | --- | --- |
| 1 | `cat__customer_state_AL` | 1.2661 |
| 2 | `cat__customer_state_PR` | -1.2129 |
| 3 | `cat__customer_state_MG` | -1.1410 |
| 4 | `cat__customer_state_SP` | -0.9710 |
| 5 | `cat__customer_state_DF` | -0.9052 |
| 6 | `cat__seller_state_MA` | 0.8384 |
| 7 | `num__estimated_delivery_days` | -0.7599 |
| 8 | `cat__customer_state_MA` | 0.7067 |
| 9 | `cat__customer_state_PA` | 0.6821 |
| 10 | `cat__product_category_malas_acessorios` | -0.6433 |
| 11 | `cat__seller_state_GO` | -0.6297 |
| 12 | `cat__product_category_market_place` | -0.6218 |
| 13 | `cat__product_category_audio` | 0.5775 |
| 14 | `cat__product_category_eletrodomesticos` | -0.5611 |
| 15 | `cat__customer_state_CE` | 0.5582 |
| 16 | `cat__customer_state_GO` | -0.5240 |
| 17 | `num__customer_seller_same_state` | -0.5219 |
| 18 | `cat__seller_state_PE` | -0.5081 |
| 19 | `cat__customer_state_SE` | 0.4965 |
| 20 | `cat__customer_state_RS` | -0.4722 |
| 21 | `cat__product_category_construcao_ferramentas_construcao` | 0.4546 |
| 22 | `cat__customer_state_MS` | -0.4433 |
| 23 | `cat__product_category_telefonia_fixa` | -0.3871 |
| 24 | `cat__seller_state_RS` | -0.3842 |
| 25 | `cat__customer_state_ES` | -0.3792 |

## Limitations

- Baseline linear model; interactions and non-linearities not captured.
- One-hot with `max_categories` collapses rare categories — some signal loss.
- `seller_id` dropped; seller-specific effects only via state + historical delivery average.
- Class imbalance may skew precision/recall; consider `class_weight` or resampling in later versions.
- Test performance is a single temporal slice; drift not assessed here.

---
*Regenerate this file with `python ml/train.py` after updating `data/ml/features.parquet`.*
