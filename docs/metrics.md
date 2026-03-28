# Metric Definitions

Formal definitions for all business metrics computed by the platform. Each metric is implemented as a dbt model in `dbt_project/models/metrics/`.

---

## Daily GMV (Gross Merchandise Value)

- **Model:** `met_daily_gmv`
- **Definition:** Sum of `payment_value` for all valid orders per day
- **Grain:** 1 row per day
- **Filter:** Valid orders only (see `docs/definitions.md`)

---

## Daily Revenue

- **Model:** `met_daily_revenue`
- **Definition:** Sum of `payment_value` for delivered orders per day
- **Grain:** 1 row per day
- **Filter:** `order_status = 'delivered'`

---

## Daily Delay Rate

- **Model:** `met_daily_delay_rate`
- **Definition:** Count of delayed orders / count of delivered orders per day
- **Grain:** 1 row per day
- **Filter:** Delivered orders with both delivery dates non-null
- **Target range:** Lower is better. Baseline to be established in Phase 4.
