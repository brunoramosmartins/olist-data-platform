# Metric Definitions

Formal definitions for all business metrics computed by the platform. Each metric is implemented as a single dbt model in `dbt_project/models/metrics/`. Amounts use **`total_payment_value`** from `fact_orders` (order-level sum of payments from `int_payments_aggregated`), in **BRL**. The Olist extract is already in currency units (not integer cents); the optional macro `cents_to_currency` exists for future sources stored as cents.

---

## Valid order (shared filter)

All metrics that reference “valid orders” apply the definition in **`docs/definitions.md`**:

1. Raw `order_status` ∈ `delivered`, `shipped`, `invoiced`, `processing`
2. At least one line item: `fact_orders.item_count >= 1`

---

## Daily GMV (Gross Merchandise Value)

| | |
|---|---|
| **Model** | `met_daily_gmv` |
| **Grain** | One row per **`order_date`** (purchase date) that has at least one qualifying order |
| **Formula** | `SUM(COALESCE(total_payment_value, 0))` over valid orders |
| **SQL filter** | Join `fact_orders` to `int_orders_enriched`; valid status list + `item_count >= 1` + `order_date IS NOT NULL` |

GMV is the single top-line total for in-flight and completed valid commerce on the purchase date.

---

## Daily Revenue

| | |
|---|---|
| **Model** | `met_daily_revenue` |
| **Grain** | One row per **`order_date`** with at least one qualifying order |
| **Formula** | `SUM(COALESCE(total_payment_value, 0))` |
| **SQL filter** | Valid-order slice **and** `order_status = 'delivered'` |

**Formula decision:** Revenue uses the **same** monetary field as GMV (`total_payment_value`) so figures are comparable. The **only** change versus GMV is the status filter: **delivered only**. That matches “recognized” completed sales on the purchase date, while GMV still includes orders that are shipped or in processing. Alternative definitions (e.g. net of refunds, freight-only) would require new source fields and are out of scope here.

---

## Daily Delay Rate

| | |
|---|---|
| **Model** | `met_daily_delay_rate` |
| **Grain** | One row per **`order_date`** with at least one **eligible** delivered order |
| **Eligible rows** | `order_status = 'delivered'` and `delivery_date` and `estimated_delivery_date` both non-null and `is_delayed` in (0, 1) (see `docs/definitions.md`) |
| **Formula** | `delayed_orders / delivered_eligible_orders` using the **`safe_divide`** macro ( **`NULL`** if denominator were zero; never happens for an existing group, but keeps logic consistent) |
| **Range** | `delay_rate` ∈ [0, 1] |

Supporting columns **`delivered_eligible_orders`** and **`delayed_orders`** are exposed for auditing and tests.

---

## Macros

| Macro | Purpose |
|---|---|
| `safe_divide(numerator, denominator)` | SQL expressions as strings; returns `NULL` if denominator is null or zero |
| `cents_to_currency(expr)` | Divide by100; optional if a source stores amounts as integer cents |

---

## Building and testing

```bash
cd dbt_project
dbt build --selector metrics
```

Plain `dbt build --select path:models/metrics` does **not** build upstream refs (e.g. `fact_orders`). Prefer **`--selector metrics`** or **`dbt build --select "+path:models/metrics"`**.
