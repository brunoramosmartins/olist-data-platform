# Business Definitions

This document defines the core business concepts used throughout the Olist Data Platform. All downstream models, metrics, and ML features must conform to these definitions. No definition uses "maybe", "usually", or "it depends".

---

## Valid Order

An order is **valid** when:

1. `order_status` is one of: `delivered`, `shipped`, `invoiced`, `processing`
2. The order has **at least one** associated record in `order_items`

Orders with `order_status` = `canceled` or `unavailable` are **excluded** from all analytical and ML models.

---

## Delay

A delivered order is **delayed** when:

1. `order_status` = `delivered`
2. `order_delivered_customer_date` is **not null**
3. `order_estimated_delivery_date` is **not null**
4. `order_delivered_customer_date > order_estimated_delivery_date`

If either date is null, the order **cannot** be classified as delayed or on-time and must be excluded from delay-related metrics and ML targets.

---

## Delay Amount

The delay amount in days is calculated as:

```sql
datediff('day', order_estimated_delivery_date, order_delivered_customer_date)
```

- **Positive value** = order was delivered late (delayed)
- **Zero** = delivered on the estimated date (on-time)
- **Negative value** = delivered early (on-time)

---

## ML Scope

- **Problem type:** Binary classification
- **Target variable:** `is_delayed` (1 = delayed, 0 = on-time)
- **Prediction point:** At order creation time (`order_approved_at`)
- **Feature constraint:** Only information available at order creation time may be used as features. No future-leaking data (e.g., `order_delivered_customer_date`, `review_score`) is allowed in the feature set
- **Eligible orders:** Only delivered orders with both delivery dates non-null
