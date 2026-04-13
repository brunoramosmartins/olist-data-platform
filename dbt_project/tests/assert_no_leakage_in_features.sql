-- Anti-leakage checks for fct_order_features (see docs/ml_design.md).
-- 1) Target is_delayed is non-null only for delivered orders with both delivery dates.
-- 2) Target is null when those dates are missing (eligible-for-label set).
-- 3) First order per seller (non-null seller) has no historical seller avg (window excludes current).

with enriched as (

    select * from {{ ref('int_orders_enriched') }}

),

features as (

    select * from {{ ref('fct_order_features') }}

),

eligible_for_label as (

    select
        order_id,
        (
            is_delivered = 1
            and order_delivered_customer_at is not null
            and order_estimated_delivery_at is not null
        ) as is_eligible
    from enriched

),

target_mismatch as (

    select
        f.order_id,
        'is_delayed present but order not eligible for delay label' as violation    from features f
    inner join eligible_for_label e on f.order_id = e.order_id
    where f.is_delayed is not null
      and e.is_eligible = false

),

missing_target as (

    select
        f.order_id,
        'is_delayed null but order eligible for delay label' as violation
    from features f
    inner join eligible_for_label e on f.order_id = e.order_id
    where f.is_delayed is null
      and e.is_eligible = true

),

ranked as (

    select
        order_id,
        seller_avg_delivery_days_historical,
        row_number() over (
            partition by seller_id
            order by order_purchase_timestamp
        ) as seller_row_num
    from features
    where seller_id is not null

),

first_row_has_hist as (

    select
        order_id,
        'first seller order must have null historical avg' as violation
    from ranked
    where seller_row_num = 1
      and seller_avg_delivery_days_historical is not null

)

select * from target_mismatch
union all
select * from missing_target
union all
select * from first_row_has_hist
