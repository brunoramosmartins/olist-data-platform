-- met_daily_revenue.sql
--
-- Daily revenue. Grain: one row per order_date with activity.
--
-- Formula decision (docs/metrics.md): same monetary field as GMV — total_payment_value
-- from fact_orders — but restricted to orders that have completed as delivered.
-- This matches “recognized” sales for the day of purchase; GMV includes in-flight
-- valid orders (shipped, processing, etc.).

with base as (

    select
        fo.order_date,
        fo.total_payment_value
    from {{ ref('fact_orders') }} as fo
    inner join {{ ref('int_orders_enriched') }} as e
        on fo.order_id = e.order_id
    where e.order_status = 'delivered'
      and fo.item_count >= 1
      and fo.order_date is not null

),

aggregated as (

    select
        order_date,
        sum(coalesce(total_payment_value, 0)) as revenue
    from base
    group by order_date

)

select * from aggregated
