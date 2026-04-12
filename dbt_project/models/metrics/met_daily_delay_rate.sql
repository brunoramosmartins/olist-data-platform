-- met_daily_delay_rate.sql
--
-- Daily delay rate. Grain: one row per order_date with eligible delivered orders.
-- Eligible: delivered, both delivery and estimated dates present, is_delayed defined.
-- Formula: delayed count / eligible delivered count (safe_divide macro).

with eligible as (

    select
        fo.order_date,
        fo.is_delayed
    from {{ ref('fact_orders') }} as fo
    inner join {{ ref('int_orders_enriched') }} as e
        on fo.order_id = e.order_id
    where e.order_status = 'delivered'
      and fo.delivery_date is not null
      and fo.estimated_delivery_date is not null
      and fo.is_delayed is not null
      and fo.order_date is not null

),

aggregated as (

    select
        order_date,
        {{ safe_divide(
            'sum(case when is_delayed = 1 then 1 else 0 end)',
            'count(*)'
        ) }} as delay_rate,
        count(*) as delivered_eligible_orders,
        sum(case when is_delayed = 1 then 1 else 0 end) as delayed_orders
    from eligible
    group by order_date

)

select * from aggregated
