-- met_daily_gmv.sql
--
-- Daily GMV (gross merchandise value). Grain: one row per order_date with activity.
-- Formula: SUM(total_payment_value) for valid orders only (docs/definitions.md).

with base as (

    select
        fo.order_date,
        fo.total_payment_value
    from {{ ref('fact_orders') }} as fo
    inner join {{ ref('int_orders_enriched') }} as e
        on fo.order_id = e.order_id
    where e.order_status in ('delivered', 'shipped', 'invoiced', 'processing')
      and fo.item_count >= 1
      and fo.order_date is not null

),

aggregated as (

    select
        order_date,
        sum(coalesce(total_payment_value, 0)) as gmv
    from base
    group by order_date

)

select * from aggregated
