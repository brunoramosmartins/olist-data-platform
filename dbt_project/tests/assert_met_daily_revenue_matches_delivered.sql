-- Total revenue across days must equal sum of payments for delivered valid orders
with from_metric as (

    select round(coalesce(sum(revenue), 0), 6) as total from {{ ref('met_daily_revenue') }}

),

from_fact as (

    select round(coalesce(sum(coalesce(fo.total_payment_value, 0)), 0), 6) as total
    from {{ ref('fact_orders') }} as fo
    inner join {{ ref('int_orders_enriched') }} as e
        on fo.order_id = e.order_id
    where e.order_status = 'delivered'
      and fo.item_count >= 1

)

select m.total as metric_total, f.total as fact_total
from from_metric m
cross join from_fact f
where m.total is distinct from f.total
