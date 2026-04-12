-- fact_orders.sql
--
-- Order-level fact table (star schema). Grain: exactly one row per order_id.
-- Item measures are aggregated from stg_order_items; primary seller and category
-- come from the line with highest (price + freight_value), then lowest item_sequence.

with enriched as (

    select * from {{ ref('int_orders_enriched') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

dim_customers as (

    select * from {{ ref('dim_customers') }}

),

item_agg as (

    select
        order_id,
        count(*) as item_count,
        sum(freight_value) as total_freight_value
    from {{ ref('stg_order_items') }}
    group by order_id

),

primary_line as (

    select
        order_id,
        seller_id,
        product_category
    from (
        select
            oi.order_id,
            oi.seller_id,
            coalesce(
                nullif(trim(p.product_category_name), ''),
                'uncategorized'
            ) as product_category,
            row_number() over (
                partition by oi.order_id
                order by (oi.price + oi.freight_value) desc, oi.item_sequence asc
            ) as rn
        from {{ ref('stg_order_items') }} as oi
        left join {{ ref('stg_products') }} as p
            on oi.product_id = p.product_id
    )
    where rn = 1

),

final as (

    select
        e.order_id,
        dc.customer_key,
        cast(e.order_purchased_at as date) as order_date,
        cast(e.order_delivered_customer_at as date) as delivery_date,
        cast(e.order_estimated_delivery_at as date) as estimated_delivery_date,
        coalesce(ia.item_count, 0) as item_count,
        coalesce(ia.total_freight_value, 0.0) as total_freight_value,
        pl.product_category,
        pl.seller_id,
        e.total_payment_value,
        e.delivery_days,
        e.delay_days,
        e.is_delayed,
        e.review_score
    from enriched e
    inner join customers c
        on e.customer_id = c.customer_id
    inner join dim_customers dc
        on c.customer_unique_id = dc.customer_unique_id
    left join item_agg ia
        on e.order_id = ia.order_id
    left join primary_line pl
        on e.order_id = pl.order_id

)

select * from final
