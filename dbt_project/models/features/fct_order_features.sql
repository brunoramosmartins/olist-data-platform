-- fct_order_features.sql
--
-- Order-level feature table for ML (training + inference). Grain: 1 row per order_id.
-- Features use only information knowable at order placement time, except the target
-- is_delayed and helper columns used for windows (stripped before publish).
--
-- Historical seller delivery: AVG(prior orders' realized delivery_days) via window
-- with ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING (no self-join).

with enriched as (

    select * from {{ ref('int_orders_enriched') }}

),

fact as (

    select * from {{ ref('fact_orders') }}

),

primary_item as (

    select
        order_id,
        product_id,
        seller_id
    from (
        select
            oi.order_id,
            oi.product_id,
            oi.seller_id,
            row_number() over (
                partition by oi.order_id
                order by (oi.price + oi.freight_value) desc, oi.item_sequence asc
            ) as rn
        from {{ ref('stg_order_items') }} as oi
    )
    where rn = 1

),

dim_customers as (

    select * from {{ ref('dim_customers') }}

),

dim_sellers as (

    select * from {{ ref('dim_sellers') }}

),

dim_products as (

    select * from {{ ref('dim_products') }}

),

base as (

    select
        e.order_id,
        e.order_purchased_at as order_purchase_timestamp,
        e.is_delayed,
        e.total_payment_value,
        e.payment_installments_max as payment_installments,
        e.payment_method_count as payment_type_count,
        e.estimated_delivery_days,
        e.delivery_days,
        extract(dow from e.order_purchased_at) as order_day_of_week,
        extract(hour from e.order_purchased_at) as order_hour,
        extract(month from e.order_purchased_at) as order_month,
        fo.total_freight_value as freight_value,
        fo.seller_id,
        p.product_category,
        p.weight_g as product_weight_g,
        case
            when p.length_cm is not null
                 and p.height_cm is not null
                 and p.width_cm is not null
            then cast(p.length_cm as double) * cast(p.height_cm as double) * cast(p.width_cm as double)
        end as product_volume_cm3,
        dc.customer_state,
        ds.seller_state,
        case
            when dc.customer_state is not null
                 and ds.seller_state is not null
                 and dc.customer_state = ds.seller_state
            then 1
            when dc.customer_state is not null
                 and ds.seller_state is not null
            then 0
        end as customer_seller_same_state
    from enriched as e
    inner join fact as fo
        on e.order_id = fo.order_id
    left join primary_item as pi
        on e.order_id = pi.order_id
    inner join dim_customers as dc
        on fo.customer_key = dc.customer_key
    left join dim_sellers as ds
        on fo.seller_id = ds.seller_id
    left join dim_products as p
        on pi.product_id = p.product_id

),

with_hist as (

    select
        order_id,
        order_purchase_timestamp,
        is_delayed,
        total_payment_value,
        payment_installments,
        payment_type_count,
        estimated_delivery_days,
        order_day_of_week,
        order_hour,
        order_month,
        freight_value,
        seller_id,
        product_category,
        product_weight_g,
        product_volume_cm3,
        customer_state,
        seller_state,
        customer_seller_same_state,
        avg(delivery_days) over (
            partition by seller_id
            order by order_purchase_timestamp
            rows between unbounded preceding and 1 preceding
        ) as seller_avg_delivery_days_historical,
        {{ safe_divide('freight_value', 'coalesce(total_payment_value, 0)') }} as freight_ratio
    from base

)

select
    order_id,
    order_purchase_timestamp,
    order_day_of_week,
    order_hour,
    order_month,
    total_payment_value,
    payment_installments,
    payment_type_count,
    product_weight_g,
    product_volume_cm3,
    product_category,
    customer_state,
    seller_state,
    customer_seller_same_state,
    seller_avg_delivery_days_historical,
    freight_value,
    freight_ratio,
    estimated_delivery_days,
    seller_id,
    is_delayed
from with_hist
