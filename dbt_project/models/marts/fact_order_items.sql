-- fact_order_items.sql
--
-- Item-level fact table. Grain: one row per order line (order_id + order_item_id).

with items as (

    select * from {{ ref('stg_order_items') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

sellers as (

    select * from {{ ref('dim_sellers') }}

),

final as (

    select
        i.order_id,
        i.item_sequence as order_item_id,
        i.product_id,
        i.seller_id,
        p.product_key,
        s.seller_key,
        i.price,
        i.freight_value
    from items i
    inner join products p on i.product_id = p.product_id
    inner join sellers s on i.seller_id = s.seller_id

)

select * from final
