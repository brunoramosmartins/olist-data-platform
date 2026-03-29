-- int_orders_enriched.sql
--
-- Keystone intermediate model. Joins orders with payments, normalized
-- status, and reviews. Computes delay metrics following definitions
-- from docs/definitions.md.
--
-- Grain: 1 row per order_id (matches stg_orders).

with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('int_payments_aggregated') }}

),

status as (

    select * from {{ ref('int_order_status_normalized') }}

),

-- Deduplicate reviews: keep the most recent review per order
reviews as (

    select * from (
        select
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_created_at,
            review_answered_at,
            row_number() over (
                partition by order_id
                order by review_created_at desc
            ) as rn
        from {{ ref('stg_order_reviews') }}
    )
    where rn = 1

),

enriched as (

    select
        -- Order core
        o.order_id,
        o.customer_id,
        o.order_purchased_at,
        o.order_approved_at,
        o.order_delivered_carrier_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,

        -- Status
        s.raw_status                                        as order_status,
        s.normalized_status,
        s.is_delivered,
        s.is_canceled,

        -- Payments
        p.total_payment_value,
        p.payment_installments_max,
        p.payment_method_count,
        p.payment_type_list,
        p.payment_count,

        -- Reviews (nullable — not all orders have reviews)
        r.review_score,
        r.review_comment_title,
        r.review_comment_message,

        -- Delivery metrics (only for delivered orders with both dates)
        case
            when s.is_delivered = 1
                 and o.order_delivered_customer_at is not null
                 and o.order_purchased_at is not null
            then datediff('day', o.order_purchased_at, o.order_delivered_customer_at)
        end                                                 as delivery_days,

        case
            when o.order_estimated_delivery_at is not null
                 and o.order_purchased_at is not null
            then datediff('day', o.order_purchased_at, o.order_estimated_delivery_at)
        end                                                 as estimated_delivery_days,

        -- Delay metrics (per docs/definitions.md)
        case
            when s.is_delivered = 1
                 and o.order_delivered_customer_at is not null
                 and o.order_estimated_delivery_at is not null
            then datediff('day', o.order_estimated_delivery_at, o.order_delivered_customer_at)
        end                                                 as delay_days,

        case
            when s.is_delivered = 1
                 and o.order_delivered_customer_at is not null
                 and o.order_estimated_delivery_at is not null
            then (o.order_delivered_customer_at > o.order_estimated_delivery_at)::int
        end                                                 as is_delayed

    from orders o
    left join payments p on o.order_id = p.order_id
    left join status s on o.order_id = s.order_id
    left join reviews r on o.order_id = r.order_id

)

select * from enriched
