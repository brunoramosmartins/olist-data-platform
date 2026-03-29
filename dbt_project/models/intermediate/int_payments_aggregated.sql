-- int_payments_aggregated.sql
--
-- Aggregates payment-level data to one row per order.
-- Downstream models join to this instead of stg_order_payments
-- to avoid row duplication from multi-payment orders.

with payments as (

    select * from {{ ref('stg_order_payments') }}

),

aggregated as (

    select
        order_id,
        sum(payment_value)                                  as total_payment_value,
        max(payment_installments)                           as payment_installments_max,
        count(distinct payment_type)                        as payment_method_count,
        string_agg(distinct payment_type, ', ' order by payment_type)
                                                            as payment_type_list,
        count(*)                                            as payment_count

    from payments
    group by order_id

)

select * from aggregated
