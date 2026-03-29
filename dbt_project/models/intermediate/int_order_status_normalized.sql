-- int_order_status_normalized.sql
--
-- Maps raw order statuses to a simplified set of normalized statuses
-- and adds boolean convenience flags.
--
-- Mapping:
--   created, approved     → placed
--   invoiced, shipped     → in_transit
--   delivered             → delivered
--   canceled              → canceled
--   unavailable, other    → other

with orders as (

    select * from {{ ref('stg_orders') }}

),

normalized as (

    select
        order_id,
        order_status as raw_status,

        case
            when order_status in ('created', 'approved')    then 'placed'
            when order_status in ('invoiced', 'shipped')    then 'in_transit'
            when order_status = 'delivered'                  then 'delivered'
            when order_status = 'canceled'                   then 'canceled'
            else 'other'
        end                                                 as normalized_status,

        (order_status = 'delivered')::int                   as is_delivered,
        (order_status = 'canceled')::int                    as is_canceled

    from orders

)

select * from normalized
