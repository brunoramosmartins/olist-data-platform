-- assert_no_future_orders.sql
--
-- Business rule: No order should have a purchase date in the future.
-- The Olist dataset is historical (2016-2018). Any order with
-- order_purchased_at > current_timestamp indicates data corruption
-- or ingestion error.
--
-- This test passes when it returns 0 rows.

select
    order_id,
    order_purchased_at
from {{ ref('stg_orders') }}
where order_purchased_at > current_timestamp
