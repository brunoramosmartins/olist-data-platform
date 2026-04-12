-- dim_sellers.sql
--
-- Seller dimension. One row per seller_id.

with sellers as (

    select * from {{ ref('stg_sellers') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        seller_city,
        seller_state,
        seller_zip_code_prefix
    from sellers

)

select * from final
