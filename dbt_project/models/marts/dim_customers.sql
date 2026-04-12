-- dim_customers.sql
--
-- Customer dimension. One row per customer_unique_id (natural person across orders).

with customers as (

    select * from {{ ref('stg_customers') }}

),

deduped as (

    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        row_number() over (
            partition by customer_unique_id
            order by customer_id
        ) as rn
    from customers

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_key,
        customer_unique_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix
    from deduped
    where rn = 1

)

select * from final
