with source as (

    select * from {{ source('olist', 'olist_customers_dataset') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        trim(lower(customer_city))                          as customer_city,
        upper(customer_state)                               as customer_state

    from source

)

select * from renamed
