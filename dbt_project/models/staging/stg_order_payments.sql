with source as (

    select * from {{ source('olist', 'olist_order_payments_dataset') }}

),

renamed as (

    select
        order_id,
        payment_sequential                                  as payment_sequence,
        lower(trim(payment_type))                           as payment_type,
        cast(payment_installments as int)                   as payment_installments,
        cast(payment_value as double)                       as payment_value

    from source

)

select * from renamed
