-- Deduplicate geolocation by zip_code_prefix.
-- The raw table has multiple rows per prefix (one per survey response).
-- We keep the average lat/lng per prefix.

with source as (

    select * from {{ source('olist', 'olist_geolocation_dataset') }}

),

deduplicated as (

    select
        geolocation_zip_code_prefix                         as zip_code_prefix,
        avg(cast(geolocation_lat as double))                as latitude,
        avg(cast(geolocation_lng as double))                as longitude,
        trim(lower(
            mode(geolocation_city)
        ))                                                  as city,
        upper(
            mode(geolocation_state)
        )                                                   as state

    from source
    group by geolocation_zip_code_prefix

)

select * from deduplicated
