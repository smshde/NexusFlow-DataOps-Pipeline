{{
  config(
    materialized = 'incremental',
    unique_key   = 'customer_id',
    schema       = 'silver',
    tags         = ['silver', 'customers']
  )
}}

-- silver_customers
-- PII-masked customer records from bronze layer

with

source as (
    select * from {{ source('bronze', 'raw_customers') }}
    {% if is_incremental() %}
    where _ingestion_ts > (
        select dateadd('hour', -3, max(_processed_ts))
        from {{ this }}
    )
    {% endif %}
),

masked as (
    select
        customer_id,
        -- PII removed — hashed identifiers only
        email_hash,
        phone_hash,
        -- safe demographics
        age_bracket,
        gender,
        country,
        state,
        -- truncated postal — k-anonymized
        left(postal_code, 3) || '**'          as postal_code_prefix,
        -- birth year only — not full DOB
        extract(year from cast(date_of_birth as date))::int as birth_year,
        -- behavioral
        loyalty_tier,
        segment,
        cast(total_orders      as integer)    as total_orders,
        cast(ltv_usd           as float)      as ltv_usd,
        cast(is_email_verified as boolean)    as is_email_verified,
        preferred_device,
        -- registration
        cast(registration_date as date)       as registration_date,
        extract(year  from cast(registration_date as date))::int as registration_year,
        extract(month from cast(registration_date as date))::int as registration_month,
        -- SCD2 metadata
        current_timestamp                     as _valid_from,
        null::timestamp                       as _valid_to,
        true                                  as _is_current,
        md5(customer_id || loyalty_tier || segment) as _record_hash,
        cast(_ingestion_ts as timestamp)      as _ingestion_ts,
        current_timestamp                     as _processed_ts
    from source
    where customer_id is not null
)

select * from masked
