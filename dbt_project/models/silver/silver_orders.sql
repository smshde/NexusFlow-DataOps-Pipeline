{{
  config(
    materialized     = 'incremental',
    incremental_strategy = 'merge',
    unique_key       = 'order_id',
    schema           = 'silver',
    tags             = ['silver', 'orders']
  )
}}

-- ============================================================
-- silver_orders
-- Cleaned, deduplicated orders from bronze layer
-- Source: Redshift Spectrum over S3 silver/orders/
-- ============================================================

with

source as (
    select * from {{ source('bronze', 'raw_orders') }}
    {% if is_incremental() %}
    where _ingestion_ts > (
        select dateadd('hour', -3, max(_processed_ts))
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by order_id
                   order by updated_at desc
               ) as _row_num
        from source
        where order_id is not null
          and customer_id is not null
          and total_amount >= 0
    )
    where _row_num = 1
),

typed as (
    select
        order_id,
        customer_id,
        order_status,
        cast(order_date  as timestamp) as order_date,
        cast(updated_at  as timestamp) as updated_at,
        items,
        cast(subtotal       as float) as subtotal,
        cast(discount_total as float) as discount_total,
        cast(shipping_fee   as float) as shipping_fee,
        cast(tax_amount     as float) as tax_amount,
        cast(total_amount   as float) as total_amount,
        payment_method,
        payment_status,
        shipping_method,
        shipping_address_city,
        shipping_address_state,
        shipping_address_country,
        cast(estimated_delivery_date as date) as estimated_delivery_date,
        cast(actual_delivery_date   as date) as actual_delivery_date,
        session_id,
        utm_source,
        utm_medium,
        utm_campaign,
        cast(is_gift as boolean)              as is_gift,
        -- derived
        extract(year  from cast(order_date as timestamp)) as order_year,
        extract(month from cast(order_date as timestamp)) as order_month,
        extract(day   from cast(order_date as timestamp)) as order_day,
        extract(hour  from cast(order_date as timestamp)) as order_hour,
        extract(dow   from cast(order_date as timestamp)) as order_dayofweek,
        case when extract(dow from cast(order_date as timestamp))
             in (0, 6) then true else false end           as is_weekend,
        json_array_length(items)                          as item_count,
        case when cast(discount_total as float) > 0
             then true else false end                     as has_discount,
        -- metadata
        cast(_ingestion_ts as timestamp)                  as _ingestion_ts,
        current_timestamp                                 as _processed_ts,
        '1.0.0'                                           as _pipeline_version
    from deduplicated
    where order_status in (
        'pending','confirmed','processing',
        'shipped','delivered','cancelled','refunded'
    )
)

select * from typed
