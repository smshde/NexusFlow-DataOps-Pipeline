{{
  config(
    materialized = 'incremental',
    unique_key   = 'event_id',
    schema       = 'silver',
    tags         = ['silver', 'clickstream']
  )
}}

-- silver_clickstream
-- Deduplicated, typed clickstream events

with

source as (
    select * from {{ source('bronze', 'raw_clickstream') }}
    {% if is_incremental() %}
    where _ingestion_ts > (
        select dateadd('hour', -3, max(_processed_ts))
        from {{ this }}
    )
    {% endif %}
),

cleaned as (
    select
        event_id,
        session_id,
        customer_id,
        event_type,
        cast(event_ts      as timestamp)      as event_ts,
        cast(event_ts      as date)           as event_date,
        extract(year  from cast(event_ts as timestamp))::int as event_year,
        extract(month from cast(event_ts as timestamp))::int as event_month,
        extract(day   from cast(event_ts as timestamp))::int as event_day,
        extract(hour  from cast(event_ts as timestamp))::int as event_hour,
        page_url,
        referrer_url,
        product_sku,
        product_category,
        search_query,
        device_type,
        browser,
        os,
        ip_hash,            -- already hashed at source
        cast(viewport_width    as integer) as viewport_width,
        cast(viewport_height   as integer) as viewport_height,
        cast(scroll_depth_pct  as integer) as scroll_depth_pct,
        cast(time_on_page_sec  as integer) as time_on_page_sec,
        utm_source,
        utm_medium,
        cast(_ingestion_ts as timestamp)   as _ingestion_ts,
        current_timestamp                  as _processed_ts
    from source
    where event_id is not null
      and session_id is not null
      and event_ts is not null
      and event_type in (
          'page_view','product_view','add_to_cart',
          'remove_from_cart','checkout_start','search',
          'wishlist_add','review_view','click'
      )
    qualify row_number() over (
        partition by event_id
        order by _ingestion_ts desc
    ) = 1
)

select * from cleaned
