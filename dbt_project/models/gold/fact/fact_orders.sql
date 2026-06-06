{{
  config(
    materialized     = 'incremental',
    incremental_strategy = 'merge',
    unique_key       = 'order_sk',
    dist             = 'order_sk',
    sort             = ['order_date', 'customer_sk'],
    tags             = ['gold', 'fact', 'orders'],
    meta             = {
      'owner': 'data-engineering',
      'domain': 'commerce',
      'description': 'Fact table for all e-commerce orders with full line-item detail',
      'sla': 'daily_6am',
      'pii': false
    }
  )
}}

/*
  fact_orders
  ===========
  Central fact table capturing all order transactions.
  Grain: one row per order (order_id).
  Conforms to star schema connecting to:
    - dim_customers
    - dim_products (via bridge table for multi-item orders)
    - dim_date
    - dim_geography
    - dim_payment
    - dim_shipping
*/

with

-- ── SOURCE SILVER ORDERS ──────────────────────────────────
silver_orders as (
    select * from {{ ref('silver_orders') }}
    {% if is_incremental() %}
    where _processed_ts > (select max(_processed_ts) from {{ this }})
    {% endif %}
),

-- ── DIMENSION LOOKUPS ────────────────────────────────────
dim_customers as (
    select
        customer_sk,
        customer_id,
        loyalty_tier,
        segment,
        age_bracket,
        country,
        preferred_device
    from {{ ref('dim_customers') }}
    where _is_current = true
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- ── PROMOTION / COUPON REFERENCE ─────────────────────────
dim_promotions as (
    select
        campaign_id,
        campaign_name,
        campaign_type,
        channel
    from {{ ref('dim_promotions') }}
),

-- ── MAIN TRANSFORMATION ───────────────────────────────────
orders_enriched as (
    select
        -- ── SURROGATE KEY ──
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_sk,

        -- ── NATURAL KEYS ──
        o.order_id,
        o.customer_id,
        o.session_id,

        -- ── FOREIGN KEYS ──
        coalesce(c.customer_sk, -1)                            as customer_sk,
        d_order.date_sk                                        as order_date_sk,
        d_delivery.date_sk                                     as delivery_date_sk,

        -- ── DATES ──
        o.order_date,
        cast(o.order_date as date)                             as order_date_day,
        o.estimated_delivery_date,
        o.actual_delivery_date,
        datediff('day',
            cast(o.order_date as date),
            coalesce(o.actual_delivery_date, o.estimated_delivery_date)
        )                                                       as delivery_days,
        case
            when o.actual_delivery_date <= o.estimated_delivery_date then true
            else false
        end                                                     as is_on_time_delivery,

        -- ── STATUS ──
        o.order_status,
        o.payment_status,
        o.payment_method,
        o.shipping_method,

        -- ── GEOGRAPHY ──
        o.shipping_address_city,
        o.shipping_address_state,
        o.shipping_address_country,

        -- ── ATTRIBUTION ──
        o.utm_source,
        o.utm_medium,
        o.utm_campaign,
        coalesce(p.campaign_type, 'unknown')                   as campaign_type,
        coalesce(p.channel,       'unknown')                   as channel,

        -- ── AMOUNTS ──
        o.subtotal,
        o.discount_total,
        o.shipping_fee,
        o.tax_amount,
        o.total_amount,
        o.total_amount - o.discount_total - o.shipping_fee
            - o.tax_amount                                      as gross_margin_usd,

        -- ── FLAGS ──
        o.is_gift,
        o.has_discount,
        o.item_count,

        -- ── CUSTOMER ENRICHMENT (at time of order) ──
        coalesce(c.loyalty_tier,      'unknown')               as customer_loyalty_tier,
        coalesce(c.segment,           'unknown')               as customer_segment,
        coalesce(c.age_bracket,       'unknown')               as customer_age_bracket,
        coalesce(c.country,           'unknown')               as customer_country,
        coalesce(c.preferred_device,  'unknown')               as customer_preferred_device,

        -- ── TIME INTELLIGENCE ──
        o.order_year,
        o.order_month,
        o.order_day,
        o.order_hour,
        o.order_dayofweek,
        o.is_weekend,

        -- ── COMPUTED METRICS ──
        case
            when o.order_status = 'delivered' then 1 else 0
        end                                                     as is_completed,
        case
            when o.order_status in ('cancelled', 'refunded') then 1 else 0
        end                                                     as is_cancelled,
        case
            when o.order_status = 'refunded' then o.total_amount else 0
        end                                                     as refund_amount,

        -- ── AOV BAND ──
        case
            when o.total_amount <  25  then 'micro'
            when o.total_amount <  100 then 'small'
            when o.total_amount <  250 then 'medium'
            when o.total_amount <  500 then 'large'
            else                            'whale'
        end                                                     as order_value_band,

        -- ── METADATA ──
        o._ingestion_ts,
        o._processed_ts,
        o._pipeline_version,
        current_timestamp()                                     as _dbt_updated_at

    from silver_orders o
    left join dim_customers  c
        on o.customer_id = c.customer_id
    left join dim_date d_order
        on cast(o.order_date as date) = d_order.full_date
    left join dim_date d_delivery
        on o.actual_delivery_date = d_delivery.full_date
    left join dim_promotions p
        on o.utm_campaign = p.campaign_id
),

-- ── FINAL SELECT ──────────────────────────────────────────
final as (
    select * from orders_enriched
)

select * from final
