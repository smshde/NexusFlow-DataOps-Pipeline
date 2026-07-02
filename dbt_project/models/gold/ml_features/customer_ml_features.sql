{{
  config(
    materialized = 'table',
    schema       = 'ml_features',
    dist         = 'customer_id',
    sort         = ['snapshot_date'],
    tags         = ['gold', 'ml', 'features', 'rfm']
  )
}}

/*
  customer_ml_features
  =====================
  Daily customer feature snapshot for:
    1. RFM Segmentation (Recency, Frequency, Monetary)
    2. Behavioral signals (device, channel, browse patterns)
    3. Churn risk indicators
    4. LLM-ready customer profile text

  Grain: one row per customer per snapshot_date
  Source: fact_orders + dim_customers + silver_clickstream
*/

with

snapshot_date as (
    select current_date as today
),

-- ── MODE LOOKUPS (per-customer most frequent value) ──────
payment_mode as (
    {{ mode_lookup_cte(ref('fact_orders'), 'customer_id', 'payment_method') }}
),
shipping_mode as (
    {{ mode_lookup_cte(ref('fact_orders'), 'customer_id', 'shipping_method') }}
),
utm_source_mode as (
    {{ mode_lookup_cte(ref('fact_orders'), 'customer_id', 'utm_source') }}
),
device_mode as (
    {{ mode_lookup_cte(ref('silver_clickstream'), 'customer_id', 'device_type') }}
),
browser_mode as (
    {{ mode_lookup_cte(ref('silver_clickstream'), 'customer_id', 'browser') }}
),
os_mode as (
    {{ mode_lookup_cte(ref('silver_clickstream'), 'customer_id', 'os') }}
),

-- ── ORDER FEATURES ────────────────────────────────────────
order_features as (
    select
        o.customer_id,
        count(distinct order_id)                                       as total_orders,
        count(distinct case when is_completed = 1
            then order_id end)                                         as completed_orders,
        count(distinct case when is_cancelled = 1
            then order_id end)                                         as cancelled_orders,
        sum(case when is_completed = 1 then total_amount else 0 end)   as total_revenue,
        avg(case when is_completed = 1 then total_amount end)          as avg_order_value,
        min(order_date)                                                as first_order_date,
        max(order_date)                                                as last_order_date,
        datediff('day', max(cast(order_date as date)), current_date) as recency_days,
        -- Frequency buckets
        count(distinct case when order_date >= dateadd('day', -30, current_date)
            then order_id end)                                         as orders_last_30d,
        count(distinct case when order_date >= dateadd('day', -90, current_date)
            then order_id end)                                         as orders_last_90d,
        count(distinct case when order_date >= dateadd('day', -365, current_date)
            then order_id end)                                         as orders_last_365d,
        -- Revenue buckets
        sum(case when order_date >= dateadd('day', -30, current_date)
            then total_amount else 0 end)                              as revenue_last_30d,
        sum(case when order_date >= dateadd('day', -90, current_date)
            then total_amount else 0 end)                              as revenue_last_90d,
        -- rfm_scores' monetary score reads this column; was missing,
        -- causing "column o.revenue_last_365d does not exist".
        sum(case when order_date >= dateadd('day', -365, current_date)
            then total_amount else 0 end)                              as revenue_last_365d,
        -- Product diversity
        count(distinct order_status)                                   as distinct_statuses,
        avg(item_count)                                                as avg_items_per_order,
        -- Payment (joined per-customer mode, see CTEs above)
        max(pm.payment_method)                                         as preferred_payment,
        max(sm.shipping_method)                                        as preferred_shipping,
        max(um.utm_source)                                             as primary_acquisition_channel,
        -- Weekend vs weekday
        avg(case when is_weekend then 1.0 else 0.0 end)                as weekend_order_pct,
        avg(order_hour)                                                as avg_order_hour,
        -- Returns
        count(distinct case when order_status = 'refunded'
            then order_id end)                                         as total_refunds,
        sum(case when order_status = 'refunded' then total_amount else 0 end) as total_refund_amount,
        count(distinct case when order_status = 'refunded'
            then order_id end) * 1.0 /
            nullif(count(distinct order_id), 0)                        as refund_rate

    from {{ ref('fact_orders') }} o
    left join payment_mode    pm on o.customer_id = pm.customer_id
    left join shipping_mode   sm on o.customer_id = sm.customer_id
    left join utm_source_mode um on o.customer_id = um.customer_id
    group by o.customer_id
),

-- ── CLICKSTREAM BEHAVIORAL FEATURES ──────────────────────
browse_features as (
    select
        c.customer_id,
        count(distinct session_id)                                     as total_sessions,
        count(distinct session_id) * 1.0 /
            nullif(count(distinct date_trunc('day', event_ts)), 0)     as sessions_per_active_day,
        count(*)                                                        as total_events,
        count(case when event_type = 'product_view'   then 1 end)      as product_views,
        count(case when event_type = 'add_to_cart'    then 1 end)      as cart_adds,
        count(case when event_type = 'search'         then 1 end)      as searches,
        count(case when event_type = 'checkout_start' then 1 end)      as checkout_starts,
        avg(time_on_page_sec)                                          as avg_time_on_page_sec,
        avg(scroll_depth_pct)                                          as avg_scroll_depth_pct,
        -- Device (joined per-customer mode, see CTEs above)
        max(dm.device_type)                                            as primary_device,
        max(bm.browser)                                                as primary_browser,
        max(om.os)                                                     as primary_os,
        -- Engagement score
        (
            count(case when event_type = 'product_view'   then 1 end) * 1.0 +
            count(case when event_type = 'add_to_cart'    then 1 end) * 3.0 +
            count(case when event_type = 'checkout_start' then 1 end) * 5.0 +
            count(case when event_type = 'search'         then 1 end) * 2.0
        ) / nullif(count(distinct session_id), 0)                      as engagement_score_per_session,
        max(event_ts)                                                  as last_active_ts,
        datediff('day', max(cast(event_ts as date)), current_date)   as days_since_last_browse

    from {{ ref('silver_clickstream') }} c
    left join device_mode dm on c.customer_id = dm.customer_id
    left join browser_mode bm on c.customer_id = bm.customer_id
    left join os_mode om on c.customer_id = om.customer_id
    where c.customer_id is not null
    group by c.customer_id
),

-- ── CUSTOMER DIMENSION ────────────────────────────────────
customer_attrs as (
    select
        customer_id,
        loyalty_tier,
        loyalty_tier_rank,
        segment,
        age_bracket,
        birth_year,
        gender,
        country,
        state,
        tenure_band,
        ltv_band,
        customer_age_days,
        is_email_verified,
        email_hash
    from {{ ref('dim_customers') }}
    where _is_current = true
),

-- ── RFM SCORING ───────────────────────────────────────────
rfm_scores as (
    select
        o.customer_id,
        -- Recency score (1=worst, 5=best)
        case
            when o.recency_days <= 7   then 5
            when o.recency_days <= 30  then 4
            when o.recency_days <= 90  then 3
            when o.recency_days <= 180 then 2
            else                            1
        end                             as r_score,
        -- Frequency score
        case
            when o.orders_last_365d >= 24 then 5
            when o.orders_last_365d >= 12 then 4
            when o.orders_last_365d >= 6  then 3
            when o.orders_last_365d >= 2  then 2
            else                               1
        end                             as f_score,
        -- Monetary score
        case
            when o.revenue_last_365d >= 5000 then 5
            when o.revenue_last_365d >= 2000 then 4
            when o.revenue_last_365d >= 500  then 3
            when o.revenue_last_365d >= 100  then 2
            else                                  1
        end                             as m_score
    from order_features o
),

-- ── CHURN RISK SCORE ─────────────────────────────────────
churn_signals as (
    select
        o.customer_id,
        -- Higher = more churn risk
        (
            -- Long recency
            case when o.recency_days > 90  then 3
                 when o.recency_days > 30  then 1
                 else 0 end
            -- Declining spend
            + case when o.revenue_last_30d < o.revenue_last_90d / 3
                then 2 else 0 end
            -- High refund rate
            + case when o.refund_rate > 0.3 then 2 else 0 end
            -- No recent browse
            + case when coalesce(b.days_since_last_browse, 999) > 60 then 2 else 0 end
        ) as churn_risk_score,
        case
            when (
                case when o.recency_days > 90  then 3
                     when o.recency_days > 30  then 1
                     else 0 end
                + case when o.revenue_last_30d < o.revenue_last_90d / 3
                    then 2 else 0 end
                + case when o.refund_rate > 0.3 then 2 else 0 end
                + case when coalesce(b.days_since_last_browse, 999) > 60 then 2 else 0 end
            ) >= 6 then 'high'
            when (
                case when o.recency_days > 90  then 3
                     when o.recency_days > 30  then 1
                     else 0 end
                + case when coalesce(b.days_since_last_browse, 999) > 60 then 2 else 0 end
            ) >= 3 then 'medium'
            else 'low'
        end as churn_risk_tier
    from order_features o
    left join browse_features b on o.customer_id = b.customer_id
),

-- ── LLM-READY CUSTOMER PROFILE TEXT ──────────────────────
llm_profile as (
    select
        o.customer_id,
        -- Structured text for RAG / LLM context injection
        'Customer ' || c.age_bracket || ' ' || c.gender ||
        ' in ' || c.state || ', ' || c.country ||
        '. Loyalty: ' || c.loyalty_tier ||
        '. Segment: ' || c.segment ||
        '. Member for ' || c.customer_age_days || ' days.' ||
        ' Total orders: ' || o.total_orders ||
        ', LTV: $' || round(o.total_revenue, 0) ||
        ', Last purchase: ' || o.recency_days || ' days ago.' ||
        ' Preferred payment: ' || coalesce(o.preferred_payment, 'unknown') ||
        '. Preferred device: ' || coalesce(b.primary_device, 'unknown') ||
        '.'                                          as customer_profile_text

    from order_features o
    left join customer_attrs c   on o.customer_id = c.customer_id
    left join browse_features b  on o.customer_id = b.customer_id
),

-- ── FINAL FEATURE SET ─────────────────────────────────────
final as (
    select
        current_date                                  as snapshot_date,
        o.customer_id,
        c.email_hash,
        c.loyalty_tier,
        c.loyalty_tier_rank,
        c.segment,
        c.age_bracket,
        c.gender,
        c.country,
        c.state,
        c.tenure_band,
        c.ltv_band,
        c.customer_age_days,
        c.is_email_verified,
        -- Order features
        o.total_orders,
        o.completed_orders,
        o.cancelled_orders,
        o.total_revenue,
        o.avg_order_value,
        o.first_order_date,
        o.last_order_date,
        o.recency_days,
        o.orders_last_30d,
        o.orders_last_90d,
        o.orders_last_365d,
        o.revenue_last_30d,
        o.revenue_last_90d,
        o.avg_items_per_order,
        o.preferred_payment,
        o.preferred_shipping,
        o.primary_acquisition_channel,
        o.weekend_order_pct,
        o.avg_order_hour,
        o.total_refunds,
        o.refund_rate,
        -- Browse features
        coalesce(b.total_sessions,        0)            as total_sessions,
        coalesce(b.total_events,          0)            as total_events,
        coalesce(b.product_views,         0)            as product_views,
        coalesce(b.cart_adds,             0)            as cart_adds,
        coalesce(b.searches,              0)            as searches,
        coalesce(b.avg_time_on_page_sec,  0)            as avg_time_on_page_sec,
        coalesce(b.avg_scroll_depth_pct,  0)            as avg_scroll_depth_pct,
        b.primary_device,
        b.primary_browser,
        coalesce(b.engagement_score_per_session, 0)     as engagement_score_per_session,
        b.days_since_last_browse,
        -- RFM
        rfm.r_score,
        rfm.f_score,
        rfm.m_score,
        rfm.r_score + rfm.f_score + rfm.m_score        as rfm_total_score,
        -- Churn
        ch.churn_risk_score,
        ch.churn_risk_tier,
        -- LLM profile
        lp.customer_profile_text,
        -- Metadata
        current_timestamp                             as _feature_created_at,
        '{{ var("environment") }}'                      as _environment

    from order_features o
    left join customer_attrs  c   on o.customer_id = c.customer_id
    left join browse_features b   on o.customer_id = b.customer_id
    left join rfm_scores      rfm on o.customer_id = rfm.customer_id
    left join churn_signals   ch  on o.customer_id = ch.customer_id
    left join llm_profile     lp  on o.customer_id = lp.customer_id
)

select * from final
