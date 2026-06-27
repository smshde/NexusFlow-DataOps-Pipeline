{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'dimension']
  )
}}

-- dim_promotions
-- Campaign and promotion reference dimension
-- Grain: one row per campaign_id (fact_orders joins on campaign_id alone).
-- A campaign can run through multiple channels/mediums in the source data —
-- grouping by (campaign_id, channel, medium) together (the old version)
-- produced multiple rows per campaign_id, which fanned out fact_orders'
-- join (3968 distinct orders became 7953 rows via duplicated joins).
-- Fix: aggregate totals at the true campaign_id grain, and separately pick
-- the single most common channel/medium per campaign for display.

with campaign_totals as (
    select
        utm_campaign                              as campaign_id,
        count(distinct order_id)                  as total_orders,
        sum(total_amount)                         as total_revenue
    from {{ ref('silver_orders') }}
    where utm_campaign is not null
    group by 1
),

campaign_channel_mode as (
    select campaign_id, utm_source, utm_medium
    from (
        select
            utm_campaign as campaign_id,
            utm_source,
            utm_medium,
            row_number() over (
                partition by utm_campaign
                order by count(*) desc
            ) as rn
        from {{ ref('silver_orders') }}
        where utm_campaign is not null
        group by utm_campaign, utm_source, utm_medium
    )
    where rn = 1
),

campaigns as (
    select
        t.campaign_id,
        t.campaign_id                              as campaign_name,
        m.utm_source                               as channel,
        case
            when m.utm_medium = 'cpc'     then 'paid_search'
            when m.utm_medium = 'social'  then 'paid_social'
            when m.utm_medium = 'email'   then 'email'
            when m.utm_medium = 'organic' then 'organic'
            else 'other'
        end                                         as campaign_type,
        m.utm_medium,
        t.total_orders,
        t.total_revenue
    from campaign_totals t
    left join campaign_channel_mode m on t.campaign_id = m.campaign_id
)

select
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as promotion_sk,
    campaign_id,
    campaign_name,
    channel,
    campaign_type,
    utm_medium,
    total_orders,
    total_revenue,
    current_timestamp as _dbt_updated_at
from campaigns
