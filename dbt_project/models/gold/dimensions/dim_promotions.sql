{{
  config(
    materialized = 'table',
    schema       = 'gold',
    tags         = ['gold', 'dimension']
  )
}}

-- dim_promotions
-- Campaign and promotion reference dimension

with campaigns as (
    select
        utm_campaign                              as campaign_id,
        utm_campaign                              as campaign_name,
        utm_source                                as channel,
        case
            when utm_medium = 'cpc'     then 'paid_search'
            when utm_medium = 'social'  then 'paid_social'
            when utm_medium = 'email'   then 'email'
            when utm_medium = 'organic' then 'organic'
            else 'other'
        end                                       as campaign_type,
        utm_medium,
        count(distinct order_id)                  as total_orders,
        sum(total_amount)                         as total_revenue
    from {{ ref('silver_orders') }}
    where utm_campaign is not null
    group by 1,2,3,4,5
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
