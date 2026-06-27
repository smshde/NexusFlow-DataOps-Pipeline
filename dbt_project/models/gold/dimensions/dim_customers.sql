{{
  config(
    materialized = 'table',
    dist         = 'customer_sk',
    sort         = ['customer_id', '_valid_from'],
    tags         = ['gold', 'dimension', 'scd2'],
    meta         = {
      'owner': 'data-engineering',
      'description': 'Customer dimension with SCD Type 2 history',
      'pii': false,
      'scd_type': 2
    }
  )
}}

/*
  dim_customers
  =============
  Slowly Changing Dimension Type 2 for customer master data.
  Grain: one row per customer per version (loyalty_tier, segment changes trigger new version).
  No PII — only hashed identifiers and safe bucketed attributes.
*/

with

silver_customers as (
    select * from {{ ref('silver_customers') }}
),

-- ── SNAPSHOT SOURCE (SCD2 via dbt snapshots) ─────────────
customer_snapshots as (
    select
        customer_id,
        loyalty_tier,
        segment,
        age_bracket,
        gender,
        country,
        state,
        postal_code_prefix,
        birth_year,
        registration_date,
        registration_year,
        registration_month,
        total_orders,
        ltv_usd,
        is_email_verified,
        preferred_device,
        email_hash,
        phone_hash,
        _valid_from,
        _valid_to,
        _is_current,
        _record_hash
    from {{ ref('snapshot_customers') }}
),

-- ── ENRICH WITH DERIVED ATTRIBUTES ───────────────────────
enriched as (
    select
        -- ── SURROGATE KEY ──
        {{ dbt_utils.generate_surrogate_key([
            'cs.customer_id', 'cs._valid_from'
        ]) }}                                               as customer_sk,

        -- ── NATURAL KEY ──
        cs.customer_id,

        -- ── HASHED IDENTIFIERS (no PII) ──
        cs.email_hash,
        cs.phone_hash,

        -- ── SAFE DEMOGRAPHICS ──
        cs.age_bracket,
        cs.birth_year,
        cs.gender,
        cs.country,
        cs.state,
        cs.postal_code_prefix,

        -- ── LOYALTY & SEGMENTATION ──
        cs.loyalty_tier,
        cs.segment,

        -- ── LOYALTY TIER NUMERIC (for sorting/ML) ──
        case cs.loyalty_tier
            when 'bronze'   then 1
            when 'silver'   then 2
            when 'gold'     then 3
            when 'platinum' then 4
            else                 0
        end                                                 as loyalty_tier_rank,

        -- ── BEHAVIORAL ──
        cs.total_orders,
        cs.ltv_usd,
        cs.is_email_verified,
        cs.preferred_device,

        -- ── ACQUISITION ──
        cs.registration_date,
        cs.registration_year,
        cs.registration_month,
        datediff('day', cs.registration_date, current_date) as customer_age_days,
        case
            when datediff('day', cs.registration_date, current_date) <=  30 then 'new'
            when datediff('day', cs.registration_date, current_date) <= 180 then 'recent'
            when datediff('day', cs.registration_date, current_date) <= 365 then 'established'
            else                                                                    'veteran'
        end                                                 as tenure_band,

        -- ── LTV BAND ──
        case
            when cs.ltv_usd <    0 then 'inactive'
            when cs.ltv_usd <  100 then 'low'
            when cs.ltv_usd <  500 then 'medium'
            when cs.ltv_usd < 2000 then 'high'
            else                        'vip'
        end                                                 as ltv_band,

        -- ── SCD2 METADATA ──
        cs._valid_from,
        cs._valid_to,
        cs._is_current,
        cs._record_hash,

        -- ── DBT METADATA ──
        current_timestamp                                 as _dbt_updated_at

    from customer_snapshots cs
),

final as (
    select * from enriched
)

select * from final
