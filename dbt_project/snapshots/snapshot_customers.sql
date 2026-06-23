{% snapshot snapshot_customers %}

{{
    config(
      target_schema = 'snapshots',
      unique_key    = 'customer_id',
      strategy      = 'check',
      check_cols    = ['loyalty_tier', 'segment', 'total_orders'],
      invalidate_hard_deletes = true
    )
}}

select * from {{ ref('silver_customers') }}

{% endsnapshot %}
