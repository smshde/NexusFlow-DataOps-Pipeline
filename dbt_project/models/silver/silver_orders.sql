{{
  config(
    materialized = 'view',
    schema       = 'silver',
    tags         = ['silver', 'orders']
  )
}}

-- silver_orders
-- Thin view over Spark's silver/orders/ Parquet output (via Redshift
-- Spectrum external schema silver_lake → Glue nexusflow_dev_silver).
-- Spark already dedups, types, and derives order_year/month/day/etc —
-- dbt does not re-derive silver from bronze. See bronze_to_silver.py.

select * from {{ source('silver_lake', 'orders') }}
