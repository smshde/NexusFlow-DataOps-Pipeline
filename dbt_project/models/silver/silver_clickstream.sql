{{
  config(
    materialized = 'table',
    schema       = 'silver',
    tags         = ['silver', 'clickstream']
  )
}}

-- silver_clickstream
-- Thin view over Spark's silver/clickstream/ Parquet output (via
-- Redshift Spectrum external schema silver_lake → Glue
-- nexusflow_dev_silver). Spark already dedups, types, and derives
-- event_year/month/day/hour — dbt does not re-derive silver from bronze.

select * from {{ source('silver_lake', 'clickstream') }}
