{{
  config(
    materialized = 'view',
    schema       = 'silver',
    tags         = ['silver', 'customers']
  )
}}

-- silver_customers
-- Thin view over Spark's silver/customers/ Parquet output (via Redshift
-- Spectrum external schema silver_lake → Glue nexusflow_dev_silver).
-- Spark already does PII masking, postal/DOB truncation, and SCD2
-- metadata — dbt does not re-derive silver from bronze.

select * from {{ source('silver_lake', 'customers') }}
