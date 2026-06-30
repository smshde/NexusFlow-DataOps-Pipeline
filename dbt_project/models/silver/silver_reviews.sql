{{ config(materialized='table', schema='silver', tags=['silver', 'cleaned']) }}

with source as (
    select * from {{ source('silver_lake', 'reviews') }}
)

select
    review_id,
    customer_id,
    customer_email_hash,
    product_sku,
    product_name,
    cast(rating as integer)        as rating,
    title,
    body,
    cast(review_date as date)      as review_date,
    verified_purchase,
    cast(helpful_votes as integer) as helpful_votes,
    cast(images_count as integer)  as images_count,
    sentiment
from source
where review_id is not null
