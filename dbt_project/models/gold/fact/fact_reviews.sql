{{ config(materialized='table', tags=['gold', 'fact']) }}

with reviews as (
    select * from {{ ref('silver_reviews') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['review_id']) }} as review_sk,
    review_id,
    customer_id,
    product_sku,
    rating,
    verified_purchase,
    helpful_votes,
    images_count,
    sentiment,
    review_date
from reviews
