{{ config(materialized='table') }}

with snap as (
    select * from {{ ref('dim_product_snapshot') }}
)

select
    {{ surrogate_key('snap.dbt_scd_id') }} as product_key,
    snap.product_id,
    snap.name,
    brand.brand_key,
    seller.seller_key,
    category.category_key,
    snap.url_key,
    snap.url,
    snap.is_authentic,
    snap.thumbnail_url,
    snap.dbt_valid_from::date as valid_from,
    snap.dbt_valid_to::date   as valid_to,
    (snap.dbt_valid_to is null) as is_current
from snap
left join {{ ref('dim_brand') }}    as brand    on brand.brand_id       = snap.brand_id
left join {{ ref('dim_seller') }}   as seller   on seller.seller_id     = snap.seller_id
left join {{ ref('dim_category') }} as category on category.category_id = snap.category_id
