{{ config(materialized='table') }}

with brand as (
    select distinct on (brand_id)
        brand_id,
        brand_name
    from {{ ref('stg_listings') }}
    where brand_id is not null
    order by brand_id, crawled_at desc
)
select
    {{ surrogate_key('brand_id') }} as brand_key,
    brand_id,
    brand_name
from brand
