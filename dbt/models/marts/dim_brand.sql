{{ config(materialized='table') }}

-- Snowflake dim tach tu stg_listings (xem docs/erd.md). distinct on +
-- crawled_at desc: 1 brand_id chi giu 1 brand_name (ban ghi moi nhat),
-- tranh brand_key bi trung neu ten brand tung doi.
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
