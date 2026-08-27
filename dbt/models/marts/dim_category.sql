{{ config(materialized='table') }}

-- Snowflake dim tach tu stg_listings (xem docs/erd.md).
with category as (
    select distinct on (category_id)
        category_id,
        category_name
    from {{ ref('stg_listings') }}
    where category_id is not null
    order by category_id, crawled_at desc
)
select
    {{ surrogate_key('category_id') }} as category_key,
    category_id,
    category_name
from category
