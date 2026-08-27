

-- Snowflake dim tach tu stg_listings (xem docs/erd.md).
with category as (
    select distinct on (category_id)
        category_id,
        category_name
    from "tiki"."staging"."stg_listings"
    where category_id is not null
    order by category_id, crawled_at desc
)
select
    abs(('x' || substr(md5(category_id::text), 1, 16))::bit(64)::bigint) as category_key,
    category_id,
    category_name
from category