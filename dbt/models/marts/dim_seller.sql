{{ config(materialized='table') }}

-- Snowflake dim tach tu stg_listings (xem docs/erd.md). Tam thoi giu toi
-- gian (chua co logo/seller_address) - xem ly do o docs/erd.md muc
-- "dim_seller giu toi gian".
with seller as (
    select distinct on (seller_id)
        seller_id,
        seller_name
    from {{ ref('stg_listings') }}
    where seller_id is not null
    order by seller_id, crawled_at desc
)
select
    {{ surrogate_key('seller_id') }} as seller_key,
    seller_id,
    seller_name
from seller
