{{ config(materialized='table') }}

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
