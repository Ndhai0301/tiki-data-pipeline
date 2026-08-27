
  create view "tiki"."staging"."stg_listings__dbt_tmp"
    
    
  as (
    select
    product_id::bigint                as product_id,
    sku,
    name,
    url_key,
    url,
    price::bigint                     as price,
    list_price::bigint                as list_price,
    discount::bigint                  as discount,
    discount_rate::int                as discount_rate,
    rating_average::double precision  as rating_average,
    review_count::int                 as review_count,
    quantity_sold::int                as quantity_sold,
    brand_id::int                     as brand_id,
    brand_name,
    seller_id::bigint                 as seller_id,
    seller_name,
    category_id::bigint               as category_id,
    primary_category_name             as category_name,
    inventory_status,
    is_authentic::boolean             as is_authentic,
    thumbnail_url,
    badge_count::int                  as badge_count,
    page::int                         as page,
    crawled_at::timestamp             as crawled_at,
    dt::date                          as dt,
    hour::int                         as hour,
    category                          as category_slug

from "tiki"."raw"."listings"
  );