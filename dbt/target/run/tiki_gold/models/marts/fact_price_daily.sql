
  
    

  create  table "tiki"."gold"."fact_price_daily__dbt_tmp"
  
  
    as
  
  (
    select distinct on (product_id, dt, hour)
    product_id,
    dt,
    hour,
    price,
    list_price,
    discount,
    discount_rate,
    rating_average,
    review_count,
    quantity_sold,
    inventory_status,
    badge_count,
    page,
    crawled_at
from "tiki"."staging"."stg_listings"
order by product_id, dt, hour, category_slug
  );
  