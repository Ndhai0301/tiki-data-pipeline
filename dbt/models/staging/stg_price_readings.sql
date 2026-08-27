-- Dedupe stg_listings ve grain (product_id, dt, hour): 1 san pham co the
-- xuat hien o nhieu category cung 1 lan crawl (vd sach nam ca
-- "nha-sach-tiki" va "sach-tieng-viet") - lay 1 dong dai dien on dinh bang
-- category_slug de tranh trung PK khi fact_price_daily/fact_price_change
-- cung doc tu day (truoc day logic nay lap lai rieng trong tung fact).
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
from {{ ref('stg_listings') }}
order by product_id, dt, hour, category_slug
