
  
    

  create  table "tiki"."gold"."dim_product__dbt_tmp"
  
  
    as
  
  (
    

-- SCD2 (xem docs/erd.md). Nguon la dbt snapshot dim_product_snapshot
-- (snapshots/dim_product_snapshot.sql) - dbt tu sinh dbt_valid_from/
-- dbt_valid_to/dbt_scd_id, dung lam valid_from/valid_to/product_key thay
-- vi tu code UPSERT tay.
--
-- product_key hash tu dbt_scd_id (khong phai product_id): moi PHIEN BAN
-- (moi lan brand/seller/category/ten doi) can 1 key rieng, de
-- fact_price_daily/fact_price_change join dung dong lich su tai thoi
-- diem phat sinh gia, khong bi gop nham vao dong hien tai.
with snap as (
    select * from "tiki"."gold"."dim_product_snapshot"
)

select
    abs(('x' || substr(md5(snap.dbt_scd_id::text), 1, 16))::bit(64)::bigint) as product_key,
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
left join "tiki"."gold"."dim_brand"    as brand    on brand.brand_id       = snap.brand_id
left join "tiki"."gold"."dim_seller"   as seller   on seller.seller_id     = snap.seller_id
left join "tiki"."gold"."dim_category" as category on category.category_id = snap.category_id
  );
  