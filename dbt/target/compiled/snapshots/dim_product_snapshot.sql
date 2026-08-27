



-- Moi lan snapshot chay: lay ban ghi MOI NHAT cua tung product_id trong lan
-- crawl vua nap (co the co nhieu dong trung product_id do trung category).
--
-- Tie-break bang category_slug (khong chi crawled_at): 1 san pham co the
-- xuat hien o 2 category CUNG luc, CUNG crawled_at (vd sach nam ca trong
-- "nha-sach-tiki" va "sach-tieng-viet") - neu khong co tie-break on dinh,
-- Postgres chon dong nao trong 2 dong tie la KHONG XAC DINH, khien
-- category_id doi qua doi lai giua cac lan chay va lam SCD2 tuong nham
-- la san pham doi category that (da gap bug nay that khi test lai).
select distinct on (product_id)
    product_id,
    sku,
    name,
    url_key,
    url,
    brand_id,
    brand_name,
    seller_id,
    seller_name,
    category_id,
    category_name,
    is_authentic,
    thumbnail_url
from "tiki"."staging"."stg_listings"
order by product_id, crawled_at desc, category_slug

