



-- Moi lan snapshot chay: lay ban ghi MOI NHAT cua tung product_id trong lan
-- crawl vua nap (co the co nhieu dong trung product_id do trung category).
--
-- category_name KHONG nam trong check_cols (va cung khong duoc dim_product
-- doc - dim_product lay ten category qua join dim_category theo category_id):
-- category_name = primary_category_name tu Tiki, field nay NULL ~57% so
-- lan tra ve (kiem chung thuc te tren stg_listings), doi qua lai NULL/co
-- gia tri MOI LAN CRAWL du san pham khong doi gi that - dua vao check_cols
-- se tao version rac cho hau het san pham (da do duoc: 86% tong so lan
-- doi version la do cot nay).
--
-- Tie-break: category_slug LA TIEU CHI CHINH (khong phai crawled_at nhu
-- truoc). 1 san pham co the xuat hien o 2 category CUNG luc (vd sach nam
-- ca trong "nha-sach-tiki" va "sach-tieng-viet"). crawled_at cua 2
-- category do lech nhau vai giay/phut moi ngay (crawl tuan tu, khong dong
-- thoi) nen neu dat crawled_at truoc category_slug trong order by, no gan
-- nhu luon quyet dinh (vi hiem khi trung tuyet doi), khien category
-- "thang" doi tuy ngay nao crawl truoc/sau - SCD2 tuong nham san pham doi
-- category that (da do duoc: 13% so lan doi version la do cot nay). Dat
-- category_slug LEN TRUOC de co dinh category thang, khong phu thuoc thu
-- tu crawl trong ngay.
--
-- crawled_at desc VAN GIU LAI, dat SAU category_slug: dung de chon ban ghi
-- MOI NHAT trong nhieu dong CUNG category_slug (1 san pham xuat hien lai o
-- cung 1 category qua nhieu ngay crawl khac nhau) - thieu no, DISTINCT ON
-- se khong co gi phan dinh giua cac dong cung category_slug, Postgres chon
-- tuy y (da gap that: dbt snapshot van insert them dong moi ngay ca khi
-- du lieu nguon khong doi, vi lan chay nao cung ra ket qua khac nhau).
--
-- dt desc: tie-break cuoi cung. Du lieu backfill/debug de lai vai cap dong
-- CUNG category_slug VA CUNG crawled_at (chi khac dt) - vi du that: product
-- 48576455 co dt=2026-08-28 va dt=2026-09-01 cung crawled_at 09:12:36 (di
-- san tu cac lan chay bu thu cong khi debug DAG). Khong co dt desc, 2 dong
-- do lai tie tiep, Postgres chon tuy y.
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
order by product_id, category_slug, crawled_at desc, dt desc
