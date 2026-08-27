{#
    SCD Type 2 cho dim_product (theo docs/erd.md). dbt snapshot tu sinh
    dbt_valid_from/dbt_valid_to/dbt_scd_id - dung lam surrogate key va
    valid_from/valid_to thay vi tu code tay.

    strategy=check: so sanh check_cols giua lan chay nay va ban ghi hien
    tai (dbt_valid_to IS NULL) trong bang snapshot - neu khac, dong ban
    ghi cu (set dbt_valid_to) va mo ban ghi moi. Dung "check" thay vi
    "timestamp" vi Silver khong co truong updated_at rieng cua Tiki, chi
    co crawled_at (thoi diem CRAWL, khong phai thoi diem THAY DOI that).
#}

{% snapshot dim_product_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='product_id',
        strategy='check',
        check_cols=['name', 'brand_id', 'brand_name', 'seller_id', 'seller_name',
                     'category_id', 'category_name', 'is_authentic', 'thumbnail_url',
                     'url', 'url_key'],
    )
}}

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
from {{ ref('stg_listings') }}
order by product_id, crawled_at desc, category_slug

{% endsnapshot %}
