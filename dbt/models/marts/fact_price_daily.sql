{{ config(materialized='table') }}

-- Grain: (date_key, hour, product_key) - 1 dong = gia do duoc 1 lan crawl.
-- Luu y: erd.md khong liet ke cot "hour" trong fact_price_daily, nhung du
-- an crawl 4 lan/ngay (00/06/12/18, xem docs/storage.md) chinh la de so
-- bien dong gia TRONG ngay - bo hour se mat dung muc tieu do, nen giu lai
-- o day (uu tien storage.md hon phan liet ke cot cua erd.md).
--
-- Join dim_product theo khoang valid_from/valid_to (khong chi product_id)
-- de gan dung phien ban SCD2 dang hieu luc tai thoi diem crawl (dt), thay
-- vi luon gan vao dong is_current moi nhat.
--
-- first_valid_from: dbt snapshot chi bat dau theo doi tu lan chay dau
-- tien (~2026-08-24), nhung Bronze/Silver co du lieu crawl truoc do (vd
-- 2026-08-21). Neu chi so "reading.dt >= valid_from" nhu binh thuong, cac
-- reading cu hon moc theo doi se khong khop dong SCD2 nao va bi INNER
-- JOIN loai bo oan. Danh dau phien ban SOM NHAT cua tung product_id va
-- coi valid_from cua no la -infinity khi join, de reading cu duoc gan vao
-- phien ban som nhat da biet (khong co thong tin nao tot hon).
with reading as (
    select * from {{ ref('stg_price_readings') }}
),
product as (
    select
        *,
        min(valid_from) over (partition by product_id) as first_valid_from
    from {{ ref('dim_product') }}
)

select
    dd.date_key,
    reading.hour,
    dp.product_key,
    reading.price,
    reading.list_price,
    reading.discount,
    reading.discount_rate,
    reading.rating_average,
    reading.review_count,
    reading.quantity_sold,
    reading.inventory_status,
    reading.badge_count,
    reading.page
from reading
join {{ ref('dim_date') }} as dd
    on dd.ngay = reading.dt
join product as dp
    on dp.product_id = reading.product_id
    and reading.dt < coalesce(dp.valid_to, 'infinity'::date)
    and reading.dt >= case
        when dp.valid_from = dp.first_valid_from then '-infinity'::date
        else dp.valid_from
    end
