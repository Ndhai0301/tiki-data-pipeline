
  
    

  create  table "tiki"."gold"."fact_price_change__dbt_tmp"
  
  
    as
  
  (
    

-- Fact "transactional" bo sung fact_price_daily (xem docs/erd.md): chi
-- ghi 1 dong khi gia THUC SU doi so voi lan crawl truoc do cua CUNG san
-- pham (LAG theo dt/hour), thay vi phai tu diff giua cac snapshot nhu
-- cach lam thu cong truoc day.
--
-- Lan crawl DAU TIEN cua 1 product_id khong tinh la "doi gia" (khong co
-- gia truoc de so) - loai bo bang price_truoc is not null.
--
-- date_key: join them dim_date de co surrogate key nhu fact_price_daily,
-- thay vi chi co changed_at (timestamp). Muc dich: BI tool (Metabase,
-- Power BI...) noi truc tiep date_key <-> dim_date.date_key duoc, khong
-- phai tu tao cot tinh toan rieng (truncate changed_at ve date) moi lan
-- dung cong cu khac.
with reading as (
    select * from "tiki"."staging"."stg_price_readings"
),
with_prev as (
    select
        product_id,
        dt,
        hour,
        crawled_at,
        price,
        lag(price) over (partition by product_id order by dt, hour) as price_truoc
    from reading
),
-- Xem giai thich first_valid_from o fact_price_daily.sql - cung 1 van de:
-- reading cu hon moc dbt snapshot bat dau theo doi can gan vao phien ban
-- som nhat da biet, khong bi INNER JOIN loai bo.
product as (
    select
        *,
        min(valid_from) over (partition by product_id) as first_valid_from
    from "tiki"."gold"."dim_product"
)

select
    dd.date_key,
    dp.product_key,
    with_prev.crawled_at  as changed_at,
    with_prev.price_truoc as price_cu,
    with_prev.price       as price_moi
from with_prev
join "tiki"."gold"."dim_date" as dd
    on dd.ngay = with_prev.dt
join product as dp
    on dp.product_id = with_prev.product_id
    and with_prev.dt < coalesce(dp.valid_to, 'infinity'::date)
    and with_prev.dt >= case
        when dp.valid_from = dp.first_valid_from then '-infinity'::date
        else dp.valid_from
    end
where with_prev.price_truoc is not null
  and with_prev.price_truoc != with_prev.price
  );
  