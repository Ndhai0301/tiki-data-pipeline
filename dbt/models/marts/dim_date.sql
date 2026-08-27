{{ config(materialized='table') }}

-- Date spine dung san (2020-2035), khong sinh tu du lieu crawl (xem
-- docs/erd.md, muc "Viec can lam khi hien thuc hoa" #2). generate_series
-- cua Postgres - khong can dbt_utils.date_spine.
-- dow theo ISODOW: 1 = Thu Hai ... 7 = Chu Nhat.
with spine as (
    select generate_series('2020-01-01'::date, '2035-12-31'::date, interval '1 day')::date as ngay
)
select
    to_char(ngay, 'YYYYMMDD')::int as date_key,
    ngay,
    extract(week from ngay)::int   as tuan,
    extract(month from ngay)::int  as thang,
    extract(isodow from ngay)::int as dow
from spine
