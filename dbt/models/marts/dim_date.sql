{{ config(materialized='table') }}

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
