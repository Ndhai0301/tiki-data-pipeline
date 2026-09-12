{{ config(materialized='table') }}

with reading as (
    select * from {{ ref('stg_price_readings') }}
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
product as (
    select
        *,
        min(valid_from) over (partition by product_id) as first_valid_from
    from {{ ref('dim_product') }}
)

select
    dd.date_key,
    dp.product_key,
    with_prev.crawled_at  as changed_at,
    with_prev.price_truoc as price_cu,
    with_prev.price       as price_moi
from with_prev
join {{ ref('dim_date') }} as dd
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
