{{ config(materialized='table') }}

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
