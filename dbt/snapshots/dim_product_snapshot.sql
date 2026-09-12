{% snapshot dim_product_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='product_id',
        strategy='check',
        check_cols=['name', 'brand_id', 'brand_name', 'seller_id', 'seller_name',
                     'category_id', 'is_authentic', 'thumbnail_url',
                     'url', 'url_key'],
    )
}}

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
order by product_id, category_slug, crawled_at desc, dt desc

{% endsnapshot %}
