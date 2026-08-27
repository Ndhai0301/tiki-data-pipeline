
      update "tiki"."gold"."dim_product_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "dim_product_snapshot__dbt_tmp000656739055" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "tiki"."gold"."dim_product_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "tiki"."gold"."dim_product_snapshot".dbt_valid_to is null;
      


    insert into "tiki"."gold"."dim_product_snapshot" ("product_id", "sku", "name", "url_key", "url", "brand_id", "brand_name", "seller_id", "seller_name", "category_id", "category_name", "is_authentic", "thumbnail_url", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."product_id",DBT_INTERNAL_SOURCE."sku",DBT_INTERNAL_SOURCE."name",DBT_INTERNAL_SOURCE."url_key",DBT_INTERNAL_SOURCE."url",DBT_INTERNAL_SOURCE."brand_id",DBT_INTERNAL_SOURCE."brand_name",DBT_INTERNAL_SOURCE."seller_id",DBT_INTERNAL_SOURCE."seller_name",DBT_INTERNAL_SOURCE."category_id",DBT_INTERNAL_SOURCE."category_name",DBT_INTERNAL_SOURCE."is_authentic",DBT_INTERNAL_SOURCE."thumbnail_url",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "dim_product_snapshot__dbt_tmp000656739055" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  