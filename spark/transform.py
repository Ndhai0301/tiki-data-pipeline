"""
Ham bien doi Bronze -> Silver, thuan tuy (khong doc/ghi file, khong tao
SparkSession). Nhan mot DataFrame dung LISTING_SCHEMA (schema.py) da co san
3 cot dt/hour/category (suy tu duong dan file o job chinh), tra ve DataFrame
Silver.

Tach rieng khoi tiki_bronze_to_silver.py (phan I/O) de test doc lap bang
du lieu gia - xem tests/test_transform.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def bronze_to_silver(df: DataFrame) -> DataFrame:
    out = (
        df
        .withColumn("product_id", F.col("id"))
        .withColumn(
            "url",
            F.when(
                F.col("url_key").isNotNull(),
                F.concat(F.lit("https://tiki.vn/"), F.col("url_key"), F.lit("-p"), F.col("id"), F.lit(".html")),
            ),
        )
        .withColumn(
            # list_price o listing API luon = 0 (xem docs/tiki_api_schema.md) ->
            # dung original_price thay the, giong logic normalize_item() ben Python.
            "list_price",
            F.coalesce(
                F.when(F.col("list_price") != 0, F.col("list_price")),
                F.col("original_price"),
                F.lit(0),
            ),
        )
        .withColumn("quantity_sold", F.col("quantity_sold.value"))
        .withColumn(
            "is_authentic",
            F.coalesce(
                F.exists(F.coalesce(F.col("badges_new"), F.array()), lambda b: b["code"] == F.lit("authentic_brand")),
                F.lit(False),
            ),
        )
        .withColumn("primary_category_name", F.col("visible_impression_info.amplitude.primary_category_name"))
        .withColumn("badge_count", F.size(F.coalesce(F.col("badges_new"), F.array())))
        .withColumn("category_id", F.col("_category_id"))
        .withColumn("page", F.col("_page"))
        .withColumn("crawled_at", F.col("_crawled_at"))
        # brand_id/seller_name khong co trong listing API (chi co o detail API,
        # xem docs/tiki_api_schema.md) - job Spark nay khong goi detail API
        # (giong cron hien tai khong dung --detail), nen luon NULL. Van khai
        # bao cot de khop dung 24-cot voi SILVER_SCHEMA ben tiki_crawl.py,
        # neu khong stg_listings.sql se loi vi thieu cot.
        .withColumn("brand_id", F.lit(None).cast("int"))
        .withColumn("seller_name", F.lit(None).cast("string"))
        .select(
            "product_id",
            "sku",
            "name",
            "url_key",
            "url",
            "price",
            "list_price",
            "discount",
            "discount_rate",
            "rating_average",
            "review_count",
            "quantity_sold",
            "brand_id",
            "brand_name",
            "seller_id",
            "seller_name",
            "category_id",
            "primary_category_name",
            "inventory_status",
            "is_authentic",
            "thumbnail_url",
            "badge_count",
            "page",
            "crawled_at",
            "dt",
            "hour",
            "category",
        )
    )
    # Dedupe theo grain (product_id, dt, hour, category): trung xay ra khi
    # cung 1 trang duoc doc lai (retry mang) hoac cung san pham xuat hien
    # o 2 trang cua CUNG 1 category trong 1 lan crawl.
    return out.dropDuplicates(["product_id", "dt", "hour", "category"])
