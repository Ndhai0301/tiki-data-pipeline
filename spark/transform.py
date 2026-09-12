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
    return out.dropDuplicates(["product_id", "dt", "hour", "category"])
