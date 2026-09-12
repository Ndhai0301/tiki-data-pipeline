from pyspark.sql import types as T

QUANTITY_SOLD_SCHEMA = T.StructType([
    T.StructField("text", T.StringType()),
    T.StructField("value", T.LongType()),
])

BADGE_SCHEMA = T.StructType([
    T.StructField("code", T.StringType()),
    T.StructField("text", T.StringType()),
])

AMPLITUDE_SCHEMA = T.StructType([
    T.StructField("is_authentic", T.IntegerType()),
    T.StructField("primary_category_name", T.StringType()),
    T.StructField("category_l1_name", T.StringType()),
    T.StructField("category_l2_name", T.StringType()),
    T.StructField("seller_type", T.StringType()),
])

VISIBLE_IMPRESSION_INFO_SCHEMA = T.StructType([
    T.StructField("amplitude", AMPLITUDE_SCHEMA),
])

LISTING_SCHEMA = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("sku", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("url_key", T.StringType()),
    T.StructField("brand_name", T.StringType()),
    T.StructField("price", T.LongType()),
    T.StructField("list_price", T.LongType()),
    T.StructField("original_price", T.LongType()),
    T.StructField("discount", T.LongType()),
    T.StructField("discount_rate", T.IntegerType()),
    T.StructField("rating_average", T.DoubleType()),
    T.StructField("review_count", T.IntegerType()),
    T.StructField("quantity_sold", QUANTITY_SOLD_SCHEMA),
    T.StructField("seller_id", T.LongType()),
    T.StructField("inventory_status", T.StringType()),
    T.StructField("thumbnail_url", T.StringType()),
    T.StructField("badges_new", T.ArrayType(BADGE_SCHEMA)),
    T.StructField("visible_impression_info", VISIBLE_IMPRESSION_INFO_SCHEMA),
    T.StructField("_category_id", T.LongType()),
    T.StructField("_page", T.IntegerType()),
    T.StructField("_crawled_at", T.StringType()),
])
