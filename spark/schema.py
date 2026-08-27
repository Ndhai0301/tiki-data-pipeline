"""
Schema tuong minh cho Bronze listing (Tiki) - KHONG dung inferSchema.

Vi sao khai bao tuong minh thay vi de Spark tu doan:
- inferSchema quet toan bo du lieu 1 lan chi de doan kieu, roi quet lai
  lan 2 de doc that -> cham gap doi.
- Kieu suy ra phu thuoc vao du lieu ngay hom do. VD discount_rate hom nay
  toan so nguyen -> Spark doan int; hom sau Tiki tra ve so thap phan ->
  doan thanh double. Doc gop nhieu partition (nhieu ngay) cung luc se vo
  schema vi 2 partition co kieu suy ra khac nhau cho cung 1 cot.
- Khai bao tuong minh: kieu co dinh, on dinh giua cac lan chay. Neu Tiki
  doi field (doi kieu, xoa field), job bao loi NGAY tai buoc doc, thay vi
  am tham doc sai roi phat hien qua dashboard sai so vai tuan sau.

Chi khai bao cac field thuc su dung toi transform (xem transform.py),
khong khai bao het ~54 field tho cua response Tiki (xem docs/tiki_api_schema.md
neu can doi chieu full schema).
"""

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

# Schema cua 1 dong trong bronze/dt=*/hour=*/category=*/listings.jsonl.gz
# (truoc khi them cot dt/hour/category suy tu duong dan file).
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
