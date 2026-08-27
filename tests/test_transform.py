"""
Pytest cho spark/transform.py::bronze_to_silver bang du lieu gia lap
(khong dung du lieu Tiki that) - test rieng ham bien doi, khong dong den
Bronze/Silver that tren dia.

Chay:
    export JAVA_HOME=~/spark/jdk-11.0.1 SPARK_HOME=~/spark/spark-3.5.8-bin-hadoop3
    pytest tests/ -v
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent.parent / "spark"))
from schema import LISTING_SCHEMA  # noqa: E402
from transform import bronze_to_silver  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .appName("test_transform")
        .master("local[1]")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _fake_row(**overrides) -> dict:
    """1 dong Bronze gia lap, du field mac dinh de khong phai lap lai o moi test."""
    base = dict(
        id=1,
        sku="SKU-1",
        name="San pham mau",
        url_key="san-pham-mau",
        brand_name="BrandX",
        price=100_000,
        list_price=0,
        original_price=120_000,
        discount=20_000,
        discount_rate=17,
        rating_average=4.5,
        review_count=10,
        quantity_sold={"text": "Da ban 5", "value": 5},
        seller_id=1,
        inventory_status="available",
        thumbnail_url="https://example.com/thumb.jpg",
        badges_new=[{"code": "authentic_brand", "text": None}],
        visible_impression_info={
            "amplitude": {
                "is_authentic": 1,
                "primary_category_name": "Danh muc mau",
                "category_l1_name": "L1",
                "category_l2_name": "L2",
                "seller_type": "OFFICIAL_STORE",
            }
        },
        _category_id=1846,
        _page=1,
        _crawled_at="2026-08-22T06:00:02+00:00",
        dt="2026-08-22",
        hour="06",
        category="laptop",
    )
    base.update(overrides)
    return base


def _make_df(spark, rows: list[dict]):
    # dt/hour/category khong nam trong LISTING_SCHEMA (o job chinh duoc suy
    # tu duong dan file), nen ghep them 3 cot string vao schema khi test.
    from pyspark.sql import types as T

    partition_cols = ("dt", "hour", "category")
    full_schema = T.StructType(
        list(LISTING_SCHEMA.fields) + [T.StructField(c, T.StringType()) for c in partition_cols]
    )
    return spark.createDataFrame(rows, schema=full_schema)


def test_list_price_fallback_to_original_price(spark):
    """list_price=0 (dung o listing API that) -> phai lay original_price."""
    df = _make_df(spark, [_fake_row(list_price=0, original_price=999_000)])
    result = bronze_to_silver(df).collect()
    assert result[0]["list_price"] == 999_000


def test_list_price_giu_nguyen_neu_khac_0(spark):
    df = _make_df(spark, [_fake_row(list_price=150_000, original_price=999_000)])
    result = bronze_to_silver(df).collect()
    assert result[0]["list_price"] == 150_000


def test_is_authentic_true_khi_co_badge(spark):
    df = _make_df(spark, [_fake_row(badges_new=[{"code": "authentic_brand", "text": None}])])
    result = bronze_to_silver(df).collect()
    assert result[0]["is_authentic"] is True


def test_is_authentic_false_khi_khong_co_badge(spark):
    df = _make_df(spark, [_fake_row(badges_new=[{"code": "delivery_info_badge", "text": "Giao nhanh"}])])
    result = bronze_to_silver(df).collect()
    assert result[0]["is_authentic"] is False


def test_is_authentic_false_khi_badges_new_null(spark):
    """badges_new=None (co the xay ra thuc te) khong duoc lam crash job."""
    df = _make_df(spark, [_fake_row(badges_new=None)])
    result = bronze_to_silver(df).collect()
    assert result[0]["is_authentic"] is False
    assert result[0]["badge_count"] == 0


def test_url_duoc_ghep_dung(spark):
    df = _make_df(spark, [_fake_row(id=279225805, url_key="apple-macbook-air")])
    result = bronze_to_silver(df).collect()
    assert result[0]["url"] == "https://tiki.vn/apple-macbook-air-p279225805.html"


def test_dedupe_trung_product_id_cung_grain(spark):
    """2 dong cung product_id + cung (dt, hour, category) -> chi con 1 dong."""
    rows = [
        _fake_row(id=1, price=100_000),
        _fake_row(id=1, price=100_000),  # trung hoan toan, vd doc lai do retry
    ]
    df = _make_df(spark, rows)
    result = bronze_to_silver(df).collect()
    assert len(result) == 1


def test_khong_dedupe_khac_grain(spark):
    """Cung product_id nhung khac hour (2 lan crawl khac nhau) -> giu ca 2."""
    rows = [
        _fake_row(id=1, hour="06"),
        _fake_row(id=1, hour="12"),
    ]
    df = _make_df(spark, rows)
    result = bronze_to_silver(df).collect()
    assert len(result) == 2


def test_cac_cot_suy_tu_field_noi_bo(spark):
    """category_id/page/crawled_at phai lay dung tu _category_id/_page/_crawled_at."""
    df = _make_df(spark, [_fake_row(_category_id=1846, _page=3, _crawled_at="2026-08-22T06:00:02+00:00")])
    result = bronze_to_silver(df).collect()
    assert result[0]["category_id"] == 1846
    assert result[0]["page"] == 3
    assert str(result[0]["crawled_at"]) == "2026-08-22T06:00:02+00:00"


def test_brand_id_seller_name_luon_null(spark):
    """listing API khong co brand.id/seller_name (chi co o detail API) -> luon None."""
    df = _make_df(spark, [_fake_row()])
    result = bronze_to_silver(df).collect()
    assert result[0]["brand_id"] is None
    assert result[0]["seller_name"] is None


def test_primary_category_name_lay_tu_amplitude(spark):
    df = _make_df(spark, [_fake_row(
        visible_impression_info={"amplitude": {
            "is_authentic": 1, "primary_category_name": "Macbook",
            "category_l1_name": "L1", "category_l2_name": "L2", "seller_type": "X",
        }}
    )])
    result = bronze_to_silver(df).collect()
    assert result[0]["primary_category_name"] == "Macbook"
