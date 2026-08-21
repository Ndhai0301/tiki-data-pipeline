#!/usr/bin/env python3
"""
tiki_crawl.py - Crawl san pham Tiki -> Bronze (raw JSONL.gz) + Silver (Parquet).

Dung curl_cffi de gia lap TLS fingerprint cua Chrome - bat buoc, vi requests
thuong bi WAF cua Tiki chan 403 (da kiem chung: curl_cffi impersonate=chrome
vuot duoc 403 ca khi khong warm-up, requests thuong thi luon bi chan).

Cai dat:
    pip install curl_cffi pandas pyarrow

Chay:
    python3 tiki_crawl.py --categories laptop --pages 1 --verbose
    python3 tiki_crawl.py --categories laptop,dien-thoai --pages 10 --rps 1.0
    python3 tiki_crawl.py --categories laptop --pages 5 --detail --detail-limit 100
    python3 tiki_crawl.py --categories all --pages 20 --rps 1.0
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

try:
    from curl_cffi import requests as curl_requests
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Thieu curl_cffi. Cai bang: pip install curl_cffi") from exc

# --------------------------------------------------------------------------- #
# Cau hinh
# --------------------------------------------------------------------------- #

BASE = "https://tiki.vn"
LISTING_URLS = [
    f"{BASE}/api/personalish/v1/blocks/listings",
    f"{BASE}/api/v2/products",
]
DETAIL_URL = f"{BASE}/api/v2/products/{{product_id}}"

CATEGORY_ALIASES: dict[str, int] = {
    "laptop": 1846,
    "dien-thoai": 1789,
    "may-tinh-bang": 1794,
    "tai-nghe": 8215,
    "sach-tieng-viet": 316,
    "do-choi": 2549,
    # 26 category cap 1 tren menu chinh cua tiki.vn (do ngay 2026-08-21)
    "thoi-trang-nam": 915,
    "thoi-trang-nu": 931,
    "tui-vi-nu": 976,
    "lam-dep-suc-khoe": 1520,
    "giay-dep-nam": 1686,
    "giay-dep-nu": 1703,
    "may-anh": 1801,
    "thiet-bi-kts-phu-kien-so": 1815,
    "dien-gia-dung": 1882,
    "nha-cua-doi-song": 1883,
    "the-thao-da-ngoai": 1975,
    "dien-tu-dien-lanh": 4221,
    "bach-hoa-online": 4384,
    "balo-va-vali": 6000,
    "nha-sach-tiki": 8322,
    "dong-ho-va-trang-suc": 8371,
    "o-to-xe-may-xe-dap": 8594,
    "voucher-dich-vu": 11312,
    "cham-soc-nha-cua": 15078,
    "cross-border-hang-quoc-te": 17166,
    "phu-kien-thoi-trang": 27498,
    "tui-thoi-trang-nam": 27616,
    "ngon": 44792,
}

UA_CHROME = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

BROWSER_HEADERS = {
    "User-Agent": UA_CHROME,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": BASE,
    "Referer": f"{BASE}/",
    "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24", "Google Chrome";v="131"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Connection": "keep-alive",
}

RETRY_STATUS = {429, 500, 502, 503, 504}
LOG = logging.getLogger("tiki")


# --------------------------------------------------------------------------- #
# Rate limiter
# --------------------------------------------------------------------------- #

class RateLimiter:
    def __init__(self, rps: float) -> None:
        self.min_interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            sleep_for = self._last + self.min_interval - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for + random.uniform(0, 0.25))
            self._last = time.monotonic()


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #

class TikiClient:
    """Bao boc curl_cffi (impersonate Chrome) kem retry + rate limit."""

    def __init__(
        self,
        rps: float = 1.0,
        timeout: int = 25,
        max_retries: int = 4,
        impersonate: str = "chrome",
    ) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self.limiter = RateLimiter(rps)
        self.impersonate = impersonate

        self.session = curl_requests.Session()
        self.session.headers.update(BROWSER_HEADERS)
        self.trackity_id = str(uuid.uuid4())

    def _get(self, url: str, params: dict[str, Any] | None):
        return self.session.get(
            url, params=params, timeout=self.timeout, impersonate=self.impersonate
        )

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> dict | None:
        for attempt in range(self.max_retries):
            self.limiter.wait()
            try:
                resp = self._get(url, params)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Loi mang lan %d (%s): %s", attempt + 1, url, exc)
            else:
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except ValueError:
                        LOG.warning(
                            "200 nhung khong phai JSON (%d byte) - co the la trang captcha",
                            len(resp.content),
                        )
                        return None
                if resp.status_code == 404:
                    return None

                snippet = (resp.text or "")[:200].replace("\n", " ")
                LOG.warning(
                    "HTTP %d (%d byte) tu %s | %s",
                    resp.status_code,
                    len(resp.content),
                    url.replace(BASE, ""),
                    snippet or "<body rong>",
                )
                if resp.status_code not in RETRY_STATUS:
                    return None

            time.sleep(2**attempt + random.uniform(0, 1))

        LOG.error("Bo cuoc sau %d lan: %s", self.max_retries, url)
        return None


# --------------------------------------------------------------------------- #
# Crawl
# --------------------------------------------------------------------------- #

def crawl_listing(
    client: TikiClient, category_id: int, pages: int, limit: int = 40
) -> Iterator[dict]:
    working_url: str | None = None

    for page in range(1, pages + 1):
        params = {
            "limit": limit,
            "include": "advertisement",
            "aggregations": 2,
            "trackity_id": client.trackity_id,
            "category": category_id,
            "page": page,
        }

        payload = None
        for url in [working_url] if working_url else LISTING_URLS:
            payload = client.get_json(url, params)
            if payload and payload.get("data"):
                working_url = url
                break

        if not payload or not payload.get("data"):
            LOG.info("Category %s: het du lieu o trang %d", category_id, page)
            break

        items = payload["data"]
        LOG.info("Category %s trang %d: %d san pham", category_id, page, len(items))
        for item in items:
            item["_category_id"] = category_id
            item["_page"] = page
            yield item


def crawl_detail(client: TikiClient, product_id: int, spid: int | None = None) -> dict | None:
    params: dict[str, Any] = {"platform": "web"}
    if spid:
        params["spid"] = spid
    return client.get_json(DETAIL_URL.format(product_id=product_id), params)


# --------------------------------------------------------------------------- #
# Chuan hoa Bronze -> Silver
# --------------------------------------------------------------------------- #

def normalize_item(item: dict, crawled_at: str) -> dict:
    """Chuan hoa 1 san pham tu listing (+ detail neu co --detail).

    Luu y: API listing (/blocks/listings) KHONG tra ve brand duoi dang object,
    seller_name, primary_category_name hay is_authentic - cac truong nay chi
    co day du khi goi kem API detail (/api/v2/products/{id}), duoc gan vao
    item["_detail"] boi crawl_detail(). Neu khong dung --detail, cac cot nay
    se la None. Chi tiet schema: xem docs/tiki_api_schema.md.
    """
    detail = item.get("_detail") or {}
    sold = item.get("quantity_sold")
    badges = item.get("badges_new") or []
    url_key, pid = item.get("url_key"), item.get("id")

    brand = detail.get("brand") if isinstance(detail.get("brand"), dict) else None
    current_seller = detail.get("current_seller") if isinstance(detail.get("current_seller"), dict) else None
    category = detail.get("categories") if isinstance(detail.get("categories"), dict) else None

    return {
        "product_id": pid,
        "sku": item.get("sku"),
        "name": item.get("name"),
        "url_key": url_key,
        "url": f"{BASE}/{url_key}-p{pid}.html" if url_key else None,
        "price": item.get("price"),
        "list_price": item.get("list_price") or item.get("original_price") or 0,
        "discount": item.get("discount"),
        "discount_rate": item.get("discount_rate"),
        "rating_average": item.get("rating_average"),
        "review_count": item.get("review_count"),
        "quantity_sold": sold.get("value") if isinstance(sold, dict) else sold,
        "brand_id": brand.get("id") if brand else None,
        "brand_name": brand.get("name") if brand else item.get("brand_name"),
        "seller_id": item.get("seller_id") or (current_seller.get("id") if current_seller else None),
        "seller_name": current_seller.get("name") if current_seller else None,
        "category_id": item.get("_category_id"),
        "primary_category_name": category.get("name") if category else None,
        "inventory_status": item.get("inventory_status"),
        "is_authentic": any(b.get("code") == "authentic_brand" for b in badges),
        "thumbnail_url": item.get("thumbnail_url"),
        "badge_count": len(badges),
        "page": item.get("_page"),
        "crawled_at": crawled_at,
    }


# --------------------------------------------------------------------------- #
# Ghi file
# --------------------------------------------------------------------------- #

def write_bronze(records: list[dict], out_dir: Path, dt: str, category: str) -> Path:
    path = out_dir / "bronze" / f"dt={dt}" / f"category={category}"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / "listings.jsonl.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    LOG.info("Bronze -> %s (%d ban ghi)", file_path, len(records))
    return file_path


def write_silver(rows: list[dict], out_dir: Path, dt: str, category: str) -> Path | None:
    if not rows:
        return None
    path = out_dir / "silver" / f"dt={dt}" / f"category={category}"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / "products.parquet"
    df = pd.DataFrame(rows).drop_duplicates(subset=["product_id"], keep="first")
    df.to_parquet(file_path, index=False, compression="snappy")
    LOG.info("Silver -> %s (%d dong sau dedupe)", file_path, len(df))
    return file_path


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def resolve_category(token: str) -> tuple[str, int]:
    token = token.strip()
    if token.isdigit():
        return token, int(token)
    if token in CATEGORY_ALIASES:
        return token, CATEGORY_ALIASES[token]
    raise SystemExit(
        f"Khong biet category '{token}'. Dung category_id dang so hoac mot trong: "
        + ", ".join(CATEGORY_ALIASES)
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Crawl san pham Tiki -> Bronze/Silver")
    p.add_argument(
        "--categories",
        default="laptop",
        help="Alias hoac category_id, cach nhau dau phay. Dung 'all' de crawl toan bo "
        "category trong CATEGORY_ALIASES (" + str(len(CATEGORY_ALIASES)) + " category).",
    )
    p.add_argument("--pages", type=int, default=3, help="So trang moi category (40 sp/trang)")
    p.add_argument("--rps", type=float, default=1.0, help="Request toi da moi giay")
    p.add_argument("--out", default="./data", help="Thu muc output")
    p.add_argument("--detail", action="store_true", help="Goi them API chi tiet san pham")
    p.add_argument("--detail-limit", type=int, default=50)
    p.add_argument(
        "--impersonate",
        default="chrome",
        help="Profile TLS cua curl_cffi (chrome, chrome131, chrome124...)",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    client = TikiClient(rps=args.rps, impersonate=args.impersonate)

    out_dir = Path(args.out).expanduser().resolve()
    now = datetime.now(timezone.utc)
    dt = now.strftime("%Y-%m-%d")
    crawled_at = now.isoformat(timespec="seconds")
    total = 0

    tokens = list(CATEGORY_ALIASES) if args.categories.strip().lower() == "all" else args.categories.split(",")
    for token in tokens:
        name, cat_id = resolve_category(token)
        LOG.info("=== Category %s (id=%d) ===", name, cat_id)

        raw_items = list(crawl_listing(client, cat_id, args.pages))
        if not raw_items:
            LOG.warning("Category %s khong lay duoc gi", name)
            continue

        if args.detail:
            for item in raw_items[: args.detail_limit]:
                detail = crawl_detail(client, item["id"], item.get("seller_product_id"))
                if detail:
                    item["_detail"] = detail
            LOG.info("Da lay detail cho %d san pham", min(len(raw_items), args.detail_limit))

        write_bronze(raw_items, out_dir, dt, name)
        rows = [normalize_item(it, crawled_at) for it in raw_items]
        write_silver(rows, out_dir, dt, name)
        total += len(rows)

    LOG.info("Xong. Tong %d san pham. Output: %s", total, out_dir)
    return 0 if total else 1


if __name__ == "__main__":
    sys.exit(main())
