#!/usr/bin/env python3
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
from zoneinfo import ZoneInfo

try:
    from curl_cffi import requests as curl_requests
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Thieu curl_cffi. Cai bang: pip install curl_cffi") from exc

BASE = "https://tiki.vn"
LISTING_URLS = [
    f"{BASE}/api/personalish/v1/blocks/listings",
    f"{BASE}/api/v2/products",
]

CATEGORY_ALIASES: dict[str, int] = {
    "laptop": 1846,
    "dien-thoai": 1789,
    "may-tinh-bang": 1794,
    "tai-nghe": 8215,
    "sach-tieng-viet": 316,
    "do-choi": 2549,
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

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


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


class TikiClient:
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


def crawl_listing(
    client: TikiClient, category_id: int, pages: int, crawled_at: str, limit: int = 40
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
            item["_crawled_at"] = crawled_at
            yield item


def write_bronze(records: list[dict], out_dir: Path, dt: str, hour: str, category: str) -> Path:
    path = out_dir / "bronze" / f"dt={dt}" / f"hour={hour}" / f"category={category}"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / "listings.jsonl.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    LOG.info("Bronze -> %s (%d ban ghi)", file_path, len(records))
    return file_path


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
    p = argparse.ArgumentParser(description="Crawl san pham Tiki -> Bronze")
    p.add_argument(
        "--categories",
        default="laptop",
        help="Alias hoac category_id, cach nhau dau phay. Dung 'all' de crawl toan bo "
        "category trong CATEGORY_ALIASES (" + str(len(CATEGORY_ALIASES)) + " category).",
    )
    p.add_argument("--pages", type=int, default=3, help="So trang moi category (40 sp/trang)")
    p.add_argument("--rps", type=float, default=1.0, help="Request toi da moi giay")
    p.add_argument("--out", default="./data", help="Thu muc output")
    p.add_argument(
        "--impersonate",
        default="chrome",
        help="Profile TLS cua curl_cffi (chrome, chrome131, chrome124...)",
    )
    p.add_argument(
        "--hour",
        default=None,
        help="Ghi de partition hour= (HH). Mac dinh: gio he thong (Asia/Ho_Chi_Minh).",
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

    out_dir = Path(args.out).expanduser().resolve()
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(LOCAL_TZ)
    dt = now_local.strftime("%Y-%m-%d")
    hour = args.hour or now_local.strftime("%H")
    crawled_at = now_utc.isoformat(timespec="seconds")
    total = 0

    tokens = list(CATEGORY_ALIASES) if args.categories.strip().lower() == "all" else args.categories.split(",")
    for i, token in enumerate(tokens):
        if i > 0:
            time.sleep(random.uniform(3, 8))

        name, cat_id = resolve_category(token)
        LOG.info("=== Category %s (id=%d) ===", name, cat_id)

        client = TikiClient(rps=args.rps, impersonate=args.impersonate)

        raw_items = list(crawl_listing(client, cat_id, args.pages, crawled_at))
        if not raw_items:
            LOG.warning("Category %s khong lay duoc gi", name)
            continue

        write_bronze(raw_items, out_dir, dt, hour, name)
        total += len(raw_items)

    LOG.info("Xong. Tong %d san pham. Output: %s", total, out_dir)
    if total == 0:
        return 1
    # Dong stdout cuoi cung, KHONG duoc log gi sau dong nay: BashOperator
    # cua Airflow day dong cuoi cua stdout vao XCom, downstream task doc lai
    # bang {{ ti.xcom_pull(task_ids='crawl_tiki') }} de biet dung dt nao -
    # crawler tu quyet dinh dt theo gio dia phuong cua chinh no, khong nhan
    # tu Airflow (Airflow chi biet "chay luc nao", khong biet "gia nay la
    # cua ngay nao" - Tiki chi tra ve gia HIEN TAI, khong co gia lich su).
    print(dt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
