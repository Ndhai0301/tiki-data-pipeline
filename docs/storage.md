# Storage layout & quy ước partition

Chốt 1 lần, không đổi tuỳ tiện — mọi query/job compaction/dbt source đều
dựa vào cấu trúc này. Nếu cần đổi, phải có kế hoạch migrate dữ liệu cũ
kèm theo, không đổi chay.

## Cây thư mục (mục tiêu)

```
data/
├── bronze/
│   └── dt=2026-08-22/
│       └── hour=06/
│           └── category=laptop/
│               └── listings.jsonl.gz      # ban goc, bat bien
├── silver/
│   └── dt=2026-08-22/
│       └── hour=06/
│           └── category=laptop/
│               └── products.parquet       # da chuan hoa kieu, dedupe trong tung file
├── silver_compacted/                       # san pham cua compact.py, xem muc "Compaction"
│   └── category=laptop/
│       └── 2026-08.parquet
└── gold/                                   # khong nam trong data/, o Postgres (xem docker-compose.yml)
```

| Layer | Định dạng | Ghi đè được? | Vai trò |
|---|---|---|---|
| Bronze | JSONL + gzip | Không bao giờ | Bản gốc để parse lại khi Tiki đổi schema |
| Silver | Parquet + snappy | Không (mỗi `dt/hour/category` là 1 file cố định, ghi 1 lần) | Kiểu dữ liệu chuẩn, dedupe theo `product_id` trong phạm vi 1 lần crawl |
| silver_compacted | Parquet + snappy | Có, job compaction ghi đè theo tháng | Gộp nhiều file nhỏ cùng category/tháng thành 1 file lớn |
| Gold | Bảng Postgres | dbt tự dựng lại | Star schema cho BI (Metabase) |

## Quy ước partition: `dt=` + `hour=` + `category=`

- **`dt=YYYY-MM-DD`**: ngày crawl.
- **`hour=HH`**: giờ crawl (00/06/12/18, khớp lịch cron 6 tiếng/lần). Bắt
  buộc có — nếu chỉ partition theo `dt` như ví dụ ban đầu, 4 lần crawl
  cùng ngày sẽ ghi đè lẫn nhau và **mất hết dữ liệu để so biến động giá
  trong ngày** (mục tiêu chính của dự án).
- **`category=<ten>`**: alias category trong `CATEGORY_ALIASES`.
- **Cả 2 giá trị `dt` và `hour` dùng giờ ĐỊA PHƯƠNG (Asia/Bangkok,
  +07:00)**, không dùng UTC. Lý do: lịch cron (`0 */6 * * *`) chạy theo
  giờ hệ thống (đã xác nhận là Asia/Bangkok), con người đọc thư mục
  `hour=06` sẽ hiểu ngay là "crawl lúc 6h sáng giờ VN" — dùng UTC sẽ ra
  `hour=23`/`hour=05`... gây nhầm lẫn không đáng có ở quy mô 1 project cá
  nhân. Đánh đổi: mất tính "portable" thường thấy khi dùng UTC, chấp nhận
  được vì hệ thống chỉ chạy trên 1 máy, 1 múi giờ.

## ✅ Đã migrate (2026-08-24)

`tiki_crawl.py` đã sửa để khớp quy ước trên:
- `dt`/`hour` tính theo `datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Bangkok"))`
  thay vì UTC trực tiếp; `crawled_at` (field bên trong dữ liệu, không phải
  partition) vẫn giữ UTC — timestamp có offset rõ ràng, không mơ hồ.
- `write_bronze`/`write_silver` nhận thêm tham số `hour`, ghi ra
  `dt=.../hour=.../category=...`.
- Cron đổi từ `--out ~/tiki-crawl/data/snap_$(date +\%Y\%m\%d_\%H)` (bọc
  snapshot theo tên thư mục) sang `--out ~/tiki-crawl/data` cố định —
  partition `hour=` tự phân biệt các lần chạy trong ngày, không cần nhét
  giờ vào tên thư mục nữa.
- 6 snapshot cũ (`snap_20260821_16` → `snap_20260824_12`, 176 file) đã
  được di chuyển vào cấu trúc mới, suy `dt`/`hour` từ tên thư mục cũ
  (vốn đã đúng giờ VN vì sinh bằng `date` của shell) — không suy từ
  giá trị `dt=` cũ bên trong (giá trị đó sai, tính theo UTC).
- Đã xác nhận bằng DuckDB: `SELECT dt, hour, category FROM
  'data/silver/*/*/*/products.parquet'` tự nhận diện đúng 3 cột, không
  cần glob thủ công.

## Compaction (vấn đề file nhỏ)

Mỗi `dt/hour/category` là 1 file Silver ~100-200 KB. Ở nhịp crawl hiện tại
(29 category × 4 lần/ngày), sau 1 tháng có **~3.480 file nhỏ** — đọc bằng
Spark/DuckDB sẽ chậm vì chi phí mở file/đọc metadata > thời gian đọc dữ
liệu thật. Với volume hiện tại (~1.5 GB/tháng) thực tế chưa cần gấp, nhưng
viết job compaction chạy định kỳ (hàng tháng) và để trong DAG là chuẩn bị
trước cho lúc cần — xem `compact.py`, output ghi vào
`data/silver_compacted/category=<ten>/<YYYY-MM>.parquet`, **không** ghi
đè hay xoá file gốc trong `silver/` (an toàn để chạy lại nhiều lần).

## `samples/`

`samples/listings_sample.jsonl` — 20 dòng thật trích từ Bronze
(`snap_20260822_06`, category `laptop`), không nén, commit thẳng vào git
(khác với `data/` bị `.gitignore`) — để người khác clone repo có ngay dữ
liệu mẫu chạy thử `normalize_item()`/transform mà không cần tự crawl.
