# Data model — Giai đoạn 01

Trả lời bằng chữ, dựa trên schema Silver hiện tại (24 cột, xem
`docs/tiki_api_schema.md` và `normalize_item()` trong `tiki_crawl.py`).
Chưa viết SQL, chỉ chốt tư duy fact/dim trước.

---

## 1. Một dòng trong `fact_price_daily` là gì?

Một dòng là **giá và các chỉ số đo lường của một sản phẩm (`product_id`) tại
một lần crawl cụ thể** — tức grain = `(product_id, crawled_at)`. Nói cách
khác: "Sản phẩm X, tại thời điểm snapshot Y, có giá/rating/tồn kho là...".
Mỗi lần cron chạy (6 tiếng/lần) sinh ra một lô dòng mới cho toàn bộ sản phẩm
đang crawl được, kể cả khi giá không đổi so với lần trước — đây là fact
table dạng "periodic snapshot", không phải "transaction" (không phải mỗi
dòng là một lần giá *thay đổi*, mà là một lần *đo*).

---

## 2. Fact vs Dim trong 24 cột Silver

**Dim** (thuộc tính mô tả sản phẩm, hiếm khi đổi — thuộc về `dim_product`):

- `product_id` (khóa)
- `sku`
- `name`
- `url_key`
- `url`
- `brand_id`
- `brand_name`
- `seller_id`
- `seller_name`
- `category_id`
- `primary_category_name`
- `is_authentic`
- `thumbnail_url`

**Fact** (đo lường, đổi theo từng lần crawl — thuộc về `fact_price_daily`):

- `price`
- `list_price`
- `discount`
- `discount_rate`
- `rating_average`
- `review_count`
- `quantity_sold`
- `inventory_status`
- `badge_count`
- `page` (vị trí trong danh sách — đổi theo thuật toán xếp hạng của Tiki mỗi lần crawl, không phải thuộc tính cố định của sản phẩm)

**Ngoài 2 nhóm trên:** `crawled_at` không phải fact (không phải con số đo
lường) cũng không phải dim (không mô tả sản phẩm) — nó là **time key**, phần
tạo nên grain của fact table, giống cột ngày trong mọi periodic-snapshot
fact table.

Lưu ý: `seller_id`/`seller_name` xếp vào dim vì gắn với *listing* (1 sản
phẩm trên Tiki thường do 1 seller cố định bán), nhưng nếu sau này thấy cùng
`product_id` đổi seller giữa các lần crawl thì nên coi là slowly-changing
dimension (Type 2) chứ không fact hoá.

---

## 3. `rating_average` và `review_count` — fact hay dim?

**Câu khó nhất, chưa chắc câu trả lời sau là đúng — để mai bàn tiếp.**

Suy nghĩ của mình: về mặt kỹ thuật chúng là **fact** — là số, thay đổi theo
thời gian, và ta đo được giá trị mới mỗi lần crawl. Nhưng chúng khác
`price` ở một điểm quan trọng:

- `price` là **transactional/state fact**: người bán có thể đổi bất cứ lúc
  nào, tăng/giảm tự do, không có quy luật đơn điệu.
- `rating_average`/`review_count` là **cumulative fact**: `review_count`
  gần như đơn điệu tăng (chỉ tăng, hiếm khi giảm trừ khi bị xoá review ảo),
  và cả hai đến từ một hệ thống nguồn khác (hành vi khách mua hàng) với tần
  suất thay đổi **chậm hơn nhiều** so với giá — phần lớn các lần crawl cách
  nhau 6 tiếng sẽ cho ra giá trị y hệt lần trước.

Hệ quả thực tế: nếu lưu y hệt như `price` (mỗi lần crawl 1 dòng mới dù giá
trị không đổi), bảng sẽ có rất nhiều dòng trùng lặp thông tin về
rating/review — tốn dung lượng và có thể gây hiểu nhầm khi tính "trung bình
theo thời gian" nếu không cẩn thận (VD: rating trung bình theo tuần sẽ bị
lệch nếu đếm cả các dòng trùng giá trị).

→ Đề xuất tạm thời: **vẫn lưu chúng trong `fact_price_daily`** (để không mất
khả năng vẽ trend "rating có tụt sau lô hàng lỗi không?"), nhưng ghi chú rõ
đây là fact "chậm biến động" (slowly-changing measure), khác bản chất với
`price`. Nếu sau này thấy bảng phình to vì trùng lặp, có thể tách riêng
thành kiểu SCD Type 2 (chỉ ghi dòng mới khi giá trị thực sự đổi) thay vì
lưu tại mọi snapshot.

---

## 4. Sản phẩm `price = 0` (hết hàng): loại bỏ hay giữ cờ riêng?

**Giữ lại, không loại bỏ**, kèm cờ trạng thái riêng. Lý do:

- Loại bỏ sẽ tạo "lỗ hổng" giả trong chuỗi thời gian: nếu sản phẩm hết hàng
  1 ngày rồi có lại, xoá dòng price=0 sẽ khiến phân tích tưởng nhầm sản
  phẩm đó *biến mất khỏi Tiki* thay vì *tạm hết hàng* — sai lệch khi tính
  tỷ lệ hết hàng, thời gian hết hàng trung bình, v.v.
- Field `inventory_status` (đã có sẵn trong schema) chính là cờ đó, không
  cần thêm cột mới — chỉ cần thống nhất quy ước đọc: `price = 0` +
  `inventory_status != "available"` → hết hàng, còn `price = 0` mà
  `inventory_status = "available"` thì nhiều khả năng là lỗi crawl/parse
  cần loại riêng (không lẫn với hết hàng thật).
- Cân nhắc thêm: có thể set `price = NULL` thay vì `0` khi hết hàng, để
  tránh các phép tính `AVG(price)`/`SUM(price)` vô tình cộng thêm 0 làm
  lệch số liệu (0 khác về ngữ nghĩa với "không có giá trị").
