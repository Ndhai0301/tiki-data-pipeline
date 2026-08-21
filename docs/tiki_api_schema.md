# Tiki API Schema (dò ngày 2026-08-20)

Ghi chú: đây là schema thực tế quan sát được từ 2 endpoint dùng trong
`tiki_crawl.py`. Tiki không công bố schema chính thức nên field có thể
thay đổi bất cứ lúc nào — coi đây là tài liệu tham khảo, không phải hợp đồng
API cố định. File JSON mẫu đầy đủ nằm cạnh file này:
- `sample_listing_response.json`
- `sample_detail_response.json`

Cách tự dò lại khi API đổi field (xem thêm ở tiki_crawl.py --probe):

```bash
python3 -c "
from curl_cffi import requests
r = requests.get('URL', params={...}, impersonate='chrome')
print(r.text)
" | python3 -m json.tool
```

---

## 1. Listing API

`GET https://tiki.vn/api/personalish/v1/blocks/listings`
`GET https://tiki.vn/api/v2/products` (cùng schema, dùng làm fallback trong code)

Params dùng trong crawler: `limit, category, page, include=advertisement, aggregations=2, trackity_id`

### Top-level

| Field | Kiểu | Ghi chú |
|---|---|---|
| `block` | object | `{code, title, icon}` — metadata block hiển thị |
| `data` | array\<object> | **Danh sách sản phẩm — bảng chính** |
| `paging` | object | `current_page, from, last_page, per_page, to, total` |
| `filters` | array | bộ lọc khả dụng (thường rỗng ở list cơ bản) |
| `amplitude` | object | tracking cấp trang: `model_name, search_experiment_variant, selected_sort_option...` |
| `sort_options` | array\<object> | `{display_value, query_value, selected}` |
| `widgets` | array | thường rỗng |
| `trace_id` | str | id trace request |

### `data[]` — 1 sản phẩm trong danh sách

| Field | Kiểu | Ghi chú |
|---|---|---|
| `id` | int | product_id |
| `sku` | str | |
| `name` | str | |
| `url_key` | str | dùng ghép URL: `https://tiki.vn/{url_key}-p{id}.html` |
| `url_path` | str | có sẵn cả `spid` trong query string |
| `brand_name` | str | **field phẳng**, KHÔNG nằm trong object `brand` (khác với API detail) |
| `price` | int | giá bán |
| `list_price` | int | **luôn = 0 ở listing**, dùng `original_price` thay thế |
| `original_price` | int | giá gốc thật (dùng cái này, không dùng `list_price`) |
| `discount` | int | |
| `discount_rate` | int | % giảm giá |
| `rating_average` | float | |
| `review_count` | int | |
| `quantity_sold` | object\|null | `{text, value}` |
| `seller_id` | int | |
| `seller` | null | **luôn null** ở listing, không có tên seller trực tiếp |
| `seller_product_id` | int | dùng làm `spid` khi gọi detail |
| `inventory_status` | str | |
| `thumbnail_url` | str | |
| `primary_category_path` | str | chuỗi id, vd `1/2/1846/8060/1827` — KHÔNG phải tên category |
| `badges_new[]` | array\<object> | `{code, text, icon, placement, type}` — nhãn hiển thị (chính hãng, giao hàng, quà tặng...) |
| `badges_v3[]` | array\<object> | tương tự badges_new, phiên bản mới |
| `impression_info[].metadata` | object | tracking nội bộ: `is_ad, is_official_store, is_tikinow, delivery_zone...` |
| **`visible_impression_info.amplitude`** | object | ⭐ **xem bảng riêng bên dưới — chứa field quan trọng không có ở nơi khác** |

### ⭐ `data[].visible_impression_info.amplitude` — field ẩn nhưng quan trọng

Đây là nhóm field **có sẵn ngay ở listing**, không cần gọi API detail:

| Field | Kiểu | Ghi chú |
|---|---|---|
| `is_authentic` | int (0/1) | hàng chính hãng hay không |
| `primary_category_name` | str | **tên category**, ứng với `primary_category_path` |
| `category_l1_name` | str | tên category cấp 1 |
| `category_l2_name` | str | tên category cấp 2 |
| `brand_name` | str | trùng với `data[].brand_name` |
| `seller_type` | str | |
| `is_top_brand` | bool | |
| `is_imported` | bool | |
| `is_freeship_xtra` | bool | |
| `is_flash_deal` | bool | |
| `tiki_verified` | int | |
| `number_of_reviews` | int | |
| `product_rating` | int | |
| `all_time_quantity_sold` | int | |

→ **Gợi ý:** sửa `normalize_item()` lấy `is_authentic`, `primary_category_name` từ đây thay vì bắt buộc gọi `--detail`, tiết kiệm request.

---

## 2. Detail API

`GET https://tiki.vn/api/v2/products/{product_id}?platform=web&spid={seller_product_id}`

~70 field ở top-level. Nhóm theo chủ đề:

### Thông tin cơ bản
`id, master_id, sku, name, url_key, short_url, price, list_price, original_price, discount, discount_rate, rating_average, review_count, favourite_count, thumbnail_url, inventory_status, inventory_type, description, meta_title, meta_description`

Lưu ý: ở đây `list_price` **có giá trị đúng** (khác listing).

### `brand` (object)
`id, name, slug` — có `brand.id` (listing không có).

### `current_seller` (object)
`id, sku, name, link, logo, price, product_id, store_id, is_best_store` — nguồn duy nhất có **tên seller**.

### `categories` (object) + `breadcrumbs[]`
`categories: {id, name, is_leaf}` — tên category lá.
`breadcrumbs[]: {url, name, category_id}` — full đường dẫn danh mục cha → con.

### `images[]`
`base_url, large_url, medium_url, small_url, thumbnail_url, is_gallery`

### `specifications[]` — thông số kỹ thuật
```
specifications[].name            (vd "Thông tin chung")
specifications[].attributes[]    {code, name, value}
```

### Biến thể sản phẩm
```
configurable_options[]   {code, name, values[].label}         # vd option "Dung lượng", "Màu"
configurable_products[]  {child_id, id, name, option1, price,
                           original_price, discount_rate,
                           inventory_status, selected,
                           seller: {id, name}, images[]}
```

### Tồn kho & chính sách
```
stock_item        {qty, min_sale_qty, max_sale_qty, preorder_date}
warranty_info[]    {name, value}
warranty_policy
return_policy      {title, body[], cta}
return_and_exchange_policy
benefits[]         {icon, text}
installment_info_v3[]  {display_text, summary, title, type, url}
```

### `tracking_info.amplitude` (object)
`is_authentic (bool), is_hero, is_top_brand, is_freeship_xtra, return_reason`

Lưu ý: ở detail `is_authentic` là **bool**, ở listing (`visible_impression_info.amplitude.is_authentic`) là **int 0/1** — cần ép kiểu nhất quán khi gộp 2 nguồn.

---

## Khác biệt field quan trọng giữa 2 API (bẫy hay gặp)

| Field mong muốn | Listing API | Detail API |
|---|---|---|
| Tên hãng | `brand_name` (str phẳng) | `brand.name` (trong object) |
| ID hãng | ❌ không có | `brand.id` |
| Giá gốc | `original_price` (KHÔNG dùng `list_price`, luôn = 0) | `list_price` (đúng) |
| Tên seller | ❌ không có (`seller` = null) | `current_seller.name` |
| Tên category | `visible_impression_info.amplitude.primary_category_name` | `categories.name` |
| Hàng chính hãng | `visible_impression_info.amplitude.is_authentic` (int) | `tracking_info.amplitude.is_authentic` (bool) |
