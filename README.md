# Industry Intelligence Bot

Minimal scaffold for the Industry Intelligence Bot. Contains ingest → filter → score → publish pipeline.

Quick start (dev):

1. Create virtualenv and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .
```

2. Copy `.env.example` → `.env` and edit values.

3. Run basic CLI (placeholders):

```powershell
python -m src.intel_bot.cli ingest
```

This repo is under active development; many commands are scaffolds. Follow `docs/PRODUCTION_PLAN.md` for design guidance.

## Dagster (task 0.12/0.13 — đồ thị asset có partition theo ngày)

Toàn bộ pipeline (`ingest → normalize → filter → score → dbt → publish`) đã có sẵn qua CLI
(xem `docs/PROGRESS.md`); Dagster gói lại đúng các bước đó thành một đồ thị asset partition
theo ngày, có lịch chạy 05:00 giờ Việt Nam và heartbeat ra ngoài (PRODUCTION_PLAN §7).

### Cấu hình bắt buộc trước khi chạy

Thêm vào `.env` (xem `.env.example`):

| Biến | Bắt buộc? | Ghi chú |
|---|---|---|
| `LLM_PROVIDER` | Có, không default | `mock` (miễn phí, an toàn cho dev) hoặc `deepseek` (tốn tiền thật). Thiếu/sai → asset `article_scores`/`article_summaries` fail rõ ràng thay vì âm thầm rơi về mock (bài học từ task 0.11, xem PROGRESS.md §5B — dữ liệu mock từng lọt vào gold vì quên đổi provider) |
| `HEARTBEAT_URL` | Không | URL dạng `https://hc-ping.com/<uuid>` từ [healthchecks.io](https://healthchecks.io) (free tier) hoặc dịch vụ dead-man's-switch tương đương (§7.5). Để trống thì asset `published_site` chỉ log warning, không fail |

Postgres phải đang chạy: `docker compose up -d postgres` (cổng 5435 — xem PROGRESS.md mục 3.1).

### Mở UI

```powershell
uv run dagster dev -f dagster_project/definitions.py
```

Mở http://localhost:3000 — đồ thị đủ 18 asset (`raw_rss` → … → `published_site`, cộng các
model dbt như `stg_articles`/`dim_source`/`fct_article_score`/`mart_daily_digest`) hiển thị
kèm quan hệ phụ thuộc xuyên Python/dbt.

### Materialize một phân vùng (từ UI)

Chọn asset (hoặc bôi đen toàn bộ đồ thị) → **Materialize** → chọn ngày (partition) ở
dropdown, mặc định là hôm nay.

### Materialize từ terminal (không cần UI)

```powershell
$env:PYTHONUTF8="1"; $env:PYTHONIOENCODING="utf-8"
uv run dagster asset materialize `
  --select "raw_rss,articles_normalized,stg_articles,articles_filtered,article_scores,article_summaries,stg_article_scores,stg_article_summaries,seed_sources,stg_sources,snap_sources,dim_source,fct_article_score,mart_daily_digest,mart_pipeline_health,published_site" `
  -f dagster_project/definitions.py --partition 2026-08-11
```

Hai lưu ý về môi trường Windows (đã tự gặp và verify khi làm task này):
- `$env:PYTHONUTF8`/`PYTHONIOENCODING` bắt buộc — dbt chạy qua subprocess trong asset dbt
  đọc file `.sql` có comment tiếng Việt, mặc định Windows dùng cp1252 sẽ vỡ.
- `--select "*"` KHÔNG dùng được — Click tự glob dấu `*` thành danh sách file trong thư mục
  hiện tại trên Windows. Liệt kê tên asset tường minh, phân tách bằng dấu phẩy (như trên).

### Chạy bù (backfill) một ngày cũ

Cùng lệnh trên, đổi `--partition` sang ngày muốn chạy bù:

```powershell
uv run dagster asset materialize --select "..." -f dagster_project/definitions.py --partition 2026-08-10
```

Idempotent theo nguyên tắc P1 (AGENTS.md): chạy lại một partition — kể cả partition đã chạy
trước đó — không sinh dữ liệu trùng (bronze dedup theo `payload_hash`, gold
`fct_article_score`/`mart_pipeline_health` dùng `MERGE`). Đã tự verify: materialize toàn đồ
thị 2 lần liên tiếp cho cùng một ngày → số dòng ở mọi bảng không đổi.

**Giới hạn cần biết:** RSS không có kho lưu trữ lịch sử — backfill một ngày cũ sẽ ingest
đúng nội dung feed HIỆN TẠI, chỉ gắn nhãn `ingest_date` là ngày cũ, không phải "khôi phục"
đúng những gì đã xuất bản ngày đó.

### Lịch chạy tự động

`daily_pipeline_schedule` chạy toàn đồ thị lúc 05:00 giờ Việt Nam mỗi ngày cho partition
"hôm nay" (PRODUCTION_PLAN §7.3 — lịch 12:00/18:00 bổ sung để lại Phase 1). Bật trong UI:
**Overview → Schedules** → bật toggle `daily_pipeline_job`. Lịch chỉ kích hoạt được khi có
tiến trình Dagster daemon đang chạy (`dagster dev` khi phát triển; triển khai daemon riêng
cho production là việc của Phase 1/deployment, chưa làm ở Phase 0).

### Heartbeat

Asset `published_site` gọi HTTP GET tới `HEARTBEAT_URL` ngay sau khi publish thành công
(PRODUCTION_PLAN §7.5 — cơ chế duy nhất phát hiện "pipeline không hề chạy"). Lỗi ping (thiếu
URL, timeout, HTTP lỗi) chỉ ghi log warning, KHÔNG làm fail asset — publish vẫn được coi là
thành công dù ping thất bại.
