# RUNBOOK (task 1.10, PRODUCTION_PLAN §18.4)

8 tình huống vận hành theo đúng bảng §18.4. Mọi lệnh chẩn đoán dưới đây đã **tự chạy thật**
trên DB dev của repo này (không phải suy đoán cú pháp) — xem `docs/PROGRESS.md` mục 18 để
biết bằng chứng cụ thể (output thật của từng lệnh lúc verify).

Quy ước: `<DATABASE_URL>` = biến môi trường thật trong `.env` của máy đang chạy. Lệnh Python
inline dùng `PYTHONUTF8=1 PYTHONIOENCODING=utf-8` phía trước — bắt buộc trên Windows console
mặc định cp1252 (xem mục 9 "Pitfall môi trường" bên dưới), có thể bỏ nếu chạy trên Linux/máy
đã tự cấu hình UTF-8.

---

## 1. Sáng không có digest

**Triệu chứng:** `https://quybuno.github.io/industry-intel-bot/` không đổi nội dung so với
hôm qua, hoặc trống.

**Chẩn đoán:**
```bash
# 1. Dagster daemon có đang chạy không
docker compose ps dagster-daemon

# 2. Run gần nhất của asset published_site — thành công hay lỗi, lỗi ở đâu
docker compose logs --tail 100 dagster-daemon | grep -A5 "published_site"

# 3. mart_daily_digest hiện có bao nhiêu dòng + pipeline_health hôm nay
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    print('digest rows:', c.execute(sa.text('SELECT count(*) FROM gold.mart_daily_digest')).scalar_one())
    r = c.execute(sa.text(\"SELECT pipeline_date, ingest_count, scored_count FROM gold.mart_pipeline_health WHERE pipeline_date = current_date\")).all()
    for row in r: print(row)
"
```
Hoặc mở Dagster UI (`http://<máy-production>:3000`, xem `docs/DEPLOYMENT.md` §2) → tab Runs →
lọc theo asset `published_site` — xem trực quan hơn CLI nếu đang ngồi máy tính.

**Xử lý:** nếu run gần nhất FAILURE, rerun đúng partition hôm nay qua UI ("Re-execute"), hoặc
CLI dự phòng (task 1.8/1.9, không thay Dagster):
```bash
uv run python -m src.intel_bot.cli pipeline --date $(date +%F) --provider deepseek
```

---

## 2. Không có cả alert lẫn digest

**Triệu chứng:** không có Telegram alert, không có digest mới, im lặng hoàn toàn — dấu hiệu
rõ nhất của "máy/daemon chết" chứ không phải lỗi logic (nếu code lỗi, sensor/alert vẫn kịp
gửi cảnh báo TRƯỚC khi im lặng).

**Chẩn đoán — kiểm tra HẠ TẦNG trước, code sau (đúng thứ tự §18.4 gốc):**
```bash
# 1. Container còn sống không (nếu lệnh này TỰ NÓ không chạy được -> Docker Desktop chết,
#    xem mục 7 bên dưới)
docker compose ps

# 2. Heartbeat có ping gần đây không — log của chính app (không thay thế email cảnh báo
#    thật từ healthchecks.io, đó mới là kênh CHÍNH — xem NotifierResource.ping_heartbeat)
docker compose logs --since 24h dagster-daemon | grep -i heartbeat

# 3. Dagster daemon process có đang treo không (CPU/memory bất thường)
docker stats --no-stream industry-intel-bot-dagster-daemon-1
```

**Xử lý:** hạ tầng chết (container down/máy tắt) → khởi động lại theo `docs/DEPLOYMENT.md`
§3 trước, KHÔNG debug code khi chưa chắc hạ tầng đang chạy. Container sống nhưng heartbeat
im lặng → xem log daemon đầy đủ tìm exception, kiểm tra `HEARTBEAT_URL` trong `.env` đúng
chưa (`echo $HEARTBEAT_URL` trong container: `docker compose exec dagster-daemon printenv
HEARTBEAT_URL`).

---

## 3. Quarantine tăng vọt

**Triệu chứng:** `quarantine_sensor` gửi alert Telegram "quarantine_rate cao".

**Chẩn đoán:**
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    r = c.execute(sa.text(
        \"SELECT failure_reason, count(*) FROM silver.score_quarantine \"
        \"WHERE created_at::date = current_date GROUP BY 1 ORDER BY 2 DESC\"
    )).all()
    for row in r: print(row)
"
```
Xem mẫu `raw_response` thật của một lỗi cụ thể (thay `<failure_reason>`):
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    r = c.execute(sa.text(
        \"SELECT raw_response FROM silver.score_quarantine \"
        \"WHERE failure_reason = :reason ORDER BY created_at DESC LIMIT 3\"
    ), {'reason': '<failure_reason>'}).all()
    for row in r: print(row[0][:500])
"
```

**Xử lý:** đọc `raw_response` mẫu → xác định provider đổi format hay prompt có lỗ hổng →
sửa prompt (`config/models.yaml`/`prompts/`) → **bump `prompt_version`** (không sửa đè cùng
version — phá khả năng so sánh trước/sau, §14.2) → rerun partition bị ảnh hưởng.

---

## 4. Điểm toàn bộ dồn 6–7 (rubric mất khả năng phân biệt)

**Triệu chứng:** `stddev_importance` trong `mart_pipeline_health` thấp bất thường (< ngưỡng
`anomaly_importance_stddev_min` ở `dbt_project.yml`).

**Chẩn đoán:**
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    r = c.execute(sa.text(
        'SELECT pipeline_date, mean_importance, stddev_importance FROM gold.mart_pipeline_health '
        'ORDER BY pipeline_date DESC LIMIT 7'
    )).all()
    for row in r: print(row)
"
```

**Xử lý:** rubric chấm điểm (prompt) không đủ phân giải — xem PRODUCTION_PLAN §22.1 (đánh
giá lại rubric, có thể do model đổi hành vi âm thầm — §13.3 "model drift"). Không sửa ngưỡng
`anomaly_importance_stddev_min` để tắt cảnh báo — đó là nới rào chắn, không phải sửa nguyên
nhân.

---

## 5. Nguồn 403 (hoặc lỗi HTTP khác kéo dài)

**Triệu chứng:** `run_rss_ingest` báo `sources_failed` tăng, hoặc `source_fail_rate` sensor
gửi alert (> 30% nguồn fail, §18.3).

**Chẩn đoán:**
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    r = c.execute(sa.text(
        'SELECT source_id, http_status, error_message, fetch_date FROM silver.source_health '
        'WHERE http_status IS NOT NULL AND http_status >= 400 '
        'ORDER BY fetch_date DESC LIMIT 10'
    )).all()
    for row in r: print(row)
"
```

**Xử lý:** đổi `user_agent` (`config/app.yaml` mục `ingest.user_agent`) nếu nguồn chặn bot
mặc định; kiểm tra `robots.txt` của nguồn đó xem có cấm crawl không (P4 rule: tôn trọng
robots.txt, §19.2); nếu nguồn liên tục fail và không sửa được, tạm `is_enabled: false` trong
`config/sources.yaml`/`config/github_sources.yaml` thay vì để sensor gửi alert lặp lại vô ích.

---

## 6. Bài trùng trên trang

**Triệu chứng:** 2 card giống hệt (cùng tiêu đề/link) xuất hiện trên digest.

**Chẩn đoán** (join `mart_daily_digest` → `silver.articles` để lấy `content_hash`, vì mart
không tự giữ cột này — chỉ có ở `silver.articles`):
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    r = c.execute(sa.text('''
        SELECT a.content_hash, count(*), array_agg(d.canonical_url)
        FROM gold.mart_daily_digest d
        JOIN silver.articles a ON a.article_id = d.article_id
        GROUP BY a.content_hash HAVING count(*) > 1
    ''')).all()
    for row in r: print(row)
"
```

**Xử lý:** nếu có kết quả (đáng lẽ 0 dòng — `int_articles_deduped.sql` đã dedup cấp 2 theo
`content_hash`), chạy lại đúng model đó:
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run dbt build --select int_articles_deduped+ --project-dir dbt_project --profiles-dir dbt_project --vars "{\"run_date\": \"$(date +%F)\"}"
```

---

## 7. Cost tăng bất thường

**Triệu chứng:** `cost_sensor` gửi alert (> 80% ngân sách tháng), hoặc tự thấy hoá đơn
DeepSeek cao hơn dự kiến.

**Chẩn đoán:**
```bash
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 uv run python -c "
import os, sqlalchemy as sa
engine = sa.create_engine(os.environ['DATABASE_URL'], future=True)
with engine.connect() as c:
    r = c.execute(sa.text(
        'SELECT pipeline_date, total_cost_usd, cost_per_article, scored_count '
        'FROM gold.mart_pipeline_health ORDER BY pipeline_date DESC LIMIT 14'
    )).all()
    for row in r: print(row)
"
```

**Xử lý:** `cost_per_article` tăng (không phải `scored_count` tăng) → prompt bị phình
(kiểm tra `prompts/` có thay đổi gần đây không, `git log -p prompts/`) hoặc giá model đổi
(kiểm tra `config/models.yaml` `pricing.*` còn đúng giá công bố không — §15.2, giá thay đổi
theo lịch nhà cung cấp). `scored_count` tăng bất thường (không phải giá/prompt) → kiểm tra
`max_articles_per_day` (`config/app.yaml`) có bị nới/bỏ qua không.

---

## 8. Git push bị từ chối (PAT hết hạn)

**Triệu chứng thật đã tái hiện** (không phải suy đoán — cố ý dùng PAT giả để verify đúng
message này xuất hiện, xem `docs/PROGRESS.md` mục 18):
```
git push thất bại: remote: Invalid username or token. Password authentication is not
supported for Git operations.
fatal: Authentication failed for 'https://github.com/.../industry-intel-bot.git/'
```
Đây LÀ tình huống đã có sẵn trong code (`src/intel_bot/publish/git_publish.py`) — asset
`published_site` KHÔNG fail, chỉ log warning + gửi alert Telegram, file cục bộ vẫn ghi đúng
(rào chắn task 1.10 mục 1) — nghĩa là: digest có thể ĐÚNG trong Postgres/file cục bộ nhưng
trang GitHub Pages KHÔNG cập nhật.

**Chẩn đoán:**
```bash
# Log warning + alert Telegram gần nhất về push
docker compose logs --since 24h dagster-daemon | grep -i "push docs-site"

# Kiểm tra token còn hợp lệ không (không cần push thật — gọi API GitHub với chính token đó)
curl -s -o /dev/null -w "HTTP=%{http_code}\n" -H "Authorization: Bearer $GIT_PUBLISH_TOKEN" https://api.github.com/user
```
`HTTP=401` = token hết hạn/sai. `HTTP=200` = token vẫn hợp lệ, lỗi push do nguyên nhân khác
(mạng, nhánh `gh-pages` bị xoá — kiểm tra `git ls-remote origin gh-pages` trả về rỗng hay
không).

**Xử lý:** tạo PAT mới (GitHub → Settings → Developer settings → Fine-grained tokens),
scope tối thiểu `Contents: Read and write` trên đúng repo này, cập nhật `GIT_PUBLISH_TOKEN`
trong `.env`, restart container để nạp lại biến môi trường:
```bash
docker compose up -d --force-recreate dagster-daemon
```

---

## 9. Pitfall môi trường Windows/Docker đã gặp thật (không phải giả định)

Ghi lại từ `docs/PROGRESS.md` mục 3 (máy dev) — có thể lặp lại trên máy production nếu cũng
là Windows + Docker Desktop:

- **Docker Desktop tự tắt/crash giữa chừng** (`docker ps` báo lỗi pipe/kết nối) — đã gặp
  nhiều lần trên máy dev. Phục hồi: `Start-Process 'C:\Program Files\Docker\Docker\Docker
  Desktop.exe'`, đợi 30-60s, rồi `docker compose up -d postgres dagster-daemon
  dagster-webserver`. Volume không mất dữ liệu qua các lần crash này (đã tự verify nhiều
  lần, đếm lại bảng trước/sau khớp).
- **Cổng Postgres đụng nhau**: nếu máy production CŨNG có Postgres native cài sẵn (không chỉ
  máy dev này mới gặp), `POSTGRES_PORT` trong `.env` phải là cổng KHÔNG bị chiếm — triệu
  chứng nếu quên: `password authentication failed` dù mật khẩu đúng (connection rơi vào
  Postgres native, không chạm container) — xác nhận bằng `docker compose logs postgres`
  không có dòng nào cho lần connect đó.
- **Console Windows mặc định cp1252** — mọi lệnh Python inline ở RUNBOOK này đặt
  `PYTHONUTF8=1 PYTHONIOENCODING=utf-8` phía trước vì lý do này (không phải thừa) — thiếu sẽ
  vỡ `UnicodeDecodeError`/`UnicodeEncodeError` ngay khi in tiếng Việt.
