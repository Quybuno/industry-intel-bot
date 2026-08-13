# PROGRESS.md — trạng thái thật của implementation, đọc TRƯỚC khi nhận task mới

> File này là NHẬT KÝ TRIỂN KHAI (living doc), khác với `PRODUCTION_PLAN.md` (bản thiết kế
> tĩnh, không sửa). Mọi AI Coding Agent nhận task mới trên repo này nên đọc file này trước,
> **sau đó** đọc phần liên quan của `PRODUCTION_PLAN.md` như `AGENTS.md` yêu cầu.
>
> Quy tắc cập nhật: sau khi hoàn thành một task, thêm/sửa các mục bên dưới — đặc biệt mục
> "Lệch khỏi plan" và "Trạng thái theo phase". Đừng để file này lạc hậu so với code.

## 1. Trạng thái theo phase (Phase 0)

| # | Task | Trạng thái | Ghi chú |
|---|------|-----------|---------|
| 0.1 | Repo scaffold | ✅ | Scaffold v1 (ORM, SQLite) — phần lớn đã bị thay thế, xem mục 4 |
| 0.2 | Docker Compose: postgres | ✅ | Cổng **5435**, không phải 5432 — xem mục 3 |
| 0.3 | Alembic: schema bronze + silver | ✅ | Migration 0001-0002 |
| 0.4 | RSS fetcher → bronze | ✅ | 8 nguồn RSS đã verify thật (không bịa URL) |
| 0.5 | Normalizer + dedup → silver.articles | ✅ | Migration 0003 (raw_url, published_at_imputed) |
| 0.6 | Filter tối thiểu | ✅ | Blocklist keyword, không embedding (Phase 2) |
| 0.7 | LLM contract (Pydantic v2) + provider mock | ✅ | `src/intel_bot/contracts/llm_score.py` |
| 0.8 | Provider cloud + cost tracking | ✅ | **DeepSeek, không phải Gemini** — xem mục 2.1 |
| 0.9 | Quarantine + luồng lỗi | ✅ | Gộp chung với 0.8, cùng một lần giao việc. Migration 0004 |
| 0.10 | dbt: staging + gold | ✅ | Xem mục 5A — composite/dedup/SCD2 chạy thật trên DB dev |
| 0.11 | Publish | ✅ | Xem mục 5B — JSON + HTML tĩnh chạy thật; 24 bài lúc mới xong, còn 12 sau khi 5B tự sửa lỗi mock (xem 5B) |
| 0.12 + 0.13 | Dagster asset graph + schedule + heartbeat | ✅ | Xem mục 5C — `dagster dev` chạy thật, materialize toàn đồ thị + backfill + heartbeat thật đều verify được |

Lệnh CLI đã có thật (chạy bằng `uv run python -m src.intel_bot.cli <lệnh>` HOẶC
`uv run intel-bot <lệnh>` — cả hai đều chạy được từ D2, xem mục 3.4):

```
ingest --date YYYY-MM-DD
validate-sources
normalize --date YYYY-MM-DD
filter --date YYYY-MM-DD
score --date YYYY-MM-DD --provider mock|deepseek
publish --date YYYY-MM-DD
doctor
```

`eval` vẫn là placeholder. `pipeline --date YYYY-MM-DD [--provider mock|deepseek]` KHÔNG
còn placeholder từ task 1.8/1.9 — chạy tuần tự ingest → normalize → filter → score → dbt
build (marts) → publish, đường chạy dự phòng khi Dagster không dùng được (xem mục 17).

Từ task 0.12, còn có đường chạy thứ hai qua Dagster (song song với CLI, không thay thế —
CLI vẫn chạy độc lập được): `uv run dagster dev -f dagster_project/definitions.py` (UI),
`uv run dagster asset materialize --select "<tên asset>" -f dagster_project/definitions.py
--partition YYYY-MM-DD` (headless). Xem README mục "Dagster" để biết chi tiết + 2 gotcha
Windows (UTF-8 env var, không dùng `--select "*"`).

## 2. Lệch khỏi PRODUCTION_PLAN.md — BẮT BUỘC đọc trước khi động vào lớp Score

### 2.1 Provider: DeepSeek thay Gemini, đồng bộ thay Batch API

Plan (§10.2, §10.3) chỉ định Gemini Batch API làm provider mặc định. Giữa task 0.8, người
dùng yêu cầu đổi sang DeepSeek. Đã xác minh trực tiếp qua `api-docs.deepseek.com` (không
đoán) ngày 2026-08-11:

- Model hiện hành: `deepseek-v4-flash` (rẻ, mặc định) và `deepseek-v4-pro`. `deepseek-chat`/
  `deepseek-reasoner` đã retire 24/07/2026.
- **DeepSeek không có Batch API** — không có endpoint submit/poll/retrieve nào được tài
  liệu hoá. `src/intel_bot/score/providers/deepseek.py` gọi `/chat/completions` ĐỒNG BỘ.
- Hệ quả: không có khái niệm "job id" để Dagster sensor poll (khác thiết kế gốc ở §10.3).
  Khi làm task orchestration (Dagster, ngoài phạm vi Phase 0), nhớ điều này.
- `config/models.yaml` có `batch_discount: 0.0` cho deepseek — không phải giá trị đoán, mà
  là hệ quả trực tiếp của việc không có Batch API để chiết khấu.
- Giá DeepSeek có cảnh báo chính thức "sẽ tăng đáng kể trong thời gian tới" — kiểm tra lại
  `config/models.yaml` so với trang pricing trước khi tin số hiện tại.
- `GEMINI_API_KEY` trong `.env`/`.env.example` là biến RIÊNG của agent (dùng ở "fast mode"
  tóm lược tài liệu — xem `~/.claude/.../memory/fast-deep-analysis-mode.md`), KHÔNG liên
  quan gì tới pipeline scoring. Đừng nhầm hai cái.

### 2.2 `LLMProvider` Protocol rộng hơn plan mô tả

Plan (§10.2) chỉ nêu `score_batch` + `estimate_cost`. Đã mở rộng thêm (bắt buộc cho task
0.8 mục "sinh tóm tắt top-K"):

- `summarize_batch(items) -> list[SummaryOutcome]` — cùng nguyên tắc lỗi như score_batch.
- `ProviderUnavailableError` — exception THẬT, raise khi cả provider không dùng được
  (§10.5 dòng "Provider không dùng được"). Đây là ngoại lệ DUY NHẤT được phép raise từ
  provider; lỗi từng bản ghi luôn là giá trị trả về (`ScoreFailure`/`SummaryFailure`).
- `ScoreFailure`/`SummaryFailure` có thêm `attempt_no` (số lần thử thật trước khi kết luận
  thất bại — ghi vào `silver.score_quarantine.attempt_no`).
- `ScoreSuccess`/`SummarySuccess` có thêm `input_cache_hit_tokens` (0 nếu provider không hỗ
  trợ cache — vd. mock; DeepSeek có context caching thật, ảnh hưởng giá).

`src/intel_bot/score/providers/mock.py` đã cập nhật theo — `MockProvider` giờ hỗ trợ
`summarize_batch()` và tham số `raise_unavailable=True` để test dòng "provider không dùng
được" mà không cần code thật.

### 2.3 Composite score cho top-K tóm tắt — TẠM THỜI, sẽ bị dbt (0.10) thay thế

`src/intel_bot/score/composite.py` cài công thức §5.7 (importance 40%, practicality 30%,
credibility 30%, depth 0%, recency boost) để CHỌN TOP-K BÀI TÓM TẮT NGAY BÂY GIỜ — vì task
0.8 cần chọn K trước khi dbt/gold tồn tại.

**Khác biệt đã biết so với công thức chính thức:** §5.7 "Sửa lỗi 4" nói `credibility` chính
thức là 80% source tier (từ `gold.dim_source`, SCD Type 2, do dbt sinh) + 20% điểm LLM thô.
`gold.dim_source` chưa tồn tại nên `composite.py` dùng credibility THÔ từ LLM — xấp xỉ, chỉ
đủ tốt để xếp hạng tương đối chọn top-K, KHÔNG phải nguồn sự thật.

**Khi làm task 0.10:** implement `gold.fct_article_score` đúng công thức thật trong dbt,
rồi **xoá `composite.py`**, sửa `runner.py::_summarize_top_k()` để đọc composite score từ
`gold.fct_article_score` thay vì gọi `compute_composite_score()`. Đừng giữ song song hai
công thức.

**Cập nhật D1 (mục 9):** việc này bị hoãn qua cả 0.10 (5A)/0.12 (5C) vì "xoá composite.py"
kéo theo một thay đổi luồng orchestration thật (dbt phải chạy XEN GIỮA chấm điểm và tóm tắt
top-K) — không đơn giản như đổi một lời gọi hàm. Đã làm ở D1.

### 2.4 Migration bổ sung ngoài kế hoạch gốc (không sửa migration cũ)

- `0003_articles_debug_cols.py` — thêm `silver.articles.raw_url`, `published_at_imputed`
  (thiếu ở migration gốc 0002, cần cho task 0.5).
- `0004_summaries_unique.py` — thêm `UNIQUE (article_id, prompt_version, model_name)` cho
  `silver.article_summaries` (thiếu ở 0002; cần để `INSERT ... ON CONFLICT DO NOTHING` cho
  tóm tắt, giống hệt cơ chế `article_scores` — P1).

## 3. Gotcha môi trường dev (máy này cụ thể)

### 3.1 Cổng Postgres

Máy dev có **3 Postgres native** cài sẵn chiếm 5432 (v17), 5433 (v16), 5434 (v14) — không
phải giả định, đã xác minh bằng `netstat` + đọc `postgresql.conf` từng bản cài. Docker
Compose `postgres` dùng cổng **5435** (`POSTGRES_PORT` trong `.env`/`.env.example`).
Triệu chứng nếu quên: lỗi `password authentication failed` dù mật khẩu đúng — vì connection
rơi vào Postgres native, không chạm container. Xác nhận bằng `docker logs <container>`
không có dòng log nào cho lần connect đó.

### 3.2 Docker Desktop không ổn định trên máy này

Đã crash/tự tắt nhiều lần giữa chừng session (silent, `docker ps` báo lỗi pipe). Cách phục
hồi: `Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe'`, chờ 30-60s, rồi
`docker compose up -d postgres`. Volume `pgdata` không mất dữ liệu qua các lần crash này.

### 3.3 Console Windows mặc định cp1252

Không in được tiếng Việt trực tiếp → mọi script CLI thật (không phải test) cần ép UTF-8 ở
đầu file:
```python
if isinstance(sys.stdout, io.TextIOWrapper) and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
```
Đã áp dụng ở `cli.py`. Khi debug bằng `python -c "..."` một lần (không sửa file), dùng
`PYTHONIOENCODING=utf-8` trước lệnh thay vì sửa code.

### 3.4 `uv run intel-bot <cmd>` KHÔNG chạy được — lỗi packaging từ task 0.1 — **ĐÃ SỬA ở D2**

`pyproject.toml` map wheel `src/intel_bot` → `intel_bot`, nhưng TOÀN BỘ codebase import
kiểu `from src.intel_bot.xxx import yyy`. Console-script entry point
(`intel-bot = "src.intel_bot.cli:main"`) vì vậy luôn báo
`ModuleNotFoundError: No module named 'src'`. Workaround dùng xuyên suốt dự án tới D2:
`uv run python -m src.intel_bot.cli <lệnh>` (chạy từ repo root) — **vẫn còn dùng được, hai
cách giờ tương đương.**

**D2 đã chọn sửa wheel mapping (KHÔNG đổi import):** ~40 file đang import
`from src.intel_bot.xxx` xuyên suốt dự án (cli.py, mọi module `src/`, toàn bộ
`dagster_project/`, toàn bộ `tests/`) — đổi hết sang `intel_bot.xxx` là diff lớn, rủi ro cao
cho lợi ích thuần packaging, và vi phạm tinh thần AGENTS.md mục 5.5 (không tự ý đổi quy ước
đã dùng xuyên suốt dự án). Thay vào đó:
- `[tool.hatch.build.targets.wheel] packages = ["src/intel_bot"]` → `packages = ["src"]`
  (đóng gói "src" làm package gốc thay vì strip mất tiền tố "src").
- Thêm `src/__init__.py` RỖNG — hatchling cần "src" là package thật (có `__init__.py`), không
  chỉ namespace package, để nhận diện đúng khi build wheel. `src/intel_bot/` vẫn KHÔNG có
  `__init__.py` (giữ nguyên namespace package như trước — Python cho phép namespace package
  lồng trong package thường).
- Verify: `uv sync` build lại wheel sạch, `uv run intel-bot doctor` chạy thật (kết nối
  Postgres, liệt kê đủ bronze/silver/gold) — không còn `ModuleNotFoundError`.
  `mypy --strict src/`/`ruff check src/` trước và sau khi thêm `src/__init__.py` ra ĐÚNG
  cùng 58/62 lỗi (đã tự verify bằng `git stash -u` so sánh) — không sinh lỗi mới, toàn bộ lỗi
  còn lại nằm trong code legacy (mục 4, D3 xoá) + vài file thật có nợ type nhỏ
  (`config.py`, `db/health.py`, `observability/logging.py`) sẽ dọn nốt ở D3 vì D3 yêu cầu
  `src/` sạch toàn bộ.

### 3.5 Ổ C gần đầy (2026-08-12) — Docker Desktop WSL disk đã relocate sang ổ D

Không phải lỗi code, nhưng ảnh hưởng trực tiếp tới việc chạy Docker/Postgres trên máy dev
này — ghi lại để không dò lại từ đầu. Ổ C chỉ còn 2.1GB trống giữa lúc làm task 1.2. Điều tra
thật (không đoán): repo đã nằm sẵn trên ổ D; `uv`/`pip` cache là **junction trỏ sang
`D:\Cache\uv`/`D:\Cache\pip`** từ trước (không chiếm ổ C thật, dù `Get-ChildItem -Recurse`
báo dung lượng theo junction); thứ THẬT SỰ chiếm ổ C là **Docker Desktop WSL disk
(`%LOCALAPPDATA%\Docker\wsl`, ~10.4GB)**. Trước khi đổi: backup `pg_dump` (format custom)
DB `intel_bot` ra `D:\db-backups\` (đã verify `pg_restore -l` đọc được, 71 TOC entries) —
phòng khi Docker Desktop's "Disk image location" (Settings → Resources → Advanced) di chuyển
lỗi. Đổi qua GUI (không tự động hoá được — không có quyền điều khiển GUI) → Docker Desktop tự
di chuyển + Apply & Restart → container `industry-intel-bot-postgres-1` exit code 255 (bị
Docker Desktop restart), `docker compose up -d postgres` khởi động lại — **dữ liệu còn
nguyên** (đã verify đếm lại bronze/gold khớp trước/sau). Ổ C: 2.1GB → 17.4GB trống. **Máy này
`wsl -l -v` KHÔNG có distro `docker-desktop-data` riêng** (khác model 2-distro truyền thống)
— nếu sau này cần thao tác WSL/Docker thủ công, đừng giả định cấu trúc chuẩn, kiểm tra lại.

## 4. Code v1 legacy còn trong repo — KHÔNG dùng, chỉ để import không vỡ — **ĐÃ XOÁ ở D3**

Scaffold task 0.1 là kiến trúc ORM/SQLite hoàn toàn khác (không phân tầng bronze/silver).
Từ task 0.4 trở đi, mỗi lần một module v2 (Core, không ORM) thay thế module v1 cùng tên
chức năng, phần v1 bị tách sang file `legacy_*.py` hoặc giữ nguyên KHÔNG SỬA, chỉ để các
import chưa dọn không vỡ. Danh sách dưới đây (+ 2 file phát hiện thêm khi làm D3, xem mục
10) đã xoá hết — mục này giữ lại làm lịch sử, KHÔNG còn đúng với code hiện tại.

- `src/intel_bot/db/models.py`, `db/repositories.py`, `db/session.py` — ORM, bảng phẳng
  không có schema bronze/silver/gold, không ai gọi từ CLI.
- `src/intel_bot/jobs/ingest_job.py`, `jobs/filter_job.py` — orchestrator v1, không còn
  trong `cli.py`.
- `src/intel_bot/ingest/legacy_rss.py`, `ingest/legacy_normalizer.py` — hàm đồng bộ cũ,
  chỉ còn được `reddit_fetcher.py`/`github_fetcher.py`/`github_trending_fetcher.py`
  (cũng legacy, chưa có bản v2) import.
- `src/intel_bot/filter/legacy_keyword_filter.py`, `filter/embedding_filter.py` — filter
  kiểu industry-group v1 + embedding Phase 2 (chưa tới lượt theo §9.1).
- `src/intel_bot/score/openai_client.py` — client OpenAI kiểu SDK cũ (`openai<1.0`,
  không tương thích `openai>=1.0.0` đã pin trong `pyproject.toml`). KHÔNG được `ruff
  format` cả thư mục `score/` — sẽ format nhầm file này (đã xảy ra 2 lần, phải
  `git checkout` lại). Format từng file cụ thể, không format nguyên thư mục. **Gotcha này
  hết áp dụng từ D3** (file đã xoá) nhưng vẫn giữ thói quen format từng file, không format
  nguyên thư mục `score/` (quy tắc chung, không riêng gotcha cũ).

## 5A. Đã làm — 0.10 (dbt: staging + intermediate + marts)

`dbt_project/` (mục 6 của plan) đã chạy thật trên DB dev — `dbt build --full-refresh` sạch
100% (44/44: 1 seed, 1 snapshot, 3 view, 2 table, 2 incremental, 35 data test).

- **Staging** (`stg_articles`, `stg_article_scores`, `stg_sources`) — view, chỉ đổi tên cột
  + ép kiểu, đọc từ `source('silver', ...)`. `stg_sources` đọc từ seed (xem dưới).
- **Seed + snapshot cho `dim_source`**: `dbt_project/seeds/seed_sources.csv` nạp nguyên văn
  `config/sources.yaml` (industries lưu dạng chuỗi `"ai|tech"`, tách bằng
  `string_to_array` ở `stg_sources`). `snapshots/snap_sources.sql` (strategy `check` trên
  `tier/is_enabled/industries`) → `marts/dim_source.sql` đổi tên cột dbt chuẩn
  (`dbt_scd_id`→`source_key`, `dbt_valid_from/to`→`valid_from/to`) sang tên nghiệp vụ §5.6.
- **`int_articles_deduped`** (ephemeral) — dedup cấp 2 theo `content_hash`, TÍNH CẢ
  composite_score ở đây (không phải ở `fct_article_score`): việc chọn "người thắng" của
  dedup phụ thuộc trực tiếp vào composite, nên phải tính trước khi lọc. Vì vậy model
  intermediate này `ref()` tới `dim_source` (marts) — lệch thứ tự thư mục gợi ý ở §6 nhưng
  không tạo vòng lặp DAG (dim_source không phụ thuộc gì vào articles/scores). Có log số
  nhóm content_hash bị trùng qua `run_query()` + `log()` mỗi lần build.
- **`fct_article_score`** — incremental/merge trên `score_id`, lookback 3 ngày qua
  `dbt_project.yml` var `fct_article_score_lookback_days`, hỗ trợ `--vars
  '{run_date: YYYY-MM-DD}'`. Đã verify: chạy 2 lần liên tiếp → `MERGE 57` (số dòng không đổi).
- **`mart_daily_digest`** — join `fct_article_score` + `stg_articles` + `dim_source`, cửa sổ
  `digest_window_hours` (var), sort composite giảm dần, `industry_group` = tag đầu tiên của
  `industry_tags` (một dòng một bài — không explode theo tag).
- **`mart_pipeline_health`** — incremental/merge trên `pipeline_date`. **Quyết định đáng
  chú ý:** `pipeline_date` là trục lịch chung (union `ingest_date`, `first_seen_date`,
  `scored_at::date`, quarantine `created_at::date`, `fetch_date`), KHÔNG chỉ riêng
  `ingest_date` — vì §7.3 nói rõ bài ingest chiều hôm trước có thể chấm điểm sáng hôm sau.
- Toàn bộ trọng số/ngưỡng/cửa sổ nằm ở `dbt_project.yml` → `vars:` (đã tự verify: đổi
  `composite_weight_importance` từ 0.40 lên 0.80 rồi `dbt run --full-refresh` → composite
  đổi theo, revert lại thấy đổi ngược — không sửa dòng SQL nào).
- Test: 5 schema-level nhóm (`unique`/`not_null`/`accepted_values`/`relationships`) +
  5 singular test theo đúng §13.2 (`assert_score_range`, `assert_no_future_published_at`,
  `assert_digest_not_empty`, `assert_no_orphan_scores`) + 1 singular bổ sung
  `assert_dim_source_single_current` (§13.1 yêu cầu "đúng 1 dòng is_current mỗi source_id"
  — không biểu diễn được bằng generic schema test nên viết singular).
  `assert_summary_five_bullets` để lại Phase 1 như plan cho phép (chưa có model summary).

**Hai lỗi phát hiện khi chạy thật trên dữ liệu thật (không phải giả định lúc viết SQL):**
1. `mart_pipeline_health.source_fail_count` ban đầu đếm SAI: coi HTTP 304 (Not Modified —
   phản hồi ĐÚNG của conditional GET §8.1) là lỗi. Sửa: fail = có `error_message` hoặc
   `http_status >= 400`. Nếu thấy `source_fail_count` bất thường cao ở lần chạy đầu tiên
   sau khi sửa lại logic này, kiểm tra lại điều kiện, đừng lặp lại lỗi cũ.
2. `generate_schema_name` mặc định của dbt nối `<target_schema>_<custom_schema>` — vì
   profile target schema và `+schema:` project đều là `gold`, mọi thứ ban đầu bị ghi vào
   `gold_gold` thay vì `gold`. Đã ghi đè macro ở `macros/get_custom_schema.sql` để dùng
   đúng schema đã khai. Nếu sau này thêm schema khác cho dbt, sửa macro này trước.

**sqlfluff:** `.sqlfluff` ở repo root, `templater = dbt`, `dialect = postgres`,
`max_line_length = 120` (comment tiếng Việt tự nhiên dài hơn 80 — không ép ngắt dòng comment
giữa chừng). Vài cột nghiệp vụ thật trùng từ khoá SQL (`depth`, `domain`) → giữ tên, đánh
dấu `-- noqa: RF04` kèm giải thích tại chỗ thay vì đổi tên (đổi sẽ lệch schema/contract).
Alias join đổi từ `source`→`src` ở vài chỗ vì `source` cũng là từ khoá.

**Sự cố môi trường đáng ghi lại — RACE CONDITION khi chạy song song nhiều `uv add`/`uv
sync`:** giữa lúc làm task này, hai lệnh `uv add` chạy nền chồng lên nhau (một cho
`dbt-core`/`dbt-postgres`, một cho `sqlfluff`/`sqlfluff-templater-dbt`) đã ghi đè lẫn nhau
lên `pyproject.toml` — kết quả cuối cùng mất TOÀN BỘ dependency gốc (`feedparser`,
`SQLAlchemy`, `alembic`, `pydantic`, `typer`, ... và cả `[project.scripts]`,
`[tool.mypy]`, `[tool.pytest.ini_options]`), phát hiện được vì `uv sync` sau đó tự uninstall
`dbt-postgres`. Đã khôi phục thủ công từ nội dung đã đọc đầu phiên, `uv lock` + `uv sync`
lại, và xác nhận `pytest tests/` vẫn 213 pass + `dbt build` vẫn sạch sau khi khôi phục.
**Bài học: KHÔNG chạy nhiều `uv add`/`uv sync` đồng thời trên cùng một venv — chúng cùng
đọc-sửa-ghi `pyproject.toml`/`uv.lock` không khoá lẫn nhau.** Chạy tuần tự.

**Đã CHỦ ĐỘNG KHÔNG làm** (nằm ngoài 8 mục nhiệm vụ chi tiết được giao cho 0.10, dù
mục 5 cũ của file này — nay đổi thành 5A — từng gợi ý làm cùng lúc): xoá
`src/intel_bot/score/composite.py` và sửa `runner.py::_summarize_top_k()` để đọc composite
từ `gold.fct_article_score` thay vì `compute_composite_score()`. Lý do hoãn: việc này đòi
hỏi `dbt run` phải chạy xen giữa bước chấm điểm và bước chọn top-K tóm tắt trong cùng một
lần `score` — một thay đổi luồng orchestration ngoài phạm vi 8 mục được giao, và task 0.12
(Dagster) mới là chỗ luồng này được thiết kế lại đúng cách. `composite.py` vẫn đang là
TẠM THỜI theo đúng docstring của nó — việc dọn dẹp này nên là một phần của task kế tiếp
đụng tới `runner.py` (0.11 hoặc 0.12), không phải bị lờ đi.

## 5B. Đã làm — 0.11 (publish JSON + HTML)

`src/intel_bot/publish/` (digest_reader.py, json_exporter.py, html_renderer.py, runner.py)
+ `templates/index.html.j2`. Chạy thật: `uv run python -m src.intel_bot.cli publish` ra
`docs-site/index.html` (12 bài — xem lỗi mock bên dưới, ban đầu là 24), `docs-site/articles.json`,
`docs-site/archive/2026-08-11.json`, cập nhật `last_published_at`. Chạy 2 lần liên tiếp →
cả 3 file giống hệt theo SHA-256 (đã tự verify bằng CLI thật, không chỉ test).

**BUG phát hiện SAU khi báo "xong 0.11", do người dùng nhìn trang thật thấy chữ lạ:**
mở `docs-site/index.html` thấy bullet kiểu *"Gạch đầu dòng giả lập số 0 cho digest
197197197197"* và *"Lý do đáng chú ý giả lập, đủ dài để qua ràng buộc 20-300 ký tự"* — đây
là text placeholder CỨNG của `MockProvider` (`src/intel_bot/score/providers/mock.py`,
dùng khi `score --provider mock` lúc phát triển/test task 0.8, không tốn tiền), không phải
tóm tắt AI thật. Gốc rễ: DB dev có 57/69 điểm và 15/27 tóm tắt do `mock` sinh (chạy trong
lúc phát triển 0.8/0.9, không phải deepseek) trên CHÍNH những bài thật đã ingest — không
phải dữ liệu test riêng biệt — nên khi 0.10/0.11 gom "bản mới nhất" cho mỗi bài, một số bài
thật vô tình nhận điểm/tóm tắt giả lập vào thẳng `gold`, rồi publish thẳng lên trang công
khai. Đây là lỗi dữ liệu-lẫn-vào-gold, không phải lỗi logic render của Python (publish chỉ
hiển thị đúng những gì `mart_daily_digest` đưa).

**Fix (dbt, đúng P5 — không lọc trong Python):** thêm var
`non_production_model_names: ["mock"]` (`dbt_project.yml`) + macro `is_production_model()`
(`macros/scoring.sql`), áp dụng ở `where` của CẢ `int_scores_latest` LẪN
`int_summaries_latest` — loại các dòng model test/CI TRƯỚC khi xếp hạng "bản mới nhất". Một
bài chỉ có điểm/tóm tắt từ `mock` giờ coi như CHƯA được chấm/tóm tắt thật, không vào
`fct_article_score`/`mart_daily_digest` nữa. Hệ quả sau `dbt build --full-refresh`:
`fct_article_score` 57→12 dòng, `mart_daily_digest` 24→12 dòng — **tất cả 12 bài còn lại
đều do `deepseek-v4-flash` chấm và tóm tắt (đã verify: 0 kết quả `LIKE '%giả lập%'` trong
`gold.mart_daily_digest` và trong `docs-site/articles.json` sau khi publish lại)**. 12 vẫn
≥ 10 theo DONE WHEN gốc của 0.11, nên không cần chạy `--provider deepseek` thêm (tốn tiền
thật) chỉ để bù số lượng.

**Bài học cho task sau:** `score --provider mock` là để test CODE (không tốn tiền, xác
định), KHÔNG phải để tạo dữ liệu digest xem thử — chạy nó trên bài thật rồi quên không lọc
lại là cách dữ liệu giả lọt ra production. `non_production_model_names` trong
`dbt_project.yml` giờ là hàng rào chung cho toàn bộ gold; nếu sau này thêm provider test
khác (không chỉ 'mock'), thêm tên vào var đó, không cần sửa SQL.

**Phát hiện quan trọng khi bắt đầu 0.11: `gold.mart_daily_digest` (làm ở 0.10) THIẾU tóm
tắt tiếng Việt.** Task 0.10 chỉ làm staging cho `stg_articles`/`stg_article_scores` —
`silver.article_summaries` chưa có staging/join nào ở gold. Nhưng card công khai (§12.4)
bắt buộc có 5 bullet + "tại sao quan trọng", và rào chắn 0.11 cấm publish đọc bảng nào khác
ngoài `mart_daily_digest`. Không có cách nào thoả cả hai nếu không sửa dbt trước. Đã bổ
sung (dbt, không phải Python — đúng P5):
- `stg_article_summaries.sql` (staging, view) + `int_summaries_latest.sql` (intermediate,
  ephemeral, lấy summary mới nhất theo article_id — cùng mẫu `int_scores_latest`).
- `mart_daily_digest` đổi từ LEFT sang **INNER JOIN** summary: một bài CHƯA có tóm tắt (chưa
  vào top-K, §4.4) là bài CHƯA sẵn sàng publish — đây là quy tắc "bài nào được xuất bản",
  cùng nhóm quyết định với dedup/ranking nên thuộc dbt. Hệ quả: `mart_daily_digest` giảm từ
  57 dòng (0.10) xuống 24 dòng (chỉ còn bài đã có summary) — vẫn ≥ 10 theo DONE WHEN.
- Thêm cột `digest_built_at` (= `current_timestamp` tại thời điểm dbt build, giống nhau ở
  mọi dòng trong cùng một lần build) — dùng làm "thời điểm chạy pipeline" ở header (§12.4)
  mà KHÔNG cần đọc `mart_pipeline_health` (bảng khác, bị rào chắn cấm). Đây cũng chính là
  cách publish chạy 2 lần vẫn ra file giống hệt: `digest_built_at` cố định cho tới lần
  `dbt run` kế tiếp, không phải `datetime.now()` của Python.
- Thêm singular test `assert_summary_five_bullets` (§13.2, để lại từ 0.10 vì lúc đó chưa có
  model summary — nay có nên bổ sung luôn). `dbt build` hiện 54/54 pass (từ 44 ở 0.10).

**Quyết định thiết kế đáng chú ý khác:**
- `fetch_digest_rows()` chạy `SELECT * FROM gold.mart_daily_digest` — **KHÔNG** thêm
  `ORDER BY` (rào chắn: không sắp xếp trong Python). Dựa vào thứ tự vật lý của bảng do
  `dbt run` build lại bằng CTAS (ổn định giữa các lần SELECT liên tiếp khi không có
  UPDATE/DELETE xen giữa) — đã tự verify bằng CLI thật (2 lần chạy liên tiếp cho cùng
  file), nhưng đây là giả định cần nhớ nếu sau này có gì UPDATE/DELETE trực tiếp lên
  `gold.mart_daily_digest` ngoài `dbt run`.
- CLI `publish --date` KHÔNG lọc lại `gold.mart_daily_digest` (mart tự quyết cửa sổ 48h khi
  build) — chỉ dùng để đặt tên file archive + hiển thị "Ngày" ở header. `now` (thời điểm
  thật) tách riêng, chỉ dùng cho `UPDATE last_published_at` — nhờ tách hai giá trị này mà
  chạy publish nhiều lần trong cùng ngày với `now` khác nhau vẫn ra file giống hệt (test
  `test_run_publish_twice_produces_identical_files` cố tình dùng `now` khác nhau ở 2 lần
  gọi để verify điều này).
- Nút phản hồi Phase 0 theo đúng §12.3 "phương án đơn giản nhất": copy `article_id` vào
  clipboard qua `navigator.clipboard`, fallback `window.prompt` nếu trình duyệt chặn — KHÔNG
  có 👍/👎 (khác bảng ở §12.4, task giao rõ dùng phương án Phase 0 của §12.3).
- Checkbox "đã đọc" lưu ở `localStorage` (key `intelBotReadArticles`), không có phần nào
  gọi network — đúng "trang tĩnh, không backend, không gọi API bên ngoài".
- `config/app.yaml` có thêm `publish.repo_url` (lấy từ `git remote -v` thật, không bịa),
  `publish.docs_site_dir`, `publish.templates_dir`. `publish.window_hours`/`archive_days`
  giữ lại chỉ để tài liệu hoá — cửa sổ 48h thật nằm ở dbt var `digest_window_hours`.
- `jinja2` được thêm THẲNG vào `pyproject.toml` (trước đó chỉ là transitive dependency qua
  `typer`/`dbt-core`, không khai báo trực tiếp dù đã dùng ngầm) — AGENTS.md đã liệt kê
  Jinja2 trong stack nên không cần dừng lại hỏi.

**Test:** `tests/test_publish_html_renderer.py` (7 test thuần, không cần Postgres — số card,
escape HTML/XSS, giữ nguyên tiếng Việt có dấu, 5 bullet, checkbox/copy button, footer),
`tests/test_publish_json_exporter.py` (4 test thuần — cấu trúc payload, JSON valid, không bị
escape `\uXXXX`), `tests/test_publish_runner.py` (5 test tích hợp Postgres thật — chèn thẳng
1 dòng vào `gold.mart_daily_digest` không qua dbt, dọn dẹp ở fixture, gồm cả test hash-giống-
hệt chạy 2 lần với `now` khác nhau). 229 test tổng (213 cũ + 16 mới), `ruff`/`mypy --strict`
sạch trên toàn bộ `src/intel_bot/publish/` + `cli.py` + 3 file test mới.

**Cố ý chưa làm** (đúng phạm vi "CHỈ thực hiện task 0.11"): chưa xoá `composite.py`/sửa
`runner.py::_summarize_top_k()` như 5A đã ghi — việc này vẫn hoãn tới 0.12 (Dagster), vì lý
do đã nêu ở 5A không đổi (ĐÃ TRẢ ở D1, mục 9). Cũng chưa làm: git commit/push `docs-site/` (không được giao),
CI ping heartbeat (§7.5, ngoài phạm vi Phase 0 theo AGENTS.md), archive pruning >7 ngày
(DONE WHEN không yêu cầu, `archive_days` trong config hiện chỉ mang tính tài liệu).

## 5C. Đã làm — 0.12 + 0.13 (Dagster asset graph + schedule + heartbeat)

`dagster_project/` (definitions.py, partitions.py, schedules.py, assets/{bronze,silver,
dbt_assets,serve}.py, resources/{postgres,llm,notifier}.py) — 18 asset, chạy thật qua
`dagster dev` VÀ qua `dagster asset materialize` CLI (không chỉ import thành công — đã
`RUN_SUCCESS` 3 lần thật trên DB dev, xem "Verify thật" bên dưới).

**Lệch khỏi chữ đúng của đề bài — quan trọng nhất:** đề bài yêu cầu nạp dbt bằng
`load_assets_from_dbt_project`. Hàm này ĐÃ BỊ GỠ khỏi `dagster-dbt` hiện hành — verify bằng
`ImportError` thật (`dagster==1.13.17`, `dagster-dbt==0.29.17`, bản mới nhất `uv add` cài
được lúc làm task), không đoán. API thay thế CHÍNH THỨC (không phải tôi tự chọn) là
decorator `@dbt_assets` + `DbtProject` + `DbtCliResource`. Logic transform vẫn 100% do
dbt/SQL quyết định — Python chỉ gọi `dbt build` qua subprocess, đúng tinh thần "KHÔNG viết
lại logic dbt trong Python".

**Asset không có trong bảng gốc §7.2 — bổ sung có chủ đích:** `articles_normalized` (Python,
silver, daily). Bảng asset trong đề bài đi thẳng `raw_rss → stg_articles → articles_filtered`,
nhưng `stg_articles` (dbt) là VIEW chỉ đổi tên cột (§11.2, task 0.10) — dedup cấp 1 + chuẩn
hoá URL + cold-start (§8.2) là business logic THẬT đã có sẵn từ task 0.5 ở
`normalize_partition()`, không thể nhét vào view và không thể bỏ qua (không có nó thì
`silver.articles` — nguồn của view — luôn rỗng). Đã cân nhắc dừng lại hỏi trước khi thêm,
nhưng đây là khoảng trống cơ học của bảng rút gọn trong đề bài (tương tự việc phải thêm
`stg_article_summaries` ở 0.11), không phải một lựa chọn sản phẩm — nên làm luôn và ghi rõ
ở đây, giống cách đã xử lý khoảng trống tương tự ở 0.11.

**5 lỗi/API-gap phát hiện khi CHẠY THẬT (không phải đoán trước), theo đúng thứ tự gặp phải:**

1. **`from __future__ import annotations` phá vỡ kiểm tra kiểu của dagster.** Asset có tham
   số `context: AssetExecutionContext` báo lỗi sai be bét ("context phải là
   AssetExecutionContext...") dù đúng kiểu — dagster so khớp class trực tiếp trên
   `inspect.signature`, không resolve forward-ref chuỗi mà future-import tạo ra. Đã tự verify
   bằng cách bật/tắt dòng import và chạy lại. **Toàn bộ file asset trong `dagster_project/`
   vì vậy KHÔNG có `from __future__ import annotations`** — khác quy ước còn lại của repo,
   ghi rõ trong docstring từng file.
2. **Asset key dbt mặc định có prefix schema** (`gold/stg_articles` thay vì `stg_articles`)
   → không khớp `deps=["stg_articles"]` mà asset Python khai, tạo ra node mồ côi song song
   với node thật. Sửa bằng `DagsterDbtTranslator.get_asset_key()` tuỳ biến, bỏ hẳn schema
   cho model/seed/snapshot.
3. **dagster-dbt cấm hai `source()` khác nhau trỏ cùng một asset key.** Định ánh xạ cả
   `score_quarantine` lẫn `source_health` về chung key với `article_scores`/`raw_rss` (vì
   đúng là 2 asset đó ghi 2 bảng kia thật) → `DagsterDbtTranslator` bị Dagster báo lỗi ngay
   khi import. Đành để 2 source đó KHÔNG override (thành asset "external" riêng, vẫn hiện
   trên đồ thị, chỉ không gộp định danh).
4. **`internal_asset_deps` của `@multi_asset` một khi đã dùng thì phải liệt kê ĐỦ mọi input
   cho MỌI output** — không tự suy ra phần còn lại từ `deps=` chung. `article_scores_and_
   summaries` (multi_asset gộp `run_score_partition()` — hàm này chấm điểm VÀ tóm tắt top-K
   trong CÙNG một lần gọi, task 0.8 — thành 2 asset riêng cho đúng bảng §7.2) ban đầu thiếu
   `articles_filtered` trong `internal_asset_deps` của output `article_scores` → CheckError.
5. **`DailyPartitionsDefinition` mặc định KHÔNG coi "hôm nay" là partition hợp lệ** cho tới
   sau nửa đêm hôm sau (`end_offset=0`: partition cuối phải kết thúc trước "now"). Vì lịch
   chạy 05:00 cần materialize được partition "hôm nay" NGAY TRONG NGÀY đó, đã thêm
   `end_offset=1` (`partitions.py`) — không có nó, `dagster asset materialize --partition
   <hôm nay>` báo lỗi "must have a PartitionsDefinition containing the passed partition key".

**Quyết định thiết kế đáng chú ý khác:**
- **`LLM_PROVIDER` bắt buộc, KHÔNG default** (`dagster_project/resources/llm.py`) — khác
  CLI (`--provider` default `mock`). Lịch 05:00 chạy không người xem; bài học trực tiếp từ
  §5B (mock từng lọt vào gold vì chạy trên bài thật rồi quên đổi provider) — để asset tự
  default về mock khi quên cấu hình là lặp lại đúng lỗi đó với một schedule TỰ ĐỘNG, rủi ro
  cao hơn hẳn một lần gõ lệnh tay. `PostgresResource`/`NotifierResource` vẫn có default rỗng
  (không nguy hiểm nếu thiếu — lỗi rõ ràng hoặc bỏ qua có log, không âm thầm sai dữ liệu).
- **Metadata P6** (`rows_inserted`/`eligible`/`scored`/`cost_usd`/`latency_p50_95`/
  `duration_seconds`, v.v.) ghi qua `MaterializeResult(metadata=...)` ở mọi asset Python;
  asset dbt tự có test/check riêng qua `dbt build` (đã hiện trong Dagster UI dưới dạng Asset
  Check, không phải tôi tự thêm).
- **`mart_pipeline_health.sql` thêm hỗ trợ `--vars run_date`** (trước đó chỉ
  `fct_article_score` có) — đồng bộ để asset `mart_pipeline_health` materialize được đúng
  MỘT partition khi cần, không chỉ dựa vào lookback mặc định.
- **`--select "*"` không dùng được trên Windows** khi gọi `dagster asset materialize` qua
  CLI — Click tự glob `*` thành danh sách file trong thư mục hiện tại (đặc thù Windows, đã
  verify bằng lỗi thật "Got unexpected extra arguments" liệt kê nguyên thư mục repo). Dùng
  danh sách tên asset tường minh, phân tách dấu phẩy (xem README).
- Resource `notifier` chỉ dùng cho heartbeat ở Phase 0 (không có sensor nào khác cần tới —
  đúng rào chắn "KHÔNG cài sensor ở bước này").
- `uv add dagster dagster-webserver dagster-dbt` kéo `dbt-core` xuống 1.11.12 (từ 1.12.0 ở
  0.10/0.11, do ràng buộc phiên bản của `dagster-dbt`) — đã verify `dbt build` + `sqlfluff`
  vẫn sạch 100% sau khi hạ phiên bản, không cần sửa SQL nào.

**Verify THẬT trên DB dev (không chỉ đọc log, đã tự chạy):**
- `dagster dev -f dagster_project/definitions.py` → HTTP 200, GraphQL `assetsOrError` trả
  đúng 18 asset khớp thiết kế — đã tắt server test sau khi verify.
- Materialize toàn đồ thị cho partition `2026-08-11` (hôm nay) → `RUN_SUCCESS`, heartbeat
  `200 OK` thật tới URL healthchecks.io do người dùng cung cấp.
- Materialize LẠI cùng partition `2026-08-11` → `RUN_SUCCESS` lần 2, heartbeat `200 OK` lần
  2 — đếm lại mọi bảng (`bronze.raw_articles`, `silver.articles`, `gold.fct_article_score`,
  `gold.mart_daily_digest`, `mart_pipeline_health.ingest_count`) **không đổi một dòng nào**.
- Materialize partition `2026-08-10` (ngày trước, có dữ liệu ingest thật từ trước) →
  `RUN_SUCCESS`, heartbeat `200 OK` lần 3. `fct_article_score`/`mart_daily_digest` không đổi
  (12/12, các bài deepseek thật không bị đụng); `mart_pipeline_health` của 08-10 đổi nhẹ
  (eligible 57→54) vì `normalize`/`filter` so tuổi bài với `datetime.now()` THẬT tại thời
  điểm chạy lại (§8.2) — hành vi đúng thiết kế, không phải lỗi.
- `docs-site/index.html` sau cùng: 12 bài (khớp `gold.mart_daily_digest`), 0 kết quả
  `grep "giả lập"`. `gold.mart_pipeline_health` có dòng `2026-08-11` với `total_cost_usd =
  0.004515` (chi phí deepseek thật từ trước, `mart_pipeline_health` tự nhặt đúng nhờ trục
  lịch chung — xem 5A).
- `ruff check`/`ruff format`/`mypy --strict` sạch trên toàn bộ `dagster_project/`;
  `sqlfluff lint dbt_project --dialect postgres` vẫn sạch; 229/229 pytest cũ không đổi.

**Cố ý chưa làm / biết còn thiếu — nói thẳng, không giấu:**
- **Không có test pytest nào cho `dagster_project/`** — khác với mọi tầng trước (0.10/0.11
  đều có test tự động). Verify hoàn toàn dựa vào chạy `dagster asset materialize`/`dagster
  dev` thật (ghi log ở trên) — đủ để chứng minh DONE WHEN nhưng không có test hồi quy tự
  động nếu code asset bị sửa sai sau này. Đây là khoảng trống thật, không phải lựa chọn có
  chủ đích — nên vá ở task dọn dẹp/Phase 1.
- `raw_github` để lại Phase 1 (rào chắn task 0.12 nói rõ).
- Sensor (`run_failure_sensor`/`freshness_sensor`/`quarantine_sensor`/`cost_sensor`, §7.4)
  và lịch 12:00/18:00 bổ sung (§7.3) — cả hai đều bị rào chắn task 0.12 cấm làm ở bước này.
- Chưa triển khai Dagster daemon cho production (chạy schedule 05:00 thật mỗi ngày không
  người canh) — Phase 0 chỉ cần chạy được qua `dagster dev`, triển khai production nằm
  ngoài phạm vi.
- Chưa xoá `src/intel_bot/score/composite.py`/sửa `runner.py::_summarize_top_k()` (nợ từ
  5A) — task 0.12 không đụng tới luồng orchestration của lệnh `score` (asset gọi lại
  nguyên hàm `run_score_partition()`), nên nợ này VẪN CÒN, chưa có task nào giao dọn nó.
  **ĐÃ TRẢ ở D1 (mục 9) — xem chi tiết ở đó**, kể cả việc tách `article_scores_and_summaries`
  (multi_asset) thành hai asset `article_scores`/`article_summaries` rời nhau mà đoạn này
  chưa lường trước.

## 6. Trạng thái DB dev hiện tại (lúc viết file này — sẽ lạc hậu, tự query lại)

```
bronze.raw_articles: 174       (ingest_date 2026-08-10 + 2026-08-11 — 0.12 materialize thật ingest thêm)
silver.articles: 115
silver.article_scores: 88      (76 provider=mock cost=0, 12 provider=deepseek-v4-flash chi phí thật)
silver.article_summaries: 45   (33 provider=mock, 12 provider=deepseek-v4-flash)
silver.score_quarantine: 0
gold.dim_source: 8             (SCD2 từ snap_sources, tất cả is_current=true — chưa có đổi tier)
gold.fct_article_score: 12     (loại model 'mock' khỏi gold, xem 5B — không đổi qua các lần materialize 0.12)
gold.mart_daily_digest: 12     (INNER JOIN summary + loại 'mock' — chỉ còn bài có tóm tắt THẬT)
gold.mart_pipeline_health: 2   (2026-08-10: ingest/filter đã chạy lại qua Dagster; 2026-08-11: total_cost_usd=0.004515 thật)
docs-site/index.html, articles.json, archive/2026-08-11.json  (sinh thật bởi asset published_site, 12 bài, 0 nội dung mock)
```
Dữ liệu deepseek là request thật, tốn tiền thật (rất nhỏ, ~$0.008, không tăng thêm ở 0.12 —
`LLM_PROVIDER=mock` trong `.env` khi test Dagster). Đừng chạy lại `--provider deepseek`/
`LLM_PROVIDER=deepseek` trên diện rộng chỉ để test — dùng mock. **Nhưng đừng chạy mock trên
bài thật rồi để nguyên trong DB nếu định publish thử** — mock đã bị loại khỏi gold (var
`non_production_model_names`) nên không lộ ra trang công khai, nhưng vẫn tốn công `dbt run`
vô ích và làm nhiễu `silver.article_scores`. `bronze.raw_articles`/`silver.articles` tăng
đáng kể so với các mốc trước — do task 0.12 tự materialize `raw_rss` thật (ingest RSS thật)
nhiều lần để test idempotency/backfill, không phải lỗi.

## 7. Config đã điền thật (không phải placeholder)

| File | Trạng thái |
|---|---|
| `config/models.yaml` | provider `deepseek` có giá thật (xác minh 2026-08-11), `ollama`/`openai` chưa dùng tới |
| `config/sources.yaml` | 8 nguồn RSS đã verify HTTP 200 thật |
| `config/rubric.yaml` | Rubric 4 tiêu chí, mốc 1/5/10 |
| `config/keywords.yaml` | `blocklist:` (v2, đang dùng) + `groups:` (v1 legacy) |
| `.env` | `DATABASE_URL`, `DEEPSEEK_API_KEY` thật + `LLM_PROVIDER=mock` + `HEARTBEAT_URL` thật (healthchecks.io, người dùng cung cấp — task 0.13). Gitignored — `.env.example` chỉ có placeholder rỗng/an toàn |
| `config/app.yaml` | `publish.repo_url` = URL git remote thật (task 0.11); `publish.docs_site_dir`/`templates_dir` |

## 8. Test

229 test Python (213 + 16 ở task 0.11), `uv run pytest tests/` — tất cả dùng Postgres THẬT
(docker, cổng 5435) cho phần integration, KHÔNG mock DB; chỉ mock mạng (`httpx.MockTransport`
hoặc `MockProvider`). Không cần biến môi trường nào để chạy phần contract/mock (task 0.7 trở
đi tự chứng minh bằng `env -i`). `ruff`/`mypy --strict` chỉ chạy sạch trên file đã viết ở
task 0.2 trở đi — code legacy (mục 4) còn nợ lint, không nằm trong phạm vi bất kỳ task nào.

Riêng dbt (task 0.10 + 0.11 + 0.12): 54 data test qua `dbt test`/`dbt build` (`--project-dir
dbt_project`, cần `DBT_PROFILES_DIR=dbt_project` hoặc `--profiles-dir dbt_project`) — độc
lập với 229 test Python ở trên, không chạy qua `pytest`. `sqlfluff lint dbt_project
--dialect postgres` sạch (macro bị `sqlfluff-templater-dbt` tự skip — giới hạn đã biết của
templater, không phải lỗi).

Riêng `dagster_project/` (task 0.12+0.13): **KHÔNG có test pytest** (xem 5C "cố ý chưa
làm") — verify bằng chạy thật (`dagster dev`, `dagster asset materialize`) chứ không phải
bằng bộ test tự động. `ruff check`/`ruff format`/`mypy --strict` sạch trên toàn bộ
`dagster_project/`.

## 9. Đã làm — Vệ sinh repo (D8–D12, giữa Phase 0 và task dọn nợ kỹ thuật D1–D4)

Task KHÔNG đụng logic — chỉ dọn những gì một người ngoài mở repo lần đầu sẽ thấy trước tiên.

**D8 — Gỡ rác runtime khỏi git (`git rm --cached`, không xoá khỏi đĩa):**
`.tmp_dagster_home_t1amzuw0/history/runs.db`, `.../history/runs/.db`,
`.../history/runs/index.db`, `.../schedules/schedules.db`, `data/dev.db`,
`data/rss_articles.db`, `data/test_ingest.db`, `logs/app.log` — 8 file, tất cả vẫn còn
trên đĩa, chỉ gỡ khỏi index.

**Kiểm tra secret trước khi gỡ (bắt buộc theo yêu cầu task) — KẾT QUẢ: KHÔNG có secret,
đã tiến hành gỡ:**
- 4 file Dagster: chỉ có 1 commit lịch sử (`3ba8754`) chạm tới các đường dẫn này — dump
  từng file ở HEAD bằng `git show`, đọc bằng `sqlite3` trực tiếp. `runs.db.runs` = 0 dòng
  (không có `run_body`/run config nào từng được ghi); `event_logs` ở 2 file `.db`/`index.db`
  chỉ có 18 dòng `FRESHNESS_STATE_CHANGE`, nội dung message rỗng, JSON event chỉ chứa
  `AssetKey` + timestamp, không có biến môi trường hay run config. `schedules.db` toàn bảng
  rỗng.
- `data/dev.db`, `data/rss_articles.db`, `data/test_ingest.db`, `logs/app.log`: chỉ có 1
  commit lịch sử (`2691690`). Quét byte thô bằng regex cho các mẫu
  `api_key|secret|password|token|DEEPSEEK|sk-|AKIA|...` — có match nhưng khi soi ngữ cảnh
  toàn bộ đều là nội dung bài báo RSS thật (vd. "...your secret weapon for creating better
  content...", "...poached employees to bring over confidential presentations, secret
  prototypes...", các URL chứa chuỗi con "risk-"/"musk-" khớp nhầm pattern `sk-`) — không
  phải giá trị credential thật. `logs/app.log` không match gì cả.
- **Kết luận: KHÔNG dừng lại, đã `git rm --cached` cả 8 file như kế hoạch.** Không cần viết
  lại git history.

Bổ sung `.gitignore`: `.env.*` (kèm `!.env.example`), `*.db` (phạm vi TOÀN repo, không chỉ
`data/*.db` — lý do: kiến trúc đã chốt là PostgreSQL, AGENTS.md mục 2; không có `.db` nào
trong repo là nguồn sự thật hợp lệ, mọi `.db` từng thấy đều là SQLite rác của scaffold v1
hoặc runtime state Dagster, nên ignore theo phần mở rộng là an toàn — xoá luôn dòng
`data/*.db` cũ vì đã bị `*.db` bao trùm), `.tmp_dagster_home*/`, `dagster_home/`.

**D9 — Xoá rác v1 không nằm trong danh sách legacy đã biết (mục 4):**
- `my_postgres_project/` — dbt project mẫu (`dbt init`) ở gốc repo. Grep xác nhận
  `my_postgres_project` chỉ tự tham chiếu trong `my_postgres_project/dbt_project.yml`
  của chính nó, không nơi nào khác trong repo (kể cả `dbt_project/`) trỏ tới. Đã xoá.
- `main.py`, `feeds.csv`, `github_repos.csv` ở gốc — scaffold v1. Grep xác nhận không còn
  import/tham chiếu (chỉ `scripts/fetch_rss.py`, cũng bị xoá cùng lượt, còn đọc
  `feeds.csv`). Đã xoá.
- `scripts/fetch_rss.py`, `init_db.py`, `seed_sources.py`, `show_data.py` — đã grep từng
  file: không có console-script (`pyproject.toml [project.scripts]` chỉ có `intel-bot`),
  không file nào khác trong `src/`, `tests/`, `dagster_project/` import chúng. 3/4 file tự
  import các module legacy ở mục 4 (`db.session`, `db.models`, `jobs.ingest_job`) —
  nhưng bản thân các script này không ai gọi. Đã xoá cả 4 (thư mục `scripts/` giờ rỗng).
- `spike/spike.py` — GIỮ theo yêu cầu (giá trị lịch sử Phase −1). Thêm đoạn ghi rõ ngay đầu
  docstring: "CODE SPIKE MỘT LẦN — KHÔNG THUỘC PIPELINE CHÍNH THỨC", không CLI/asset nào
  gọi tới, không bảo trì theo chuẩn code pipeline.

**D10 — Sửa khai báo dependency:**
- `pydantic>=1.10.2` → `pydantic>=2,<3` (`pyproject.toml`) — khớp AGENTS.md mục 2
  ("Pydantic v2"); `uv.lock` đã khoá 2.13.4 từ trước nhưng ràng buộc cũ vẫn cho phép
  resolve về v1, sẽ vỡ toàn bộ tầng contract nếu ai đó `uv lock` lại trên máy khác.
- `requests`, `beautifulsoup4`, `openai`: grep xác nhận CHỈ còn code legacy import —
  `score/openai_client.py` (`openai`), `ingest/legacy_rss.py` + `ingest/github_fetcher.py`
  + `ingest/github_trending_fetcher.py` (`requests`; file cuối còn `beautifulsoup4`). Cả 4
  file đều nằm trong danh sách legacy đã biết ở mục 4, không ai gọi từ CLI/Dagster.
  **CHƯA xoá 3 dependency này** — đúng yêu cầu, để dành xoá cùng lúc với D3 (xoá code
  legacy) ở prompt 11, tránh vỡ import trước khi code bị xoá.
- `uv lock` rồi `uv sync` (tuần tự, đúng mục 8) — resolve sạch (180 gói), venv đồng bộ
  lại đúng lock. `uv run pytest tests/` → **229/229 pass**, không hỏng gì.

**D11 — Đổi `AGENTS.MD` → `AGENTS.md`:** `git mv AGENTS.MD AGENTS.tmp && git mv AGENTS.tmp
AGENTS.md` (2 bước, vì Windows không phân biệt hoa/thường). Kiểm tra các tài liệu tham
chiếu cứng khác: `docs/PRODUCTION_PLAN.md`, `docs/PROGRESS.md` đã đúng case sẵn trên đĩa;
grep toàn bộ `README.md` + `docs/*.md` cho tên 3 file này không thấy sai lệch hoa/thường
nào — mọi link README đều mở được.

**D12 — README nói đúng sự thật:** README mở đầu cũ ghi "Minimal scaffold... many commands
are scaffolds" và hướng dẫn `python -m venv` + `pip install -e .` — sai trên cả hai mặt:
mâu thuẫn AGENTS.md mục 2 (bắt buộc `uv`, không `pip` trực tiếp) và sai thực tế (pipeline đã
chạy end-to-end thật từ 0.10-0.13). Đã sửa lại phần mở đầu (giữ nguyên toàn bộ phần Dagster
phía dưới, vốn đã đúng và chi tiết — không viết lại cả file, để dành prompt 22 cấu trúc lại):
- Mô tả đúng pipeline thật (bronze/silver/gold, LLM tiếng Việt, publish tĩnh) thay vì
  "minimal scaffold".
- Quick start đổi sang `uv sync` (không còn `python -m venv`/`pip install -e .`).
- Liệt kê đúng danh sách lệnh CLI đã chạy thật (`ingest`/`validate-sources`/`normalize`/
  `filter`/`score`/`publish`/`doctor`), nêu rõ CHỈ `pipeline`/`eval` còn là placeholder —
  không nói cả bộ CLI là scaffold.
- Thêm ghi chú cổng Postgres 5435 và lỗi `uv run intel-bot` (trỏ sang PROGRESS.md mục 3.1/
  3.4) ngay ở quick start, thay vì để người đọc tự đâm vào 2 cạm bẫy này.
- Verify: `uv run python -m src.intel_bot.cli doctor` chạy thật, kết nối Postgres OK, liệt
  kê đủ 3 schema (bronze 1 bảng, silver 5 bảng, gold 6 bảng). Grep lại README không còn
  `pip install`/`venv`/gọi cả pipeline là "placeholder"/"scaffold" — 2 chỗ còn chứa từ
  "scaffold"/"placeholder" đều đúng nghĩa (khẳng định KHÔNG PHẢI scaffold; nói đúng
  `pipeline`/`eval` là 2 lệnh CHƯA làm, không phải toàn bộ CLI).

**Lệch/ghi chú đáng chú ý:**
- `AGENTS.md` có một thay đổi nội dung CHƯA COMMIT từ trước khi task dọn dẹp này bắt đầu
  (thêm mục 7–9: đọc PROGRESS.md trước plan, bảng cạm bẫy môi trường, quy tắc dữ liệu test)
  — không phải do task này tạo ra. Commit D11 CHỈ đổi tên file (`git mv` không kèm `git add`
  nội dung), nên nó lấy đúng nội dung đã commit gần nhất (6 mục gốc) đặt dưới tên mới — phần
  mục 7–9 vẫn nằm nguyên trong working tree dưới dạng thay đổi CHƯA COMMIT, đúng rào chắn
  AGENTS.md mục 5.3 (không tự sửa/commit nội dung AGENTS.md khi không được yêu cầu rõ ràng).
  Xác nhận bằng `git show HEAD:AGENTS.md` — dừng ở mục 6, không có mục 7–9. Người dùng cần tự
  quyết định có muốn commit phần nội dung đó không.
- `uv sync` sau khi đổi ràng buộc pydantic tiện thể gỡ 4 gói không còn trong lock
  (`ast-serialize`, `dbt-core-experimental-parser`, `metricflow`, `rapidfuzz`) — đây là
  extra của `dbt-core` (semantic layer) từng bị cài lẻ vào venv trước đó nhưng chưa từng có
  trong `uv.lock`/`pyproject.toml`; `uv sync` chỉ đang đồng bộ venv về đúng lock, không phải
  hệ quả của việc đổi ràng buộc pydantic. Đã verify `dbt build`/`pytest` không phụ thuộc
  chúng (229 test vẫn pass).

**Cố ý chưa làm** (ngoài phạm vi D8–D12, để dành D1–D4 hoặc task sau):
- Xoá code v1 legacy (`db/models.py`, `db/repositories.py`, `db/session.py`,
  `jobs/ingest_job.py`, `jobs/filter_job.py`, `ingest/legacy_rss.py`,
  `ingest/legacy_normalizer.py`, `filter/legacy_keyword_filter.py`,
  `filter/embedding_filter.py`, `score/openai_client.py`) và 3 dependency liên quan — nằm
  ở D3 (prompt 11), không phải task này.
- Chưa đụng `.env`/`.env.example` (rào chắn AGENTS.md mục 5.3) ngoài việc mở rộng pattern
  `.env.*` trong `.gitignore`.
- Chưa sửa gốc rễ lỗi `uv run intel-bot` (mục 3.4) — không thuộc phạm vi D8–D12, README chỉ
  ghi rõ workaround.
- README chưa được cấu trúc lại toàn diện (để dành prompt 22 theo đúng yêu cầu D12) — chỉ
  sửa phần mở đầu cho không còn nói sai.

## 10. Đã làm — Dọn nợ kỹ thuật (D1–D4, sau Phase 0, trước Phase 1)

**D1 — Xoá công thức composite trùng lặp (nợ từ 2.3/5A/5C):**

Vấn đề cốt lõi phát hiện khi phân tích (lý do việc này bị hoãn 3 lần): `run_score_partition()`
cũ chấm điểm RỒI NGAY TRONG CÙNG một lần gọi Python đọc lại điểm, tính composite tạm bằng
`compute_composite_score()`, chọn top-K, tóm tắt — không đụng dbt. Để đọc composite từ
`gold.fct_article_score` (§5.7: credibility = 80% source tier + 20% LLM), **dbt phải chạy
XEN GIỮA** hai bước đó — `fct_article_score` cần `article_scores` (Python vừa ghi) +
`dim_source` (SCD2) để tính blended credibility, không có cách nào bỏ qua.

Đã trình bày phương án cho cả hai đường chạy TRƯỚC khi viết code (theo đúng yêu cầu), người
dùng chọn: **một lệnh CLI `score` duy nhất, tự gọi `dbt build` bên trong** (thay vì tách
`score`/`summarize` thành 2 lệnh CLI riêng — phương án bị loại vì thêm bước thủ công dễ quên,
đúng kiểu lỗi mock-lọt-gold đã từng xảy ra ở 5B).

- `src/intel_bot/score/runner.py`: `run_score_partition()` giờ CHỈ chấm điểm (bỏ tham số
  `top_k_summaries`, bỏ lời gọi `_summarize_top_k` nội bộ). Hàm mới
  `run_summarize_top_k_partition()` (public, thay `_summarize_top_k` private cũ) đọc top-K
  qua `load_top_k_from_fct_article_score()` — một câu SQL `ORDER BY composite_score DESC
  LIMIT :k` join `gold.fct_article_score` với `silver.articles` (lấy title/snippet) — KHÔNG
  tính lại composite trong Python (P5). `ScoredArticle`/`load_scored_articles`/
  `select_top_k_by_composite` (logic Python cũ) đã xoá theo.
- `src/intel_bot/cli.py`: lệnh `score` sau khi chấm điểm — nếu `scored > 0` và không
  `budget_stopped` — gọi `_run_dbt_build_for_fct_article_score()` (subprocess
  `dbt build --select +fct_article_score --vars run_date`, giống hệt cách
  `dagster_project/assets/dbt_assets.py` gọi dbt, P5) rồi mới `run_summarize_top_k_partition()`.
  Không tốn thời gian dbt build khi không có gì mới chấm (rerun trên partition đã xong).
- `dagster_project/assets/silver.py`: multi_asset `article_scores_and_summaries` (dùng
  `internal_asset_deps`, nguồn của lỗi CheckError đã ghi ở 5C) tách thành HAI `@asset` bình
  thường — `article_scores` (`deps=["articles_filtered"]`) và `article_summaries`
  (`deps=["fct_article_score"]`, LỆCH bảng §7.2 gốc ghi phụ thuộc `article_scores` trực
  tiếp — cùng loại khoảng trống rút gọn như `articles_normalized`, không phải lựa chọn sản
  phẩm). Không cần code orchestration gọi dbt thủ công bên Dagster: `fct_article_score` đã
  là asset dbt thật trong `daily_dbt_assets`, dagster-dbt tự subset-chạy đúng lúc (cơ chế đã
  hoạt động từ 0.12 cho `stg_articles` chạy trước `articles_filtered` trong cùng
  `@dbt_assets`, giờ áp dụng thêm cho `fct_article_score` chạy trước `article_summaries`).
  `definitions.py` cập nhật theo (import + danh sách `assets=[...]`).
- Xoá `src/intel_bot/score/composite.py` + `tests/test_composite.py` (11 test).
- Test: viết lại `tests/test_score_runner.py` phần top-K — chèn thẳng fixture vào
  `gold.fct_article_score` (KHÔNG chạy dbt thật, cùng mẫu `tests/test_publish_runner.py` đã
  dùng cho `gold.mart_daily_digest`) để test `run_summarize_top_k_partition()` độc lập với
  dbt build (chậm, cần `DBT_PROFILES_DIR`). `tests/test_cli_score.py` thêm 3 test xác nhận
  dbt build + summarize được/không được gọi đúng theo `scored`/`budget_stopped`.
- **Sự cố môi trường gặp khi verify:** Docker Desktop crash giữa chừng (đúng gotcha đã biết
  ở mục 3.2) — `uv run pytest` treo vô thời hạn (không timeout rõ ràng) vì `engine.connect()`
  cố nối Postgres trong khi Docker daemon đã chết, không phải lỗi do code D1. Khởi động lại
  Docker Desktop + `docker compose up -d postgres` rồi chạy lại là qua.

**D2 — Sửa packaging (nợ từ mục 3.4):** đã cân nhắc 2 hướng, chọn sửa wheel mapping thay vì
đổi import (lý do đầy đủ + chi tiết kỹ thuật đã ghi lại ngay ở mục 3.4, không lặp lại ở đây
để tránh hai nguồn sự thật). Tóm tắt: `pyproject.toml` `packages = ["src"]` (từ
`["src/intel_bot"]`) + `src/__init__.py` rỗng mới. Verify: `uv run intel-bot doctor` chạy
thật, `uv run pytest` 223/223 pass, `mypy --strict`/`ruff check` trên `src/` không sinh lỗi
mới so với trước khi đổi (đã tự verify bằng `git stash -u` so sánh số lỗi trước/sau).

**D3 — Xoá code v1 legacy (nợ từ mục 4):** đã xoá đúng 9 file liệt kê ở mục 4, cộng thêm
2 file phát hiện khi grep xác nhận (không nằm trong danh sách gốc, nhưng cùng loại — chỉ
được import bởi chính code legacy đang xoá):
- `src/intel_bot/ingest/deduplicator.py` — dedup kiểu ORM (`sqlalchemy.orm.Session`,
  `db.models.Article`), chỉ `jobs/ingest_job.py` import. `ingest/normalizer.py` (v2, đang
  dùng thật) có content_hash/dedup riêng, không liên quan file này.
- `src/intel_bot/ingest/source_defaults.py` — `RSS_SOURCES` HARDCODE danh sách URL nguồn
  trong Python, đúng thứ AGENTS.md mục 3 cấm ("Tuyệt đối không hardcode... URL nguồn"). Chỉ
  `ingest/__init__.py` (re-export) và `jobs/ingest_job.py` dùng.

Danh sách đầy đủ đã xoá: `db/models.py`, `db/repositories.py`, `db/session.py`,
`jobs/` (cả thư mục — `ingest_job.py`, `filter_job.py`, `__init__.py`, không còn gì khác bên
trong nên xoá nguyên thư mục), `ingest/legacy_rss.py`, `ingest/legacy_normalizer.py`,
`ingest/reddit_fetcher.py`, `ingest/github_fetcher.py`, `ingest/github_trending_fetcher.py`,
`ingest/deduplicator.py`, `ingest/source_defaults.py`, `filter/legacy_keyword_filter.py`,
`filter/embedding_filter.py`, `score/openai_client.py` — 17 file/1 thư mục.

Sửa (không xoá) `filter/__init__.py` và `ingest/__init__.py` — bỏ import/export của các
module đã xoá, giữ nguyên phần re-export module v2 còn sống (`keyword_filter`;
`normalizer`/`rss_fetcher`). Grep xác nhận trước: không nơi nào trong repo import theo kiểu
`from src.intel_bot.filter import X`/`from src.intel_bot.ingest import X` (package-level) —
chỉ tự các `__init__.py` này dùng — nên sửa an toàn, không có import nào khác vỡ.

**Dọn nốt để `src/` sạch mypy --strict + ruff check TOÀN BỘ (DONE WHEN D3, không chỉ xoá
file):** sau khi xoá legacy, còn 6 lỗi nằm ở 3 file THẬT đang dùng (không phải legacy, không
xoá) — sửa tại chỗ, không đổi hành vi:
- `config.py`: `Settings.__init__` thiếu `-> None`; `load_config_dir()` thiếu annotation cho
  biến `conf`. (`Settings`/`settings` chỉ còn được `observability/logging.py::setup_logging()`
  dùng, và `setup_logging()` hiện KHÔNG ai gọi — có vẻ cũng là code chết, nhưng KHÔNG xoá vì
  không nằm trong danh sách mục 4 và không thuộc phạm vi D3 — chỉ sửa type cho sạch.)
- `observability/logging.py`: `log_event(..., **fields)` thiếu annotation → `**fields: object`.
- `db/health.py`: `check_connection()` trả `Any` (so sánh `.scalar_one() == 1`) thay vì `bool`
  → bọc `bool(...)`.
- `ruff format` áp riêng cho từng file vừa sửa (`config.py`, `observability/logging.py`) —
  KHÔNG chạy `ruff format` nguyên thư mục nào.

Verify cuối: `ruff check src/` — sạch. `mypy --strict src/` — sạch (26 file). `uv run pytest`
223/223 pass (không đổi so với sau D2 — D3 không xoá/sửa test nào, đúng dự kiến vì không có
test nào import code legacy, xem grep trong report). Dagster `Definitions` vẫn load được đủ
18 asset key (`resolve_asset_graph().get_all_asset_keys()`), xác nhận D3 không phá `import`
nào của `dagster_project/`.

**D4 — Test cho `dagster_project/` (nợ từ 5C "cố ý chưa làm"):** file mới
`tests/test_dagster_definitions.py`, 27 test — CHỈ load `Definitions`/resource, KHÔNG
materialize asset nào, nên không cần Postgres/LLM thật (tự verify bằng
`env -u DATABASE_URL -u LLM_PROVIDER -u DEEPSEEK_API_KEY uv run pytest ...` — 27/27 vẫn pass).
`PostgresResource`/`LLMResource` không tự kết nối lúc khởi tạo (chỉ khi gọi
`.get_connection()`/`.build()`), `resolve_asset_graph()` chỉ phân tích tĩnh.

Bám sát đúng 5 gạch đầu dòng D3 giao, cộng 1 mục KHÔNG còn áp dụng được sau D1 (nói rõ lý do
thay vì lờ đi):
1. **Định nghĩa hợp lệ:** `Definitions` load được, đúng 18 asset key, khớp bộ key mong đợi
   (`test_definitions_load_with_expected_asset_count`).
2. **Bắt lại lỗi prefix schema đã gặp ở 0.12** (`gold/stg_articles` mồ côi thay vì
   `stg_articles`, PROGRESS.md mục 5C lỗi #2): `test_no_asset_key_has_stray_schema_prefix` —
   assert không còn asset key nào dính prefix ngoài 2 exception đã biết
   (`silver/source_health`, `silver/score_quarantine`, 2 source "external" không override).
3. **Đồ thị phụ thuộc đúng bảng §7.2 + khoảng trống đã ghi nhận:** 18 test tham số hoá
   (`test_asset_dependency_graph_matches_expected`, một test/asset), so khớp `parent_keys`
   thật với bảng mong đợi hardcode trong file — bao gồm cả `articles_normalized` (bổ sung từ
   0.12) và `article_summaries` đổi deps sang `fct_article_score` (D1).
4. **`internal_asset_deps` của multi_asset (lỗi CheckError #4 ở 0.12) — KHÔNG CÒN ÁP DỤNG
   ĐƯỢC:** D1 đã xoá hẳn multi_asset `article_scores_and_summaries` (tách thành hai `@asset`
   độc lập, xem mục 9/10 D1) — không còn `internal_asset_deps` nào trong repo để test lại
   đúng lớp lỗi này. Thay bằng `test_article_scores_and_summaries_are_independent_assets_not_multi_asset`
   — assert cấu trúc mới (2 `AssetsDefinition` độc lập, không phải multi_asset) khiến lớp lỗi
   đó không còn khả năng tái phát, thay vì giả vờ test một cơ chế đã bị xoá.
5. **Partition `end_offset=1`:** `test_daily_partitions_end_offset_allows_today` (khẳng định
   "hôm nay" nằm trong `get_partition_keys()` với `current_time` cố định) +
   `test_daily_partitions_end_offset_zero_would_exclude_today` (đối chứng: dựng lại đúng
   partition mặc định `end_offset=0` để chứng minh dòng cấu hình không phải thừa).
6. **Resource `llm` không default:** 4 test — thiếu `LLM_PROVIDER` (rỗng) raise `Failure`;
   tên provider không hỗ trợ raise `Failure`; `deepseek` thiếu API key raise `Failure`
   (không tự bịa key); `mock` build được ngay không cần env/mạng nào.

Verify: `ruff check`/`mypy --strict`/`ruff format --check` sạch trên file mới. `uv run
pytest tests/` — **250/250 pass** (223 sau D3 + 27 mới) — vượt mốc 229 gốc dù D1 đã xoá 12
test composite (11 + 1), đúng yêu cầu DONE WHEN "không test cũ nào bị xoá để cho xanh" (chỉ
xoá test của code đã xoá theo D1, không xoá để né lỗi).

## 11. Verify cuối D1–D4 — chạy lại toàn đồ thị Dagster thật, kiểm chứng luồng D1 end-to-end

Sau khi D1–D4 xong, chạy lại **toàn bộ đồ thị 16 asset buildable** (bỏ 2 asset "external"
`silver/source_health`/`silver/score_quarantine`, không tự materialize được) cho partition
`2026-08-11` — partition ĐÃ materialize nhiều lần trước đó (xem mục 5C/6) — bằng đúng lệnh
README đã ghi (`LLM_PROVIDER=mock`, an toàn/miễn phí):

```
PYTHONUTF8=1 PYTHONIOENCODING=utf-8 LLM_PROVIDER=mock uv run dagster asset materialize \
  --select "raw_rss,articles_normalized,stg_articles,articles_filtered,article_scores,article_summaries,stg_article_scores,stg_article_summaries,seed_sources,stg_sources,snap_sources,dim_source,fct_article_score,mart_daily_digest,mart_pipeline_health,published_site" \
  -f dagster_project/definitions.py --partition 2026-08-11
```

**Kết quả: `RUN_SUCCESS`.** Đây là lần đầu tiên đồ thị chạy thật với `article_scores`/
`article_summaries` là HAI asset tách rời (D1) thay vì một multi_asset — xác nhận trực tiếp
luồng mới hoạt động đúng: `article_scores` chạy xong → `daily_dbt_assets` build
`fct_article_score` (dagster-dbt tự subset đúng lúc, không cần code orchestration thủ công
nào ở phía Dagster, đúng thiết kế D1) → `article_summaries` mới chạy, đọc top-K từ
`gold.fct_article_score` vừa build.

Đếm lại trước/sau (không đổi ở 2 bảng DONE WHEN yêu cầu, dù `bronze`/`silver` có tăng vì
ingest RSS thật lấy thêm bài mới của partition 08-11 — giống hệt hành vi đã ghi ở 5C):

| Bảng | Trước | Sau | Đổi? |
|---|---|---|---|
| `bronze.raw_articles` | 174 | 190 | Tăng (ingest RSS thật lấy thêm bài mới) |
| `silver.articles` | 115 | 126 | Tăng (theo bronze) |
| `silver.article_scores` | 88 | 99 | Tăng (mock chấm bài mới của 08-11) |
| `silver.article_summaries` | 45 | 45 | **KHÔNG đổi** |
| **`gold.fct_article_score`** | **12** | **12** | **KHÔNG đổi một dòng nào (DONE WHEN)** |
| **`gold.mart_daily_digest`** | **12** | **12** | **KHÔNG đổi một dòng nào (DONE WHEN)** |
| `gold.mart_pipeline_health` | 2 | 2 | KHÔNG đổi |

11 điểm mock mới (99−88) của partition 08-11 KHÔNG lọt vào `fct_article_score` — đúng hàng
rào `non_production_model_names` (mục 9 AGENTS.md/5B) đang hoạt động, và giờ hàng rào này áp
dụng ĐÚNG LÚC hơn D1 cũ: trước D1, top-K tóm tắt chọn theo composite tạm trong Python KHÔNG
biết gì về hàng rào này (chỉ dbt lọc SAU khi đã tồn tại); từ D1, `run_summarize_top_k_partition`
đọc thẳng từ `gold.fct_article_score` nên tự động chỉ thấy bài đã qua hàng rào — không cần
biết khái niệm "mock" tồn tại. Xác nhận thêm: `SELECT DISTINCT model_name FROM
gold.fct_article_score` → chỉ `deepseek-v4-flash`, không dòng nào chứa "mock"/"giả lập" trong
`gold.mart_daily_digest.summary_vi`. Heartbeat `200 OK` (asset `published_site`).

**Tổng kết trạng thái sau D1–D4:** `grep -r "compute_composite_score" src/ tests/` → rỗng.
`composite.py` đã xoá. `uv run intel-bot doctor` chạy được. `ruff check src/` + `mypy
--strict src/` sạch toàn bộ. `uv run pytest tests/` → 250 pass (tăng so với 229, không xoá
test cũ để né lỗi — chỉ xoá test của code đã xoá). Dagster `RUN_SUCCESS` cho partition đã
materialize trước đó, `gold.fct_article_score`/`gold.mart_daily_digest` không đổi một dòng.
Mục 3.4 và mục 4 đã đóng (đánh dấu "ĐÃ SỬA"/"ĐÃ XOÁ" ngay tiêu đề). Mục 5A/5C đã ghi chú
D1/D4 trả nợ ngay tại chỗ nợ được ghi ban đầu.

**Cố ý chưa làm / ngoài phạm vi D1–D4:**
- `raw_github`, sensor, lịch 12:00/18:00 bổ sung, Dagster daemon production — vẫn ngoài
  phạm vi (rào chắn "KHÔNG thêm asset, KHÔNG đụng sensor" của task này + rào chắn cũ từ
  0.12 chưa ai gỡ).
- Migration mới cho D1 — KHÔNG cần: D1 không đổi schema nào (đọc thêm từ bảng
  `gold.fct_article_score` đã tồn tại từ 0.10, không tạo cột/bảng mới).
- `setup_logging()` trong `observability/logging.py` (và `Settings`/`settings` ở
  `config.py` mà nó dùng) có vẻ là code chết (không ai gọi, xem mục 10 D3) — KHÔNG xoá vì
  không nằm trong danh sách mục 4 và ngoài phạm vi D3 (chỉ sửa type cho sạch mypy). Nếu có
  task dọn dẹp tiếp theo, đây là ứng viên.

## 12. Đã làm — 1.1 Mở rộng nguồn (PRODUCTION_PLAN §8.1/§8.5/§5.6) — DỪNG GIỮA CHỪNG theo
rào chắn, chờ quyết định

**Nguồn mới — 12/33 ứng viên qua verify thật (HTTP 200 + feedparser parse được + entry mới
nhất không quá cũ + robots.txt cho phép), ngày 2026-08-12:**

16 URL người dùng đưa (2 trùng nguồn đã có `techcrunch_ai`/`construction_dive`, bỏ qua) +
15 nguồn AI/data-engineering tự đề xuất (được người dùng cho phép mở rộng phạm vi tìm kiếm,
nhưng GitHub/Reddit KHÔNG thêm qua kênh này — cần fetcher riêng, task 1.2 mới làm, Reddit
fetcher legacy đã xoá ở D3) → **12 nguồn pass, 21 fail**:

| Lý do fail | Số lượng | Ví dụ |
|---|---|---|
| HTTP 403 (chặn bot) | 8 | ENR, BDC Network, Autodesk Construction, Industry Week, Assembly Magazine, SME, ACHR News, Contracting Business |
| HTTP 404 | 5 | ASHRAE Journal, Anthropic `/rss.xml`, Uber Eng `/feed/`, Apache Airflow blog, deeplearning.ai The Batch |
| HTTP 500 | 1 | Manufacturing News |
| Feed rỗng/redirect sang HTML (không phải feed thật) | 4 | ICT News VN, VnEconomy, LangChain Blog, Airbnb Engineering |
| Connect timeout/refused | 2 | HPAC Magazine, Medium "data-engineering-things" (slug có thể sai) |
| **Feed "chết" theo nghĩa thực dụng** | 1 | VentureBeat AI — HTTP 200, parse được, nhưng entry mới nhất đã 84 ngày; với cửa sổ digest 48h + cold-start loại bài >7 ngày (§8.2), feed này đóng góp 0 bài cho digest dù kỹ thuật chưa "chết 2 năm" như ví dụ trong đề bài. Loại, có ghi rõ lý do để dễ đổi ý sau. |

12 nguồn pass (tier/industries đã được người dùng xác nhận trước khi ghi file — KHÔNG tự
chốt): `the_decoder`, `mit_technology_review`, `ars_technica_tech`, `the_verge_ai`,
`huggingface_blog`, `simon_willison`, `marktechpost`, `openai_news`, `netflix_tech_blog`,
`towards_data_science`, `databricks_blog`, `dbt_labs_blog`. **8 (cũ) + 12 (mới) = 20 nguồn**
— đúng mốc ~20 của §8.5.

Không có tag "data engineering" riêng trong `INDUSTRY_TAGS` (`contracts/llm_score.py` —
`{ai, construction, hvac, manufacturing, iot}`) nên các nguồn thiên data engineering
(`netflix_tech_blog`/`towards_data_science`/`databricks_blog`/`dbt_labs_blog`) gán
`industries: [ai, tech]` giống `techcrunch_ai` đã có — thêm tag mới là sửa **code**
(`INDUSTRY_TAGS`), ngoài phạm vi "chỉ là dữ liệu cấu hình" của task này.

**`seed_sources.csv` + `dbt snapshot` — SCD2 đúng, đã tự verify bằng SQL trực tiếp (không
chỉ đọc log dbt):**
- `dbt seed` → `INSERT 20` vào `gold.seed_sources`.
- `dbt snapshot` (lần đầu sau khi seed) → `INSERT 0 12`: đúng 12 dòng MỚI, **0 dòng bị
  update/đóng** — 8 nguồn cũ hoàn toàn không bị chạm.
- Query `gold.dim_source` trước/sau: 8 nguồn cũ giữ NGUYÊN `valid_from` gốc
  (`2026-08-11 04:18:00`), 12 nguồn mới có `valid_from` = lúc snapshot chạy
  (`2026-08-12 07:37:13`); toàn bộ 20 dòng `is_current=true`, `valid_to=NULL`; không
  `source_id` nào có >1 dòng. Snapshot chạy lại lần 2 → `INSERT 0 0` (idempotent).
- `dbt build` toàn bộ sau đó: **54/54 PASS** (bao gồm `assert_dim_source_single_current`),
  `fct_article_score`/`mart_daily_digest` giữ nguyên 12/12 (đúng — không có bài mới nào
  được chấm/tóm tắt trong bước này).

**Cứng hoá `validate-sources` (điểm 4, code — được phép sửa):**
- `SourceValidation` (`ingest/rss_fetcher.py`) thêm field `latest_entry_date` (ISO string,
  hàm thuần mới `latest_entry_date()` — ưu tiên `published_parsed`/`updated_parsed`, fallback
  `published`/`updated` thô). **CHỦ Ý KHÔNG** đưa staleness vào tiêu chí `ok` (giữ nguyên
  đúng 4 tiêu chí gốc §8.5: HTTP 200 + parse được + ≥1 entry + có trường ngày) — ngưỡng
  "bao nhiêu ngày là feed chết" là quyết định nghiệp vụ cần một task riêng để chỉnh (dễ gây
  CI đỏ giả nếu một nguồn thật chỉ đăng bài thưa hơn ngưỡng tự chọn), chỉ thêm cột hiển thị
  cho người đọc bảng tự nhận ra.
- CLI `validate-sources`: thêm cột "ngày mới nhất" vào bảng in ra; **thêm
  `raise typer.Exit(code=1)` khi có ≥1 nguồn fail** (trước đây luôn exit 0 dù bảng in ra
  toàn FAIL — đây chính là lỗi khiến lệnh này vô dụng khi chạy trong CI).
- Test mới `tests/test_cli_validate_sources.py` (5 test): all-OK → exit 0; có 1 fail → exit
  ≠ 0 + tên nguồn fail xuất hiện trong output; 3 test cho hàm thuần `latest_entry_date()`.
- Verify: `uv run intel-bot validate-sources` thật trên 20 nguồn → **20/20 OK, exit 0**.
  `ruff check`/`mypy --strict`/`ruff format --check` sạch. `uv run pytest` → **255/255 pass**
  (250 sau D1–D4 + 5 mới).

**Ingest thật 1 partition (hôm nay, 2026-08-12) — PHÁT HIỆN VƯỢT NGƯỠNG, DỪNG theo rào
chắn "nếu >500 bài/ngày, DỪNG và báo cáo, không tự làm tiếp":**

```
uv run intel-bot ingest --date 2026-08-12
→ entries_fetched=2148 rows_inserted=2148 sources_ok=20 sources_failed=0
```

**2148 ≫ 500** — đã DỪNG, KHÔNG chạy `normalize`/`filter`/`score` tiếp cho partition này,
KHÔNG tự quyết định bật embedding filter sớm (§9.1 nói rõ đây là quyết định kiến trúc, chờ
người quyết).

**Phân tích nguyên nhân (breakdown theo source_id, không đoán):**

| source_id | Số dòng ingest hôm nay |
|---|---|
| `openai_news` | 1124 |
| `huggingface_blog` | 839 |
| `simon_willison` | 30 |
| `dbt_labs_blog` | 25 |
| `ars_technica_tech`, `techcrunch_ai`, `towards_data_science` | 20 mỗi nguồn |
| `netflix_tech_blog`, `databricks_blog`, `construction_enquirer`, `the_verge_ai`, `the_decoder`, `marktechpost`, `mit_technology_review` | 10 mỗi nguồn |
| 6 nguồn còn lại (đã ingest từ 08-10/08-11 trước đó) | 0 (toàn bộ entry đã có trong bronze, dedup theo `payload_hash`) |

`openai_news` + `huggingface_blog` = **1963/2148 (91%)** tổng số dòng. Đây là feed
**full-history KHÔNG phân trang** (feedparser trả về TOÀN BỘ entry hiện có trong XML, không
giới hạn "N bài gần nhất" như đa số feed khác) — lần ingest ĐẦU TIÊN của 2 nguồn này coi
toàn bộ lịch sử là "mới" vì `payload_hash` chưa từng thấy. **Không phải nhịp hằng ngày sẽ
lặp lại**: đã tự verify bằng cách chạy lại `ingest --date 2026-08-12` lần 2 (cùng ngày) →
`rows_inserted=0` (idempotent qua dedup cấp 0, đúng §8.4). Loại bỏ 2 nguồn này:
2148 − 1963 = **185 bài/18 nguồn/ngày** — RẤT KHỚP giả định `~20 feed × ~10 bài/ngày ≈ 200`
của plan (§8.5 dòng 636).

**Không tự quyết — cần người dùng chọn một trong các hướng (hoặc hướng khác):**
1. Giữ nguyên 2 nguồn, chấp nhận backlog một-lần lớn hôm nay (~1963 bài) sẽ được `normalize`
   cold-start (§8.2) loại gần hết vì `published_at` cũ hơn 7 ngày — chỉ tốn dung lượng bronze,
   không tốn tiền LLM. Rủi ro: không biết chắc 2 feed này có luôn full-history hay chỉ tình
   cờ hôm nay lớn.
2. Bỏ 2 nguồn khỏi `sources.yaml` (dễ nhất, nhưng mất 2 nguồn AI hàng đầu — OpenAI/HF).
3. Bật embedding filter sớm hơn kế hoạch (§9.1) — đúng là quyết định kiến trúc lớn, đề bài
   nói rõ không tự làm.
4. Thêm giới hạn số entry/nguồn/lần fetch ở tầng ingest (vd. chỉ lấy N entry mới nhất) —
   đây là sửa **logic fetcher**, rào chắn task này cấm ("KHÔNG đổi logic fetcher").

**Cố ý CHƯA làm** (vì lý do trên, chờ quyết định): `normalize`/`filter`/`score` cho
partition 2026-08-12; đối chiếu số bài ELIGIBLE thật (chỉ có số RAW ingest, chưa qua
cold-start/filter); mọi thay đổi liên quan tới 2 nguồn `openai_news`/`huggingface_blog`.

**Cập nhật (task 1.2, cùng ngày 2026-08-12) — bằng chứng mới, CHƯA phải quyết định:** khi
verify `raw_github` (mục 13), materialize nhầm `raw_rss` thật cho partition **2026-08-11**
(lẽ ra chỉ định verify `raw_github`, xem "Sự cố" ở mục 13) vô tình tái hiện đúng vấn đề này
lần thứ hai, dưới một partition khác. Bằng chứng thu được, người dùng chọn GIỮ LẠI thay vì
dọn: `openai_news` dump lại **đúng 1124 dòng full-history** (giống hệt số liệu lần đầu) —
xác nhận đây là ĐẶC TÍNH CỐ ĐỊNH của feed (không hỗ trợ conditional GET, không phải ngẫu
nhiên "hôm đó lớn" như rủi ro đã nêu ở phương án 1 phía trên). Cold-start tự loại 1109/1124
(`too_old`), 15 còn lại được mock chấm điểm với cost=$0, KHÔNG lọt gold
(`non_production_model_names` chặn đúng). `huggingface_blog` KHÔNG dump lại (server trả
304 — có vẻ nguồn này CÓ hỗ trợ conditional GET, khác với suy đoán ban đầu gộp chung 2
nguồn). Việc này CỦNG CỐ phương án 1 (giữ nguyên nguồn, chấp nhận cold-start lọc, không tốn
tiền) nhưng **KHÔNG tự chọn giùm** — 4 phương án ở trên vẫn treo, người dùng quyết định khi
quay lại task 1.1.

## 13. Đã làm — 1.2 GitHub Search fetcher (PRODUCTION_PLAN §4.1, §7.2, §8.1, §8.5, §24.5)

`src/intel_bot/ingest/github_fetcher.py` (mới, kiến trúc v2 — KHÔNG liên quan
`github_fetcher.py`/`github_trending_fetcher.py` v1 đã xoá ở D3) + `config/github_sources.yaml`
(mới) + asset Dagster `raw_github` (`dagster_project/assets/bronze.py`). Bronze/silver dùng
LẠI đúng bảng/hàm chuẩn hoá hiện có — không migration nào (schema `bronze.raw_articles` đã
có sẵn `source_type` cho đúng 2 giá trị 'rss'/'github' từ migration 0002).

**24.5 verify trước khi code — số liệu thật, không đoán (2026-08-12, qua docs.github.com):**
- Search API authenticated (PAT): **30 request/phút** cho mọi endpoint search TRỪ
  `/search/code`. `/search/code` riêng: **10 request/phút**, bắt buộc xác thực (module này
  KHÔNG dùng endpoint đó). Unauthenticated: **10 request/phút**.
  Nguồn: https://docs.github.com/en/rest/search/search,
  https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- `X-GitHub-Api-Version` hiện hành: `2026-03-10` (bản cũ `2022-11-28` là mặc định nếu bỏ
  header — verify qua https://docs.github.com/en/rest/about-the-rest-api/api-versions).
- `GITHUB_TOKEN` trong `.env` thật đang RỖNG (đã tự kiểm tra, không đoán) — người dùng chọn
  chạy **unauthenticated** cho task này thay vì dừng lại chờ điền PAT.

**Thiết kế đáng chú ý:**
- **KHÔNG nhánh riêng trong `normalize_partition()`.** `github_fetcher.repo_to_payload()`
  giữ NGUYÊN toàn bộ trường gốc GitHub trả về (P3) và THÊM 4 khoá alias
  (`title`/`link`/`summary`/`updated_parsed`, lấy từ `full_name`/`html_url`/`description`/
  `pushed_at`) — đúng những khoá `src/intel_bot/ingest/normalizer.py::parse_entry()` đã đọc
  cho RSS, cùng tinh thần feedparser tự có cặp `published`/`published_parsed` song song.
  `canonicalize_url()` đã tự rút gọn URL GitHub về `github.com/{owner}/{repo}` từ task 0.5,
  không cần sửa gì. Test `test_repo_to_payload_is_parseable_by_shared_normalizer_without_branch`
  verify trực tiếp `parse_entry()` đọc đúng payload đã shape, không mock lại normalizer.
- **`config/github_sources.yaml` (khoá config MỚI, đã nêu tên/vị trí trước khi thêm) tách
  RIÊNG khỏi `config/sources.yaml`:** file RSS nạp thẳng vào `gold.dim_source` qua
  `seed_sources.csv` + dbt seed/snapshot (task 1.1) — gộp nguồn github vào đó kéo theo đổi
  tầng dbt, vi phạm rào chắn "KHÔNG đụng tầng score/dbt/publish ở bước này". Hệ quả ĐÃ BIẾT,
  không phải bug: nguồn github chưa có dòng nào trong `dim_source`, nên
  `macros/scoring.sql::source_tier_score()` trả 0 cho các bài này (nhánh "tier không khớp
  map nào" đã có sẵn từ task 0.10, không crash) — `credibility_blended` tạm thời chỉ còn
  20% điểm LLM cho tới khi có task riêng đưa nguồn github vào `seed_sources.csv`.
- **5 truy vấn (`github_ai`/`github_construction`/`github_hvac`/`github_manufacturing`/
  `github_iot`), khớp đúng 5 tag của `INDUSTRY_TAGS`, đã verify THẬT** bằng gọi trực tiếp
  `api.github.com/search/repositories` (unauthenticated) trước khi ghi vào config — không
  suy đoán topic nào hoạt động: `topic:llm`/`topic:iot` KHÔNG kèm ngưỡng `stars:>N` với
  `sort=updated` trả về gần như toàn dự án hobby/nhiễu (đã tự thấy: "New-Grad-Data-Science-
  Jobs-2027", bot crypto…) — thêm `stars:>N` mới lọc ra tín hiệu chất lượng thật
  (`vllm-project/vllm`, `huggingface/transformers`, `FreeCAD/FreeCAD`, `apache/plc4x`,
  `frappe/erpnext`…). Reuse khoá config CŨ `ingest.github_per_source` (đã có sẵn trong
  `config/app.yaml` từ trước, không ai xoá khi dọn v1 legacy) làm `per_page` — không thêm
  khoá mới cho việc này.
- **Rate limit + backoff (P4):** chạy TUẦN TỰ (không đồng thời — ngân sách chia sẻ chung),
  đọc `X-RateLimit-Remaining` + bắt 403/429 sau MỖI request; vượt hạn mức → log warning
  `event=github_rate_limited`, dừng sạch phần truy vấn còn lại, trả `rate_limited=True`,
  KHÔNG raise (test `test_rate_limited_response_stops_cleanly_without_raising`: 403 ở truy
  vấn 1 → truy vấn 2 KHÔNG hề chạy). Một truy vấn lỗi thường (vd. 422 cú pháp sai) KHÔNG làm
  hỏng các truy vấn khác — khác hành vi dừng sạch của rate limit (test
  `test_one_query_error_does_not_abort_other_queries`).
- **`GITHUB_TOKEN` rỗng KHÔNG raise Failure** ở asset (khác resource `llm` task 0.12) — GitHub
  Search API tự thân hỗ trợ unauthenticated, không phải một cấu hình thiếu sót cần chặn cứng;
  chỉ log rõ đang chạy ở hạn mức thấp hơn (10 thay vì 30 req/phút).
- **`articles_normalized` (`dagster_project/assets/silver.py`) đổi `deps=["raw_rss"]` →
  `deps=["raw_rss", "raw_github"]`** — cách duy nhất đổi để tích hợp asset mới vào đồ thị
  hiện có, đúng như đề bài yêu cầu.
- **Khoảng trống ĐÃ BIẾT trên đồ thị Dagster (không sửa được mà không đụng tầng dbt):**
  `mart_pipeline_health` (dbt) đếm `source('bronze', 'raw_articles')` KHÔNG lọc
  `source_type` (đã đọc SQL xác nhận — số liệu ĐÚNG, tự động gồm cả bài github), nhưng cạnh
  phụ thuộc HIỂN THỊ trên đồ thị Dagster của node đó chỉ trỏ về `raw_rss`, không có
  `raw_github` — do `_SourceAwareDbtTranslator` (`dagster_project/assets/dbt_assets.py`) chỉ
  ánh xạ 1-1 `(schema, table) -> asset_key`, và cả 2 asset Python cùng ghi CHUNG một bảng vật
  lý `bronze.raw_articles` nên dbt chỉ thấy được MỘT `source()`. Sửa đúng cần tách source dbt
  hoặc viết lại translator — đụng tầng dbt, ngoài phạm vi task này, không tự sửa.

**Test (`tests/test_github_ingest.py`, 10 test, mock mạng bằng `httpx.MockTransport` — không
gọi API thật):** 3 unit cho `repo_to_payload()` (giữ nguyên trường gốc + thêm alias, xử lý
`description=None`, payload parse được bởi `parse_entry()` dùng chung), 3 unit cho
`is_rate_limited_response()` (403/429, `X-RateLimit-Remaining=0`, response bình thường), 4
integration Postgres thật (ghi bronze từ fixture, idempotent chạy 2 lần, dừng sạch khi rate
limit, một truy vấn lỗi không hỏng truy vấn khác). `tests/test_dagster_definitions.py` cập
nhật: 18→19 asset, `raw_github: frozenset()`, `articles_normalized` thêm `raw_github` vào
deps mong đợi. `ruff check`/`ruff format --check`/`mypy --strict` sạch trên toàn bộ file mới
+ sửa. `uv run pytest tests/` → **266/266 pass** (255 trước + 11 mới: 10 github + 1 dep mới).

**Verify THẬT trên DB dev (không chỉ mock, đã tự chạy — unauthenticated):**
- `dagster asset materialize --select "raw_github" --partition 2026-08-11` → `RUN_SUCCESS`,
  **25 dòng thật** ghi vào `bronze.raw_articles` (`source_type='github'`, 5 nguồn × 5
  repo/nguồn), `silver.source_health` có đủ 5 dòng `http_status=200 error_message=NULL`.
- Chạy LẠI cùng lệnh (~1 phút sau) → `RUN_SUCCESS` lần 2, **0 nhóm `payload_hash` trùng**
  (`GROUP BY payload_hash HAVING COUNT(*)>1` → rỗng) — nhưng tổng dòng tăng 25→29, KHÔNG
  phải lỗi dedup: 4 repo có `pushed_at` mới hơn giữa 2 lần gọi (repo hot, mới có commit) nên
  payload thật sự KHÁC → `payload_hash` khác → ghi dòng mới là ĐÚNG hành vi P1/P2 (bronze
  bất biến, chỉ chặn trùng NGUYÊN VĂN). Đây KHÔNG phải phép thử idempotency sạch (nguồn sống
  thay đổi giữa 2 lần gọi) — phép thử idempotency thật (payload cố định, phải cho
  `rows_inserted=0` ở lần 2) nằm ở `test_ingest_twice_same_day_is_idempotent` (mock, đã pass).
- `silver.articles` sau khi `articles_normalized` chạy: **34 dòng `source_id LIKE
  'github_%'`**, `canonical_url` dạng `github.com/{owner}/{repo}` đúng chuẩn, **0 nhóm
  `canonical_url` trùng** trong toàn bảng (không đụng bài RSS).
- Materialize toàn bộ 17 asset buildable (thêm `raw_github` vào danh sách 16 asset ở mục 11)
  cho partition **2026-08-11** (KHÔNG phải hôm nay 2026-08-12 — theo lựa chọn của người dùng,
  xem mục 12 "Cập nhật"), `LLM_PROVIDER=mock` → **`RUN_SUCCESS`**, dbt `Completed successfully`
  (`PASS=9 WARN=0 ERROR=0`), heartbeat `200 OK`. `gold.fct_article_score`/
  `gold.mart_daily_digest` **giữ nguyên 12/12** (KHÔNG đổi — bài github chưa vào gold, đúng
  thiết kế đã ghi ở trên vì thiếu `dim_source`), **0 dòng mock lọt `mart_daily_digest`**.

**Sự cố xảy ra trong lúc verify — đã báo người dùng ngay, không giấu:** để chọn asset cho
lệnh materialize toàn đồ thị, copy nguyên danh sách `--select` từ mục 11 (có `raw_rss`) mà
không nghĩ kỹ: `raw_rss` không "time-travel" theo partition — materialize nó cho BẤT KỲ
partition nào cũng fetch feed sống TẠI THỜI ĐIỂM CHẠY rồi gắn nhãn `ingest_date` theo
partition đó. Chọn `2026-08-11` để TRÁNH đụng quyết định 1.1 đang treo ở `2026-08-12`, nhưng
vì có `raw_rss` trong `--select`, đã vô tình tái hiện đúng sự cố `openai_news` full-history
dump (1124 dòng, giống hệt số liệu lần đầu) — lần này dưới nhãn `2026-08-11`. Không tốn tiền
thật (`LLM_PROVIDER=mock`, cost tổng không đổi), không lọt gold — nhưng là hành động ngoài ý
định, đã dừng ngay, báo cáo đầy đủ số liệu, hỏi người dùng trước khi làm gì tiếp. Người dùng
chọn GIỮ LẠI dữ liệu (không dọn) — chi tiết đầy đủ + hệ quả cho quyết định 1.1 ở mục 12
"Cập nhật". **Bài học cho task sau: KHÔNG copy nguyên `--select` có `raw_rss`/bất kỳ asset
fetch-nguồn-sống nào vào lệnh materialize trừ khi THẬT SỰ cần ingest lại — asset dạng này
không an toàn để "chỉ chạy cho đủ đồ thị".**

**Cố ý chưa làm / ngoài phạm vi task 1.2:**
- Đưa nguồn github vào `gold.dim_source`/`seed_sources.csv` — rào chắn "KHÔNG đụng tầng
  score/dbt/publish" cấm ở bước này; hệ quả là `credibility_blended` của bài github tạm thời
  chỉ có 20% trọng số (điểm LLM thô), thiếu hẳn 80% source tier.
  Anomaly detection + Freshness SLA — thuộc task 1.4/1.5 kế tiếp (rào chắn: chỉ 1.2).
- Không đụng CLI `ingest` (`cli.py`) — chỉ asset Dagster gọi `github_fetcher` trực tiếp, đúng
  đề bài chỉ giao "Asset Dagster `raw_github`", không giao tích hợp CLI. `run_github_ingest()`
  vẫn là hàm module-level tái dùng được cho CLI sau này nếu cần, không phải rào cản.
- Không sửa `_SourceAwareDbtTranslator` để nối cạnh `raw_github → mart_pipeline_health` trên
  đồ thị Dagster (xem "Khoảng trống ĐÃ BIẾT" ở trên) — đụng tầng dbt, ngoài phạm vi.
- Chưa xin/điền `GITHUB_TOKEN` thật — đang chạy unauthenticated (10 req/phút), đủ cho 5 truy
  vấn/ngày hiện tại; nâng lên 30 req/phút bằng cách điền PAT vào `.env` khi cần mở rộng số
  truy vấn/nguồn.

**Bổ sung — verify end-to-end bằng `LLM_PROVIDER=deepseek` THẬT (cùng ngày, theo yêu cầu
"muốn hiện chính xác data trong csdl lên web"):** phát hiện thêm một gotcha khi làm việc này,
không thuộc code của task 1.2 nhưng ảnh hưởng trực tiếp tới việc bài github lên trang được
hay không — ghi lại đây cho đủ:

- **65 bài (19 github + 46 rss, partition 2026-08-11) từng bị mock chấm khi tôi verify task
  1.2 KẸT VĨNH VIỄN ở `status='scored'`** — `run_score_partition()`/`load_eligible_articles()`
  chỉ chọn `status='eligible'`; MỘT KHI đã có bất kỳ điểm nào (kể cả mock), status không bao
  giờ tự quay lại 'eligible' để được chấm lại bằng provider thật. Đây là hệ quả CHƯA từng gặp
  của việc chạy mock trên dữ liệu thật (khác lỗi 5B — lỗi đó là mock LỌT gold, đã có hàng rào
  `non_production_model_names`; lỗi NÀY là mock làm bài không bao giờ được chấm thật nữa).
  Đã UPDATE thủ công `status='eligible'` cho đúng 65 bài này (xác định qua điều kiện "có
  đúng 1+ dòng `article_scores` và TẤT CẢ đều `model_name='mock'`" — không đụng bài nào khác)
  sau khi hỏi và được người dùng đồng ý tốn ~$0.02 thật.
- `score --provider deepseek --date 2026-08-11` chạy thật: **scored=65 quarantined=0
  summarized=15, cost=$0.03024431200** (đúng số thật, không phải ước tính). `dbt build
  --select mart_daily_digest` → **12 → 27 bài**. `publish` → `docs-site/index.html`/
  `articles.json` có 27 bài thật, 0 dòng mock.
- **0/19 bài github lọt vào 27 bài đó** — đã xếp hạng composite thật để biết vì sao: bài
  github gần nhất (`github_ai`, composite=5.90) THIẾU ĐÚNG 0.22 điểm so với hạng #15 (cắt
  tại 6.12) để vào top-K tóm tắt. Sát nút — khớp đúng dự đoán ở phần "Khoảng trống ĐÃ BIẾT"
  phía trên: `credibility_blended` của bài github chỉ 0.2–2.0 (thang 10) vì thiếu 80% trọng
  số source-tier (chưa có trong `gold.dim_source`). Có tier dù chỉ tier 2-3 gần như chắc chắn
  đủ bù khoảng cách này. **Việc thêm nguồn github vào `dim_source`/`seed_sources.csv` — đụng
  tầng dbt, ngoài phạm vi task 1.2 — là bước tiếp theo tự nhiên nếu muốn bài github thật sự
  xuất hiện trên trang, chưa làm, chờ quyết định riêng.**

## 14. Đã làm — 1.4 Anomaly checks + 1.5 Freshness policy (PRODUCTION_PLAN §13.3, §13.4, §18.2)

`dbt_project/tests/assert_ingest_count_no_anomaly.sql` + 5 test singular khác (severity=warn)
+ 9 var mới trong `dbt_project.yml` (task 1.4) — `dagster_project/checks.py` mới, gắn
`FreshnessPolicy` lên 3 asset qua `Definitions.map_resolved_asset_specs()` (task 1.5).

### 14.1 Mâu thuẫn phát hiện TRƯỚC khi code — đã báo cáo, người dùng chọn hướng

Đề bài mở đầu bằng "`gold.mart_pipeline_health` đã có từ task 0.10 với đầy đủ cột
funnel/cost/latency" — **SAI, đã verify bằng cách đọc thẳng SQL thật** (không phải PROGRESS.md
mục 5A, file đó cũng không nói rõ): bảng thật lúc đó CHỈ có `pipeline_date, latency_p50/95_ms,
ingest_count, eligible_count, excluded_count, excluded_ratio, quarantine_count,
total_cost_usd, source_fail_count, computed_at` — **thiếu `mean_importance`,
`stddev_importance`, một cột tag-rỗng, và một `scored_count` đúng trục ngày cho cost/bài** —
4/6 kiểm tra §13.3 không thể viết được nếu chỉ đọc mart như hiện có. Đã dừng lại, trình bày
đúng 2 phương án (mở rộng mart thêm cột / để test tự tính thẳng từ staging), người dùng chọn
**mở rộng mart** (đúng tinh thần §18.2 "metrics nằm hết trong mart, trả lời được bằng SQL").

**Cột mới thêm vào `mart_pipeline_health.sql` (additive — KHÔNG đổi logic cột cũ nào):**
`scored_count`, `mean_importance`, `stddev_importance`, `empty_tag_count`, `empty_tag_rate`,
`cost_per_article`, `quarantine_rate`. Tất cả tính trên CTE `cost_latency_daily` (trục
`scored_at::date`, JOIN `stg_article_scores`→`stg_articles` lấy `industry_tags`), lọc
`is_production_model()` (loại `mock` — bài học 5B, không để dữ liệu test lẫn vào theo dõi
drift/chi phí thật). `quarantine_rate` dùng mẫu số `scored_count + quarantine_count`
(KHÔNG dùng `eligible_count` có sẵn — khác trục ngày `first_seen_date`, sẽ sai). Mọi tỷ
lệ/thống kê là NULL (không phải 0) khi ngày đó chưa chấm bài nào (P4 — không suy diễn).
Model là `incremental`/`merge` nên cần `dbt run --full-refresh` một lần để backfill cột mới
cho dòng cũ — đã chạy, verify bằng `SELECT` lại 3 dòng thật.

### 14.2 6 kiểm tra bất thường (§13.3) — singular test, severity=warn

| Test | Kiểm tra | Loại ngưỡng |
|---|---|---|
| `assert_ingest_count_no_anomaly` | Row count ingest lệch > 3σ / 14 ngày trước | Baseline cửa sổ |
| `assert_quarantine_rate_reasonable` | quarantine_rate > 10% | Tuyệt đối/ngày |
| `assert_importance_mean_no_drift` | mean_importance lệch > 1.0 / 7 ngày trước | Baseline cửa sổ |
| `assert_importance_stddev_sufficient` | stddev_importance < 0.8 | Tuyệt đối/ngày |
| `assert_empty_tag_rate_reasonable` | empty_tag_rate > 5% | Tuyệt đối/ngày |
| `assert_cost_per_article_no_drift` | cost_per_article lệch tương đối > 50% / 7 ngày trước | Baseline cửa sổ |

Mọi ngưỡng (3σ/10%/1.0/0.8/5%/50%/14 ngày/7 ngày) là `vars:` trong `dbt_project.yml`, không
hardcode SQL — đã tự verify bằng cách đổi `anomaly_importance_stddev_min` 0.8→0.95, chạy lại:
ngày 2026-08-11 (stddev thật = 0.888) đổi từ PASS sang WARN, revert lại thì PASS lại — chứng
minh bằng SỐ LIỆU THẬT, không phải fixture giả.

**severity=warn cho cả 6** (đề bài mục 3): `{{ config(severity="warn") }}` đầu mỗi file test —
bất thường phải HIỆN ra (WARN trong output `dbt build`) nhưng không được làm `dbt build` exit
khác 0, để `fct_article_score`/`mart_daily_digest` phía sau vẫn chạy tiếp. Đã tự verify: chèn
thủ công 1 dòng `pipeline_date=2026-08-13, stddev_importance=0.3` (giả lập bất thường) →
`dbt test` ra đúng `assert_importance_stddev_sufficient WARN 1`, **7 test còn lại vẫn PASS**,
exit code THẬT = 0 (verify riêng, không qua pipe) → xoá dòng giả đi, `dbt build` toàn dự án
lại **60/60 PASS 0 WARN** (không còn 1 warn nào sót lại).

**Quy tắc dữ liệu tối thiểu (đề bài mục 4):** 3 kiểm tra có baseline (ingest/importance-mean/
cost) dùng CHÍNH độ dài cửa sổ (14 hoặc 7) làm ngưỡng tối thiểu — `row_number() over (order by
pipeline_date) > window_days` trên CHÍNH các dòng đã có trong `mart_pipeline_health` (không
suy diễn ngày trống). Hiện tại DB dev chỉ có đúng 3 dòng `pipeline_date` (2026-08-10/11/12,
đúng thực tế "hôm nay mới có 2-3 ngày lịch sử" đề bài nêu) — cả 3 kiểm tra baseline chưa từng
đủ điều kiện đánh giá, PASS đúng vì "chưa đủ dữ liệu", không phải PASS giả bằng chia 0/so NULL
(đã verify không có `/0` nào trong SQL — mọi phép chia có `nullif`/CASE guard trước). 3 kiểm
tra ngưỡng tuyệt đối (quarantine/stddev/tag-rỗng) không cần quy tắc này — §13.3 không mô tả
chúng theo cửa sổ lịch sử.

### 14.3 Freshness SLA (§13.4) — API thật đã dùng, LỆCH có giải thích

**Verify API 2 vòng, không đoán (đúng rào chắn mục 5):**
1. `dir(dagster)` lọc "fresh" trên `dagster==1.13.17` cài thật → 6 hàm/lớp:
   `FreshnessPolicy`, `LegacyFreshnessPolicy`, `apply_freshness_policy`,
   `build_last_update_freshness_checks`, `build_sensor_for_freshness_checks` (không dùng —
   rào chắn "không sensor"), `build_time_partition_freshness_checks`.
2. Thử `build_last_update_freshness_checks` trước (docstring khớp — `severity` độc lập
   ngưỡng thời gian). Load `Definitions` thật → dagster tự in
   `SupersessionWarning: ...is superseded... Attach FreshnessPolicy objects to your assets
   instead` — **bằng chứng runtime thật**, không phải đoán. Đổi sang
   `FreshnessPolicy.time_window()` + `apply_freshness_policy()`. Gắn vào asset ĐÃ tồn tại
   (không tự định nghĩa asset mới) cần `Definitions.map_resolved_asset_specs()` — bản đầu
   dùng `map_asset_specs(selection=...)` (theo `inspect.signature`, có tham số `selection`
   thật) nhưng CHẠY THẬT thì lỗi `CheckError: selection parameter is no longer supported...
   Please use map_resolved_asset_specs instead` — **lỗi runtime thứ hai**, sửa theo đúng
   thông báo, không tự đoán tên hàm khác.
3. `map_resolved_asset_specs` tự bản thân in `PreviewWarning: ...currently in preview, and
   may have breaking changes in patch version releases` — ghi nhận thẳng: API mới nhất của
   Dagster cho freshness ở bản 1.13.17 vẫn đang preview, có thể đổi ở bản patch sau. Không có
   lựa chọn nào khác ổn định hơn tồn tại (đã liệt kê đủ 6 API ở bước 1) — chấp nhận rủi ro
   này, ghi rõ ở đây để không bị hỏi lại "sao dùng API chưa ổn định".

**Lệch so với §13.4 — cần nói rõ:** `FreshnessPolicy.time_window()` bắt buộc `fail_window`;
`warn_window` tuỳ chọn. §13.4 chỉ định nghĩa ĐÚNG 1 ngưỡng (26h) cho MỖI asset, với
`raw_rss`/`article_scores` chỉ có mức Warning (không có mức Critical/fail nào được mô tả) —
API hiện hành không biểu diễn được "chỉ WARN, không bao giờ FAIL". Đã KHÔNG suy diễn một
`fail_window` gần 26h (sẽ biến "Warning" thành "Critical" giả ngay khi vừa vi phạm SLA, sai ý
plan) — thêm khoá config MỚI `observability.freshness_fail_ceiling_hours: 168` (7 ngày,
`config/app.yaml`) làm "trần an toàn" thuần kỹ thuật để API có giá trị hợp lệ, KHÔNG phải một
SLA thật. `mart_daily_digest` (Critical) dùng ĐÚNG 26h làm `fail_window`, không có
`warn_window` — khớp thẳng plan, không suy diễn gì.

**Ánh xạ "Critical" (§13.4) sang Dagster:** `AssetCheckSeverity`/state của Dagster không có
mức "CRITICAL" riêng — `TimeWindowFreshnessPolicy` chỉ có `fail_window`/`warn_window` (state
PASS/WARN/FAIL/UNKNOWN). "Critical" của plan → `fail_window` (mức cao nhất sẵn có, không có
`warn_window`) cho `mart_daily_digest`.

**Verify THẬT qua GraphQL — đúng field Dagster UI thật sự gọi, không chỉ đọc object Python:**
field `assetNodes.freshnessPolicy` (GraphQL) ánh xạ `LegacyFreshnessPolicy` (đã tự verify:
introspect `__type(name: "FreshnessPolicy")` ra field `maximumLagMinutes`/`cronSchedule` —
đúng chữ ký `LegacyFreshnessPolicy`, KHÔNG phải cái mình dùng) → trả `null` cho cả 3 asset,
**KHÔNG phải bug, chỉ là field GraphQL cũ không biết tới `FreshnessPolicy` mới.** Field đúng
là `assetNodes.internalFreshnessPolicy` — khởi `dagster dev` thật (port 3111), query GraphQL
trực tiếp:
```
raw_rss:            failWindowSeconds=604800 (168h) warnWindowSeconds=93600 (26h)
article_scores:      failWindowSeconds=604800 (168h) warnWindowSeconds=93600 (26h)
mart_daily_digest:   failWindowSeconds=93600 (26h)   warnWindowSeconds=null
articles_normalized: null (asset khác, KHÔNG bị đụng — verify selection chính xác)
```
Khớp đúng thiết kế. Đã tắt server test sau khi verify (`taskkill` PID lắng nghe port 3111,
xác nhận `curl` connection refused).

**Test:** `uv run pytest tests/` → **266/266 pass** (không đổi so với trước — freshness policy
không đụng đồ thị dependency, `test_dagster_definitions.py` không cần sửa). `ruff check`/
`mypy --strict dagster_project/` sạch 14 file.

**Cố ý chưa làm / ngoài phạm vi (đúng rào chắn task 1.4/1.5):**
- KHÔNG cài sensor (`run_failure_sensor`/`freshness_sensor`/`quarantine_sensor`/`cost_sensor`,
  §7.4) — dù `build_sensor_for_freshness_checks` tồn tại sẵn, không gọi.
- KHÔNG gửi Telegram/Slack/email — việc GỬI cảnh báo đi đâu là prompt 15 (§18.3), đề bài nói
  rõ.
- Không đưa nguồn github (task 1.2) vào `mart_pipeline_health`/anomaly checks riêng — mọi
  metric tính chung cả RSS lẫn github (không phân biệt `source_type`), đúng §13.3 (không có
  yêu cầu tách theo nguồn).
- Chưa thêm test cho chính `dagster_project/checks.py` (giống khoảng trống đã biết ở 5C/D4 —
  `apply_freshness_policies()` verify bằng chạy thật + GraphQL, chưa có test pytest tự động
  hồi quy nếu code này bị sửa sai sau — có thể vá bằng cách assert
  `spec.freshness_policy_by_asset_key` kiểu D4 đã làm cho asset graph, để dành task dọn nợ).

## 15. Đã làm — 1.6 Sensors + alert (PRODUCTION_PLAN §7.4, §18.3) — CODE XONG, CHỜ CREDENTIALS
THẬT để verify hết DONE WHEN

`src/intel_bot/observability/alerting.py` (mới, thuần/testable) + `dagster_project/sensors.py`
(mới, 4 sensor) + mở rộng `NotifierResource.send_alert()` + lịch 12:00/18:00
(`dagster_project/schedules.py`) + 3 khoá config mới + 3 biến môi trường mới.

### 15.1 Kênh alert — đề bài để trống placeholder, đã hỏi

Đề bài để nguyên `<Telegram bot | Slack webhook>` chưa điền — dừng hỏi theo đúng rào chắn
AGENTS.md mục 5.7. `.env`/`.env.example` đã có sẵn `SLACK_WEBHOOK_URL` (rỗng) từ trước, nhưng
người dùng chọn **Telegram** — thêm biến MỚI `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` (đúng đề
bài gợi ý "token/URL ở biến môi trường TÊN BIẾN" do agent đặt tên).

### 15.2 Mâu thuẫn §7.4 (4 tên sensor) vs §18.3 (6 điều kiện, nhiệm vụ 3 yêu cầu bám sát) —
đã tự giải quyết, ghi rõ theo đúng rào chắn "báo cáo mâu thuẫn"

2 bảng KHÔNG khớp 1-1. Cách giải quyết (chi tiết + lý do đầy đủ nằm trong docstring
`dagster_project/sensors.py`, tóm tắt ở đây):

| Sensor (tên theo §7.4) | Điều kiện THẬT SỰ implement (theo §18.3, ưu tiên vì nhiệm vụ 3 nói rõ) |
|---|---|
| `run_failure_sensor` | "Dagster run failed" (Critical) |
| `freshness_sensor` | "mart_daily_digest rỗng" (Critical) — **KHÔNG phải ">26h"** của §7.4; SLA ">26h" đã có `FreshnessPolicy` (task 1.5), sensor đó CHƯA gắn gửi alert vì nhiệm vụ 1.5 chỉ "định nghĩa policy" |
| `quarantine_sensor` | "Quarantine rate > 10%" + "Anomaly bất kỳ §13.3" (quarantine rate CHÍNH LÀ 1 trong 6 kiểm tra đó, không phải điều kiện thứ 7) + "> 30% nguồn fail" (không sensor nào trong §7.4 được đặt tên riêng cho điều kiện này — gộp vào đây, cùng nguồn dữ liệu `mart_pipeline_health`, giữ đúng "4 sensor") |
| `cost_sensor` | "Cost tháng > 80% ngân sách" (Warning) — CHỈ cảnh báo, **KHÔNG** "tự chuyển sang model rẻ hơn" (§7.4 mô tả cho sensor này nhưng đó là auto-remediation, tính năng khác, nhiệm vụ 3 chỉ giao "điều kiện và mức") |

### 15.3 Ngưỡng anomaly — MỘT nguồn duy nhất, không định nghĩa lần 2 (rào chắn nhiệm vụ 3)

`src/intel_bot/observability/alerting.py::load_dbt_vars()` đọc THẲNG `dbt_project.yml` —
cùng 9 var task 1.4 đã tạo, không hardcode số nào ở Python. Đã cân nhắc đọc thẳng
`AssetCheckExecutionRecord` nội bộ của Dagster (kết quả 6 dbt test đã hiện thành Asset Check)
thay vì đọc lại `mart_pipeline_health` bằng SQL — API đó là `NamedTuple(LoadableBy)` nội bộ
lưu trữ (`DagsterInstance._event_storage_impl`), không dành cho code ngoài; chọn SQL tường
minh, rủi ro thấp hơn dù phải LẶP LẠI logic so sánh (không lặp số ngưỡng). Test
`test_load_dbt_vars_has_all_anomaly_thresholds_task_1_4` khẳng định 9 khoá đó khớp — đổi tên
var ở dbt mà quên sửa Python thì test này rớt ngay, không âm thầm sai lệch.

`> 30% nguồn fail` (§18.3) KHÔNG có var dbt tương ứng (task 1.4 không tạo — §13.3 không có
kiểm tra này) → khoá config MỚI `observability.source_fail_rate_threshold: 0.30`
(`config/app.yaml`) — đúng số §18.3 đã cho, không tự đặt số khác.

### 15.4 Chống spam (nhiệm vụ 4)

`context.cursor` của Dagster sensor — chuỗi JSON `{condition_key: iso_timestamp}` — Dagster
tự lưu vào **run storage của chính Dagster instance** (Postgres/SQLite backend cấu hình ở
`dagster.yaml`, KHÔNG phải biến Python trong bộ nhớ tiến trình) giữa các tick, **giữ nguyên
qua daemon restart** (đọc lại đúng cursor cũ khi daemon khởi động lại). Chỉ áp dụng cho 3
sensor POLLING (freshness/quarantine/cost) — `run_failure_sensor` KHÔNG cần: Dagster tự đảm
bảo cursor nội bộ riêng của run-status sensor gọi hàm ĐÚNG MỘT LẦN mỗi lần run fail thật (một
sự kiện, không phải trạng thái lặp lại mỗi tick), khác về bản chất với 3 sensor kia (poll lại
CÙNG một trạng thái mỗi 60s, sẽ spam nếu không tự chống lặp). `alert_dedup_window_hours`
(config/app.yaml, mặc định 6h) dùng chung cho cả 3.

### 15.5 API sensor đã verify thật (dagster==1.13.17, không đoán)

`dir(dagster)` lọc "sensor" → dùng `@run_failure_sensor` (built-in, khớp thẳng) + `@sensor`
(generic, tự viết polling) cho 3 sensor còn lại. Resource lấy qua `context.resources.<key>`
— mẫu chuẩn (KHÔNG phải API preview như `map_resolved_asset_specs` ở task 1.5).

### 15.6 Lịch bổ sung 12:00/18:00 (nhiệm vụ 6)

`ingest_only_job` (mới) chọn đúng `raw_rss, raw_github, articles_normalized, stg_articles` —
bổ sung `raw_github` (task 1.2, chưa tồn tại lúc §7.3 viết) vào nhóm "raw_*" cho nhất quán;
`articles_normalized` BẮT BUỘC có trong selection (thiếu thì `stg_articles` — VIEW đọc
`silver.articles` — luôn phản ánh dữ liệu CŨ, ingest thêm vô nghĩa). 2 schedule
(`midday_ingest_schedule` 12:00, `evening_ingest_schedule` 18:00) share 1 job, **giữ
`default_status` mặc định STOPPED** — đúng quy ước `daily_pipeline_schedule` đã có từ task
0.12 (chưa có Dagster daemon production, xem PROGRESS.md mục 5C) — không tự ý đổi quy ước
một lịch mà để lịch kia khác.

### 15.7 Verify THẬT đã làm được (không cần Telegram credentials)

- `dbt build` không đụng gì (task này không sửa dbt) — 60/60 vẫn PASS (chạy lại xác nhận).
- `uv run pytest tests/` → **282/282 pass** (266 trước + 16 mới `tests/test_alerting.py`,
  Postgres thật, chèn/dọn dữ liệu dải ngày 2025-01-xx tách biệt hoàn toàn dữ liệu thật).
  Phát hiện + tự sửa 1 lỗi thật khi viết test: `check_pipeline_health_anomalies()` (ingest
  count 3σ) ban đầu có thêm guard `baseline_stddev > 0` KHÔNG có trong
  `assert_ingest_count_no_anomaly.sql` (task 1.4) — lệch hành vi giữa SQL/Python dù cùng đọc
  chung ngưỡng, đã bỏ guard để khớp đúng dbt (baseline rock-solid + deviation thật vẫn là tín
  hiệu đáng báo, xem comment tại chỗ).
- `ruff check`/`ruff format --check`/`mypy --strict` sạch trên toàn bộ file mới + sửa.
- **`dagster dev` thật (port 3111) + GraphQL** (cùng cách task 1.5 đã verify freshness):
  `schedulesOrError` → đúng 3 lịch, cron `0 5 * * *`/`0 12 * * *`/`0 18 * * *`, status
  STOPPED cả 3 (đúng thiết kế 15.6). `sensorsOrError` → đúng 4 sensor
  (`run_failure_sensor`/`freshness_sensor`/`quarantine_sensor`/`cost_sensor`), status RUNNING
  cả 4 (`DefaultSensorStatus.RUNNING` — khác lịch, sensor cần chạy ngay để verify DONE WHEN
  trong cửa sổ 5 phút). Đã tắt server test sau khi verify (`taskkill`, xác nhận connection
  refused).

### 15.8 CHƯA verify được — chờ credentials thật, KHÔNG tự bịa để "cho xong"

`.env` thật: `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`/`DAGSTER_WEBSERVER_URL` đều **rỗng**
(đã tự kiểm tra bằng `grep`, không đoán) — người dùng chưa điền. `NotifierResource.send_alert()`
hiện chỉ log warning "chưa cấu hình đủ" và trả `False`, không gửi gì thật — an toàn (đã chạy
`dagster dev` thật ở 15.7 với sensor RUNNING, không có tin nhắn giả nào bị gửi do thiếu key).

**5 gạch đầu DONE WHEN CHƯA verify được vì lý do trên** (không phải code chưa xong — code đã
chạy được, tests xanh, chỉ thiếu input thật từ người dùng):
1. Cố ý fail một asset (sai credential DB) → nhận message thật trong 5 phút.
2. `DELETE` tạm `mart_daily_digest` → nhận alert Critical → khôi phục.
3. Kích Warning (quarantine/anomaly) → nhận alert đúng mức.
4. Bắn lại cùng điều kiện trong cửa sổ chống lặp → không gửi lần hai.
5. Heartbeat vẫn ping bình thường, độc lập kênh alert (heartbeat tự nó không đổi gì ở task
   này, vẫn dùng `HEARTBEAT_URL` sẵn có — rủi ro thấp, nhưng chưa tự chạy lại để xác nhận
   "không đổi" bằng số liệu thật kể từ khi thêm `send_alert()`).

Sẽ chạy verify thật ngay khi có `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` — không đánh dấu task
1.6 là "xong" cho tới lúc đó.

## 16. Verify THẬT task 1.6 (tiếp mục 15) — có credentials, 6/6 DONE WHEN đã xác nhận, 2 lỗi
thật phát hiện + đã sửa

Người dùng cung cấp `TELEGRAM_BOT_TOKEN` (dán trực tiếp trong chat — đã ghi thẳng vào `.env`,
KHÔNG lặp lại token trong bất kỳ output nào sau đó) + `TELEGRAM_CHAT_ID` (lấy qua
`getUpdates` sau khi người dùng nhắn `/start` cho bot `@quybruno_bot` — lần thử đầu với ID
đoán trước đó báo "chat not found", đúng như dự đoán vì bot chưa có phiên chat).

### 16.1 Lỗi thật #1 — httpx tự log token qua URL request

Test gửi thật lần đầu (qua đúng code path `NotifierResource.send_alert()`, không phải curl)
gửi thành công (HTTP 200, tin nhắn tới thật), NHƯNG log `INFO:httpx:HTTP Request: POST
https://api.telegram.org/bot<token>/sendMessage ...` — **httpx tự log nguyên URL ở mức INFO,
và token Telegram nằm NGAY TRONG URL** (thiết kế của Telegram Bot API, không phải lựa chọn ở
đây). Code tự viết không hề log token, nhưng đây vẫn là lộ thật nếu logger `"httpx"` ở mức
INFO trở xuống — vi phạm đúng rào chắn "KHÔNG log token". Sửa: `send_alert()` nâng tạm mức
logger `"httpx"` lên WARNING chỉ trong lúc gọi (`_suppress_httpx_url_logging()`), khôi phục
ngay sau — KHÔNG đổi cấu hình logging toàn cục (RSS/github fetcher vẫn log INFO httpx bình
thường, URL của chúng không chứa secret). Verify lại: gửi thật lần 2 → chỉ còn dòng log sạch
"Đã gửi alert Telegram thành công.", không còn URL/token nào trong output.

### 16.2 Lỗi thật #2 — `run_failure_sensor` crash vì thiếu required_resource_keys

Cố ý fail thật một asset (`articles_normalized`, `--config` override
`resources.postgres.config.database_url` thành cổng không tồn tại 5499 — cách CHÍNH THỐNG
của Dagster để ép resource lỗi cho một run cụ thể, KHÔNG phải sửa `.env`/mã nguồn) → run FAIL
thật (lỗi kết nối Postgres thật từ `psycopg`) → `run_failure_sensor` tick **FAILURE**, lỗi
`DagsterUnknownResourceError: Unknown resource 'notifier'`. Nguyên nhân: `@run_failure_sensor`
(khác `@sensor`) **KHÔNG có tham số `required_resource_keys`** (verify bằng
`inspect.getsource(dagster.run_failure_sensor)` — không suy đoán từ tài liệu), nên
`context.resources.notifier` (đã dùng y hệt 3 sensor kia) không resolve được. Sửa: tự dựng
`NotifierResource` thẳng từ biến môi trường bên trong hàm (giống hệt cách `definitions.py`
dựng nó), không phụ thuộc cơ chế inject resource của decorator này.

**Gotcha môi trường phát hiện thêm khi test lỗi #2 (ghi lại cho lần sau):** `dagster dev`
không set `DAGSTER_HOME` mặc định dùng thư mục TẠM (`.tmp_dagster_home_<random>`, tự xoá khi
process thoát — thấy rõ qua log "This will be removed when dagster dev exits"). Một tiến
trình `dagster asset materialize` CLI chạy RIÊNG (không set `DAGSTER_HOME` trỏ đúng thư mục
đó) sẽ dùng MỘT instance KHÁC — `run_failure_sensor` của `dagster dev` đang chạy sẽ KHÔNG
BAO GIỜ thấy run đó fail. Phải set `DAGSTER_HOME` của lệnh CLI trỏ ĐÚNG thư mục instance mà
`dagster dev` đang dùng (hoặc dùng một `DAGSTER_HOME` cố định, tự tạo thư mục trước — Dagster
KHÔNG tự tạo thư mục nếu biến này trỏ tới đường dẫn chưa tồn tại, đã tự verify bằng
`DagsterInvariantViolationError` thật) để hai bên chia sẻ chung run storage.

### 16.3 6/6 DONE WHEN — verify thật, có bằng chứng cụ thể (tick history GraphQL + log dòng lệnh)

1. **Cố ý fail asset → message thật trong 5 phút, có asset/partition/run link:** verify 2
   lần (lần 1 bắt được lỗi #2 ở trên, lần 2 sau khi sửa) — log dev server dòng
   `22:24:46 ... run_failure_sensor - Đã gửi alert Telegram thành công.` ngay sau
   `Sensor "run_failure_sensor" acted on run status FAILURE của run <run_id>` — từ lúc
   trigger fail (~22:20) tới lúc gửi (~22:24:46) khoảng 4 phút, trong hạn 5 phút. Nội dung
   message (đọc lại code `run_failure_alert_sensor`): job name, `step_keys` từ
   `get_step_failure_events()`, `context.partition_key`, link `{DAGSTER_WEBSERVER_URL}/runs/{run_id}`.
2. **`DELETE` `mart_daily_digest` → Critical → khôi phục:** xoá thật 27 dòng, tick
   `freshness_sensor` kế tiếp ghi cursor `{"mart_daily_digest_empty": "2026-08-12T22:02:00..."}`
   (trước đó các tick đều "có dữ liệu, không có gì bất thường") → khôi phục bằng
   `dbt build --select mart_daily_digest` (rebuild từ `fct_article_score`, KHÔNG cần backup
   thủ công vì mart này luôn được dbt tái tạo toàn bộ). **Lưu ý số liệu:** rebuild ra 15 dòng
   thay vì 27 — KHÔNG phải mất dữ liệu (`fct_article_score` vẫn nguyên 77 dòng, verify trực
   tiếp), mà do cửa sổ 48h của `mart_daily_digest` là cửa sổ TRƯỢT theo `current_timestamp`
   thật (§5.8/§11.2) — vài giờ trôi qua thật trong phiên làm việc khiến batch bài cũ hơn (12
   bài baseline từ trước) trôi ra khỏi cửa sổ 48h, đúng thiết kế "luôn phản ánh trạng thái
   HIỆN TẠI", không phải lỗi của việc xoá/khôi phục.
3. **Warning (quarantine) → đúng mức:** chèn dòng thật `quarantine_rate=0.30` (> ngưỡng
   0.10) → tick `quarantine_sensor` ghi cursor `{"quarantine_rate_high": "2026-08-12T21:57:55..."}`.
4. **Bắn lại cùng điều kiện trong cửa sổ chống lặp → không gửi lần 2:** verify qua 3 tick
   liên tiếp sau đó (~21:58:55, ~22:00:35, ~22:01:35 — cách nhau đúng `minimum_interval_seconds=60`),
   điều kiện vẫn đúng (chưa xoá dòng test) nhưng cursor GIỮ NGUYÊN timestamp gửi ban đầu cả
   3 lần — chống lặp hoạt động đúng qua thời gian thật, không phải suy đoán từ code.
5. **Heartbeat vẫn ping bình thường, độc lập kênh alert:** gọi thật `ping_heartbeat()` sau
   khi đã thêm `send_alert()` → `HTTP/1.1 200 OK` tới đúng `HEARTBEAT_URL` cũ, không đổi gì.
6. **Lịch 05:00/12:00/18:00 hiện trong Dagster UI:** đã verify ở mục 15.7 (GraphQL thật, 3
   schedule đúng cron, không đổi lại ở đây).

**Dọn dẹp sau verify:** xoá dòng test `mart_pipeline_health` (2026-08-20), xoá thư mục
`DAGSTER_HOME` tạm dùng để test (`.dagster_home_verify`), tắt server test (`taskkill`, xác
nhận connection refused). `uv run pytest tests/` → **282/282 pass** không đổi. `ruff check`/
`mypy --strict` sạch 43 file (`src/`, `dagster_project/`, `tests/`).

**Task 1.6 chính thức HOÀN THÀNH — cả 6/6 DONE WHEN đã verify bằng dữ liệu/log thật, không
còn mục nào "chờ".**

## 17. Đã làm — 1.8 Test backfill + 1.9 CI (PRODUCTION_PLAN §17.1, §17.3, §20.1–20.4, §23.2,
§23.3) — CODE + VERIFY LOCAL XONG, CHỜ GITHUB PAT cho phần còn lại

Nhánh `feature/ci-backfill-pipeline`. Trước khi bắt đầu: commit hết việc tồn đọng của task
1.2/1.4/1.5/1.6 lên `main` (4 commit, xem mục 13–16), rồi mới tạo nhánh này — theo đúng lựa
chọn của user khi được hỏi.

### 17.1 Chia `tests/` thành `tests/unit` và `tests/integration`

Refactor cấu trúc thuần tuý qua `git mv` (Git giữ được rename detection) — KHÔNG sửa nội
dung test, không xoá test nào. Phân loại bằng cách grep từng file tìm
`db_connection`/`db_engine`/`DATABASE_URL`/`create_engine`: có dùng DB thật → `integration/`
(8 file + `fixtures/`), không đụng DB → `unit/` (12 file). 2 sửa CƠ HỌC bắt buộc (không phải
đổi test-logic): `TEMPLATES_DIR` trong `test_publish_html_renderer.py`/`test_publish_runner.py`
thiếu 1 cấp `.parent` sau khi file dời sâu hơn 1 thư mục. Verify: 282/282 pass sau khi dời,
`tests/unit` verify chạy được cả khi `env -u DATABASE_URL -u POSTGRES_*` (thật sự không phụ
thuộc DB).

### 17.2 CLI `pipeline` — từ placeholder thành lệnh thật

Chạy tuần tự ingest (RSS-only, khớp quyết định cũ ở task 1.2 rằng CLI `ingest` không gộp
GitHub) → normalize → filter → score → dbt build (marts) → publish cho một `--date`, dùng
LẠI đúng các hàm mà từng lệnh CLI riêng lẻ đã gọi (không viết lại business logic, P5) —
đường dự phòng khi Dagster không dùng được, KHÔNG thay thế Dagster (Dagster vẫn giữ lịch
chạy hằng ngày + 4 sensor alert). Refactor `score` để dùng chung `_build_provider()`/
`_score_and_summarize()` với `pipeline` thay vì copy-paste. Verify thật:
`pipeline --date 2026-08-12 --provider mock` chạy hết 6 bước trên Postgres dev thật,
`gold.mart_daily_digest` giữ nguyên 15 dòng, không rò placeholder mock vào gold.

### 17.3 Test backfill tự động (`tests/integration/test_backfill.py`) — CHƯA từng tồn tại

Chạy TOÀN BỘ đường ống thật (ingest mock qua `httpx.MockTransport` → normalize → filter →
score mock → dbt build) trên Postgres THẬT, 2 partition riêng biệt hoàn toàn dữ liệu thật
(2019-06-14/15). `now` CỐ ĐỊNH xuyên suốt 2 lần chạy CÙNG một partition — tách bạch đúng
"đổi vì mất tính lũy đẳng" khỏi "đổi vì thiết kế" (§8.2 cold-start so tuổi bài với `now` THẬT
— cạm bẫy đã gặp ở 0.12). 3 assertion đúng DONE WHEN: (1) materialize D, đếm số dòng THẬT ở
mọi bảng bronze/silver/gold liên quan, sanity-check >0; (2) materialize LẠI D → count không
đổi; (3) materialize D-1 → D không đổi, D-1 có dữ liệu riêng khác 0.

**Bug thật phát hiện qua assertion (2):** `upsert_silver_article()` (`src/intel_bot/ingest/loader.py`)
tính lại `first_seen_date` bằng `LEAST(first_seen_at, EXCLUDED.first_seen_at)::date` — suy
từ THỜI ĐIỂM FETCH THẬT thay vì so trực tiếp cột `first_seen_date` (nhãn partition, độc lập
thời gian thật). Vô hại vận hành hằng ngày (`ingest_date` luôn trùng ngày thật), nhưng SAI
khi backfill: re-normalize partition CŨ khiến `first_seen_date` "nhảy" về ngày chạy THẬT —
phá tính lũy đẳng (P1) đúng lúc backfill. Báo cho user qua `AskUserQuestion` (không tự sửa
rồi coi như xong, đúng rào chắn "test đỏ là thông tin, không phải phiền phức") — user chọn
sửa ngay: so trực tiếp 2 cột `first_seen_date` cũ/mới thay vì suy từ `first_seen_at`. Đã
kiểm tra dữ liệu prod hiện có (95 dòng `first_seen_at::date != first_seen_date`, chủ yếu
GitHub) — xác định đây là hành vi INSERT hợp lệ (nhãn partition khác ngày chạy thật lúc
verify trước đó), KHÔNG phải hậu quả của bug này, KHÔNG tự sửa lùi vì thiếu audit trail.

**Lỗi thiết kế test tự phát hiện (assertion 3), SAU khi sửa bug trên:** D và D-1 ban đầu
dùng CHUNG 1 fixture RSS → cùng `canonical_url` → khi xử lý D-1, `LEAST(first_seen_date)`
(đã sửa đúng) hợp lệ gán lại nhãn bài đó từ D về D-1 sớm hơn — ĐÚNG hành vi dedup cấp 1 cho
CÙNG một bài xuất hiện 2 ngày, KHÔNG phải vi phạm isolation. Sửa bằng cách tạo fixture thứ 2
nội dung khác hẳn (`sample_valid_2.xml`, `canonical_url` không trùng) cho D-1 — KHÔNG nới
assertion. Sau sửa: PASS cho đúng lý do.

Verify cuối: `uv run pytest tests/integration/test_backfill.py` PASS (37s), full suite
**283/283 pass** (282 cũ + 1 mới), teardown dọn sạch (đếm lại = 0 ở cả 3 bảng
bronze/silver/gold cho 2 ngày test). `ruff`/`ruff format`/`mypy --strict` sạch.

Tiện thể mang luôn fix ép UTF-8 subprocess (`PYTHONUTF8`/`PYTHONIOENCODING`, phát hiện khi
viết helper `_dbt_build` của test) ngược vào `cli.py::_run_dbt_build` — không dựa vào tiến
trình cha nhớ set biến này trước khi gọi `pipeline`/`score`.

### 17.4 `.github/workflows/ci.yml` — 9 bước theo bảng §17.1 + gitleaks (§23.2)

Xác nhận tên lệnh `dagster definitions validate` đúng trên dagster 1.13.17 đang cài
(`uv run dagster definitions --help`) — có cảnh báo superseded bởi `dg check defs` nhưng
lệnh cũ vẫn chạy đúng, dùng lệnh cũ vì `dg` chưa phải dependency của repo.

Thứ tự chạy trong workflow ưu tiên phụ thuộc thật (bảng §17.1 liệt kê CÁC KIỂM TRA bắt
buộc, không phải thứ tự tuần tự bắt buộc) — `alembic upgrade head` chạy SỚM (ngay sau
mypy), không phải cuối cùng, vì sqlfluff (templater dbt), pytest integration, và
dbt parse/compile đều cần schema đã tồn tại trên service container Postgres mới tinh của
CI. gitleaks chạy job riêng, độc lập Python/DB.

Rào chắn cứng đã thoả bằng cấu trúc, không phải lời hứa: KHÔNG set `DEEPSEEK_API_KEY`/
`HEARTBEAT_URL`/`TELEGRAM_*`/`SLACK_WEBHOOK_URL` trong `env:` của job → code P4 sẵn có (đã
verify thật ở task 1.6) tự log warning và bỏ qua, không gọi mạng thật. KHÔNG có
`dbt build --full-refresh` nào trong workflow — `dbt build` thật (mock) duy nhất nằm trong
`test_backfill.py`, chạy trên service container tạm của CI, không phải DB thật.

**Phạm vi lint (bước 2/3) cố ý giới hạn** `src/ dagster_project/ tests/ alembic/env.py` —
code pipeline đang bảo trì — KHÔNG phải toàn repo: `ruff check .`/`ruff format --check .`
trên toàn repo phát hiện lỗi ở `alembic/versions/*.py` (boilerplate tự sinh của alembic khi
`revision --autogenerate`, kiểu `Optional`/`Sequence` từ template mặc định, chưa từng sửa
tay sau khi sinh) và `spike/spike.py` (đã ghi rõ ở mục 10: "CODE SPIKE MỘT LẦN — KHÔNG THUỘC
PIPELINE CHÍNH THỨC", giữ vì giá trị lịch sử, không bảo trì theo chuẩn code pipeline). Đây
là quyết định về PHẠM VI kiểm tra, không phải nới RULESET — ruleset (`ruff`/`mypy --strict`)
giữ nguyên, không sửa `pyproject.toml`/thêm `# type: ignore`. Tiện thể dọn 2 file format lệch
từ trước (không liên quan việc đang làm, thuần line-wrap, không đổi hành vi):
`alembic/env.py`, `src/intel_bot/db/bronze.py` — để bước 2 xanh ngay từ PR đầu tiên.

**Dry-run cả 9 bước THẬT trên máy dev trước khi push** (Postgres thật, DB tạm riêng
`ci_test_tmp` cho bước alembic để mô phỏng đúng "service container mới tinh"), thời gian đo
được:

| Bước | Lệnh | Thời gian |
|---|---|---|
| 2 | `ruff check` + `ruff format --check` (scope ở trên) | ~2-3s |
| 3 | `mypy --strict src/` (28 file) | <5s |
| 4 | `alembic upgrade head` (DB trống → 4 migration) | 0.94s |
| 5 | `sqlfluff lint dbt_project/` | vài giây (đã trừ nhiễu 683 warning từ `target/` cũ, gitignored, sẽ không xuất hiện trên checkout sạch của CI) |
| 6 | `pytest tests/unit` | 230 passed, 3.37s |
| 7 | `pytest tests/integration` | 53 passed, 39.80s |
| 8 | `dbt parse` + `dbt compile` | 6.5s + 5.1s |
| 9 | `dagster definitions validate` | 4.57s |

Tổng ước tính runner CI (không tính setup Python/uv + cold cache): dưới 2 phút.

**gitleaks dry-run cục bộ** (docker chưa cài binary gitleaks native, dùng
`docker run --rm zricethezav/gitleaks:latest git -v .`): quét 25 commit, 1.77MB, **no leaks
found** — PR đầu tiên sẽ xanh ngay, không có secret nào lộ trong lịch sử hiện tại.

### 17.5 PAT + PR thật + 4 lỗi CI thật tìm được — chỉ lộ ra trên checkout sạch, không lộ ở
máy dev (đã tích luỹ state qua cả phiên làm việc)

User điền PAT (fine-grained, `repo` industry-intel-bot) vào `.env` biến `GITHUB_TOKEN` (biến
đã có sẵn tên, trước đó dành cho GitHub Search fetcher task 1.2, giờ dùng chung). 2 vòng sửa
quyền PAT trước khi push được — ghi lại vì dễ lặp lại nhầm lần sau:
1. Thiếu quyền → `403 Permission denied` khi push dù `/repos/.../permissions` báo
   `admin:true, push:true` — vì fine-grained PAT có mô hình quyền RIÊNG cho từng token, độc
   lập với quyền collaborator của tài khoản. Cần bật tường minh Contents/Pull requests/
   Administration ở trang sửa token.
2. Vẫn bị chặn → `refusing to allow a Personal Access Token to create or update workflow
   .github/workflows/ci.yml without workflow scope` — vì nhánh có sửa file trong
   `.github/workflows/`, quyền **Workflows** (khác **Actions** — dễ nhầm, Actions chỉ quản
   lý workflow *runs*) mới cho phép việc này.

`git push` qua PAT nhúng thẳng vào URL remote bị permission classifier của Claude Code auto
mode chặn (hành động outward-facing, khó đảo ngược) — user tự chạy `git push` bằng tay lần
đầu; các lần push sau (sau khi user đã xác nhận) chạy được qua Bash bình thường. Mở PR +
theo dõi CI + tải log lỗi làm qua REST API thẳng (`curl` + `Authorization: Bearer`), không
cần cài `gh` CLI — đủ dùng.

**PR #1** mở vào `main`: https://github.com/Quybuno/industry-intel-bot/pull/1 (5 commit ban
đầu, sau đó thêm 3 commit sửa lỗi CI thật, tổng 8 commit).

**Lần push đầu tiên CI đỏ ngay — và 3 vòng sửa tiếp theo cũng đỏ, mỗi vòng một lỗi THẬT khác
nhau, chỉ lộ ra trên GitHub Actions checkout sạch, KHÔNG lộ trên máy dev vì máy dev đã tích
luỹ `dbt_project/target/` + toàn bộ bảng/view gold từ rất nhiều lần `dbt build` thủ công
xuyên suốt phiên làm việc.** Đây đúng là giá trị của CI mà đề bài đặt ra ("PR sau này phá vỡ
tính lũy đẳng — không có gì ngăn" áp dụng y hệt cho "môi trường sạch phơi ra thứ máy dev che
giấu"). Mỗi lỗi đều: tái hiện được cục bộ trên DB tạm mới migrate (drop/create + alembic
upgrade head, không phải suy đoán từ log), sửa, xác nhận sửa đúng, rồi mới push:

1. **`pytest tests/unit` fail lúc COLLECT** — `tests/unit/test_dagster_definitions.py` import
   `dagster_project.definitions`, mà `dagster_project/assets/dbt_assets.py` đọc
   `dbt_project/target/manifest.json` ngay lúc import module (decorator-time, không phải lúc
   materialize). Workflow ban đầu chạy `dbt parse + compile` SAU `pytest tests/unit` — sai
   thứ tự. Sửa: đổi thứ tự, `dbt parse + compile` chạy sớm hơn.
2. **`dbt compile` tự vỡ** — `models/intermediate/int_articles_deduped.sql` gọi `run_query()`
   ngay trong thân model (mục đích: log số nhóm content_hash trùng), chạy THẬT lúc compile
   (Jinja `{% if execute %}`), SELECT từ `gold.stg_articles` — một VIEW dbt tự tạo, alembic
   không biết tới. DB tạm mới migrate chưa có view này. Sửa: thêm `dbt seed` +
   `dbt run --select staging` trước `dbt compile`.
3. **`pytest tests/integration` fail hàng loạt** — nhiều test SELECT/INSERT trực tiếp vào
   `gold.fct_article_score`/`mart_daily_digest`/`mart_pipeline_health` qua SQL thô (không qua
   dbt ref), những bảng này CHỈ tồn tại sau khi chạy `dbt build` thật. Sửa: nâng bước seeding
   từ `dbt run --select staging` lên `dbt build` đầy đủ (`--exclude assert_digest_not_empty`
   — test nghiệp vụ §13.2 đòi >=5 dòng, đúng cho production, sai bối cảnh cho một lần build
   0-dòng chỉ để tạo schema). `tests/integration/test_backfill.py` tự nó cũng dính đúng lỗi
   này ở bước `_dbt_build` mart riêng của nó (partition test chỉ vài bài mock, không bao giờ
   đạt ngưỡng 5 dòng) — thêm `--exclude` tương tự vào helper `_dbt_build` của test.
4. **`test_alerting.py::test_check_digest_empty_returns_none_when_digest_has_rows` fail** —
   test PHỤ THUỘC dữ liệu thật có sẵn của repo (docstring cũ ghi thẳng: "dữ liệu 2026-08 đã
   có từ các task trước") thay vì tự tạo fixture — đúng trên máy dev (luôn có data thật),
   SAI trên DB CI trống. Đây là lỗ hổng cô lập test có SẴN từ task 1.6 (không phải lỗi của
   1.8/1.9), nên KHÔNG tự sửa — báo cho user qua `AskUserQuestion` với 3 lựa chọn (tự viết
   fixture / loại riêng test này khỏi CI kèm ghi chú / dừng hẳn chờ user tự sửa 1.6). User
   chọn: viết fixture. Thêm `_insert_digest_row`/`_cleanup_digest_row` + fixture `digest_row`
   (đúng khuôn `clean_health_table` đã có sẵn cùng file, và đúng những gì docstring module đã
   ghi từ đầu nhưng code trước đó chưa làm theo).

**Kết quả cuối — run https://github.com/Quybuno/industry-intel-bot/actions/runs/31664879120:**
CI xanh 9/9 step, gitleaks xanh, tổng thời gian job CI **1 phút 50 giây** (03:45:29–03:47:19
UTC), gitleaks **10 giây**. Breakdown từng step (từ log thật, không phải ước tính):

| Step | Thời gian thật trên CI |
|---|---|
| Initialize containers (Postgres service) | 20s |
| Setup Python 3.12 + uv + install deps | ~4s |
| ruff check + format --check | <1s |
| mypy src/ | 8s |
| alembic upgrade head | 2s |
| dbt parse + seed + build + compile | 20s |
| sqlfluff lint dbt_project/ | 13s |
| pytest tests/unit | 6s |
| pytest tests/integration | 25s |
| dagster definitions validate | 4s |

Trước khi push MỖI lần sửa, đều tái hiện lỗi + xác nhận fix trên DB tạm cục bộ (drop/create
`ci_test_tmp`, `alembic upgrade head` từ đầu) — không đoán từ log CI rồi sửa mù.
