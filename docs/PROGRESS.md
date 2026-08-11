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
| 0.11 | Publish | ⬜ CHƯA LÀM | Xem mục 5B |

Lệnh CLI đã có thật (chạy bằng `uv run python -m src.intel_bot.cli <lệnh>` — xem mục 3.4
về lý do không dùng `uv run intel-bot`):

```
ingest --date YYYY-MM-DD
validate-sources
normalize --date YYYY-MM-DD
filter --date YYYY-MM-DD
score --date YYYY-MM-DD --provider mock|deepseek
doctor
```

`publish`, `pipeline`, `eval` vẫn là placeholder (`typer.echo("... (placeholder)")`).

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

### 3.4 `uv run intel-bot <cmd>` KHÔNG chạy được — lỗi packaging từ task 0.1

`pyproject.toml` map wheel `src/intel_bot` → `intel_bot`, nhưng TOÀN BỘ codebase import
kiểu `from src.intel_bot.xxx import yyy`. Console-script entry point
(`intel-bot = "src.intel_bot.cli:main"`) vì vậy luôn báo
`ModuleNotFoundError: No module named 'src'`. Workaround dùng xuyên suốt dự án:
`uv run python -m src.intel_bot.cli <lệnh>` (chạy từ repo root). Chưa có task nào sửa gốc
rễ (đổi toàn bộ import sang `intel_bot.xxx` hoặc sửa lại wheel mapping) — đây là việc dọn
dẹp còn treo, không thuộc phạm vi bất kỳ task nào đã giao.

## 4. Code v1 legacy còn trong repo — KHÔNG dùng, chỉ để import không vỡ

Scaffold task 0.1 là kiến trúc ORM/SQLite hoàn toàn khác (không phân tầng bronze/silver).
Từ task 0.4 trở đi, mỗi lần một module v2 (Core, không ORM) thay thế module v1 cùng tên
chức năng, phần v1 bị tách sang file `legacy_*.py` hoặc giữ nguyên KHÔNG SỬA, chỉ để các
import chưa dọn không vỡ. Danh sách file legacy, an toàn xoá ở một task dọn dẹp riêng
(chưa ai giao việc này):

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
  `git checkout` lại). Format từng file cụ thể, không format nguyên thư mục.

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

## 5B. Việc tiếp theo — 0.11 (publish)

- Đọc `gold.mart_daily_digest`, publish JSON + HTML, KHÔNG business logic trong Python
  (§12.1 — mọi logic đã nằm ở dbt).
- Cân nhắc luôn việc dọn `composite.py`/`runner.py` nêu ở 5A nếu 0.11 đụng tới luồng score
  trước khi publish; nếu không, để lại rõ ràng cho 0.12.

## 6. Trạng thái DB dev hiện tại (lúc viết file này — sẽ lạc hậu, tự query lại)

```
bronze.raw_articles: 92        (ingest_date=2026-08-10, 8 nguồn thật)
silver.articles: 92            (57 scored, 35 excluded, 0 eligible)
silver.article_scores: 69      (45 provider=mock cost=0, 12 provider=deepseek-v4-flash chi phí thật)
silver.article_summaries: 27
silver.score_quarantine: 0
gold.dim_source: 8             (SCD2 từ snap_sources, tất cả is_current=true — chưa có đổi tier)
gold.fct_article_score: 57     (== số bài scored ở silver, dedup cấp 2 chưa gộp bản nào)
gold.mart_daily_digest: 57     (cửa sổ 48h theo first_seen_at, tính từ lúc build — sẽ giảm dần theo thời gian)
gold.mart_pipeline_health: 2   (2026-08-10: ingest/filter; 2026-08-11: score — pipeline_date là trục lịch chung, xem 5A)
```
Dữ liệu deepseek là request thật, tốn tiền thật (rất nhỏ, ~$0.008). Đừng chạy lại
`--provider deepseek` trên diện rộng chỉ để test — dùng `--provider mock`.

## 7. Config đã điền thật (không phải placeholder)

| File | Trạng thái |
|---|---|
| `config/models.yaml` | provider `deepseek` có giá thật (xác minh 2026-08-11), `ollama`/`openai` chưa dùng tới |
| `config/sources.yaml` | 8 nguồn RSS đã verify HTTP 200 thật |
| `config/rubric.yaml` | Rubric 4 tiêu chí, mốc 1/5/10 |
| `config/keywords.yaml` | `blocklist:` (v2, đang dùng) + `groups:` (v1 legacy) |
| `.env` | Có `DATABASE_URL`, `DEEPSEEK_API_KEY` thật (gitignored) — `.env.example` chỉ có placeholder rỗng |

## 8. Test

213 test Python, `uv run pytest tests/` — tất cả dùng Postgres THẬT (docker, cổng 5435) cho
phần integration, KHÔNG mock DB; chỉ mock mạng (`httpx.MockTransport` hoặc `MockProvider`).
Không cần biến môi trường nào để chạy phần contract/mock (task 0.7 trở đi tự chứng minh
bằng `env -i`). `ruff`/`mypy --strict` chỉ chạy sạch trên file đã viết ở task 0.2 trở đi —
code legacy (mục 4) còn nợ lint, không nằm trong phạm vi bất kỳ task nào.

Riêng dbt (task 0.10): 35 data test qua `dbt test`/`dbt build` (`--project-dir dbt_project`,
cần `DBT_PROFILES_DIR=dbt_project` hoặc `--profiles-dir dbt_project`) — độc lập với 213 test
Python ở trên, không chạy qua `pytest`. `sqlfluff lint dbt_project --dialect postgres` sạch
(macro bị `sqlfluff-templater-dbt` tự skip — giới hạn đã biết của templater, không phải lỗi).
