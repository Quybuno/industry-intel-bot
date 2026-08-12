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

`pipeline`, `eval` vẫn là placeholder (`typer.echo("... (placeholder)")`).

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
