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
| 0.10 | dbt: staging + gold | ⬜ CHƯA LÀM | Xem mục 5 — có phụ thuộc ngược vào 0.8/0.9 |
| 0.11 | Publish | ⬜ CHƯA LÀM | |

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

## 5. Việc tiếp theo — 0.10 (dbt) và 0.11 (publish)

**0.10 cần:**
- Thêm `dbt-core` + `dbt-postgres` vào `pyproject.toml` (chưa có — dừng và hỏi nếu cần
  version cụ thể, theo đúng tinh thần AGENTS.md mục 4).
- `gold.dim_source` (SCD2) từ `config/sources.yaml` (đọc `tier`, `industries`).
- `gold.fct_article_score` — công thức THẬT §5.7 (credibility 80/20 blend, recency, depth
  weight 0). Sau khi xong, xoá `composite.py`, sửa `runner.py` (xem mục 2.3).
- `gold.mart_daily_digest`, `gold.mart_pipeline_health` (§5.8).
- KHÔNG động vào bronze/silver — chỉ đọc.

**0.11 cần:**
- Đọc `gold.mart_daily_digest`, publish JSON + HTML, KHÔNG business logic trong Python
  (§12.1 — mọi logic đã nằm ở dbt).

## 6. Trạng thái DB dev hiện tại (lúc viết file này — sẽ lạc hậu, tự query lại)

```
bronze.raw_articles: 92        (ingest_date=2026-08-10, 8 nguồn thật)
silver.articles: 92            (57 scored, 35 excluded, 0 eligible)
silver.article_scores: 69      (45 provider=mock cost=0, 12 provider=deepseek-v4-flash chi phí thật)
silver.article_summaries: 27
silver.score_quarantine: 0
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

213 test, `uv run pytest tests/` — tất cả dùng Postgres THẬT (docker, cổng 5435) cho phần
integration, KHÔNG mock DB; chỉ mock mạng (`httpx.MockTransport` hoặc `MockProvider`).
Không cần biến môi trường nào để chạy phần contract/mock (task 0.7 trở đi tự chứng minh
bằng `env -i`). `ruff`/`mypy --strict` chỉ chạy sạch trên file đã viết ở task 0.2 trở đi —
code legacy (mục 4) còn nợ lint, không nằm trong phạm vi bất kỳ task nào.
