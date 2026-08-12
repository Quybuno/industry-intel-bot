"""Asset tầng silver (task 0.12 mục 1, PRODUCTION_PLAN §7.2) — bọc lại các hàm CLI đã có ở
task 0.5/0.6/0.8/0.9, không viết lại logic nghiệp vụ nào.

`articles_normalized` KHÔNG có trong bảng asset gốc §7.2 (bảng chỉ có raw_rss ->
stg_articles -> articles_filtered) — bổ sung có chủ đích: `stg_articles` (dbt) là VIEW
CHỈ đổi tên cột/ép kiểu (§11.2, task 0.10), không có business logic; dedup cấp 1 + chuẩn
hoá URL + quy tắc cold-start (§8.2-8.4) là business logic THẬT, đã tồn tại từ task 0.5 ở
`normalize_partition()`, không thể nhét vào dbt view và không thể bỏ qua. Không có asset
này thì `silver.articles` (nguồn của view `stg_articles`) sẽ luôn rỗng. Xem docs/PROGRESS.md
mục 5C để biết lý do đầy đủ.

**D1 (dọn nợ kỹ thuật) — `article_scores`/`article_summaries` KHÔNG còn là một multi_asset
gộp chung.** Trước đây một lần gọi `run_score_partition()` sinh cả điểm lẫn tóm tắt top-K
(dùng công thức composite TẠM `composite.py`, đã xoá — xem PROGRESS.md mục 9 D1). Giờ top-K
đọc composite score CHÍNH THỨC (§5.7) từ `gold.fct_article_score` (dbt), nên `article_scores`
phải ghi xong TRƯỚC, dbt build `fct_article_score` chạy TIẾP, rồi `article_summaries` mới
đọc được — hai asset Python tách rời, `article_summaries` khai `deps=["fct_article_score"]`
(LỆCH bảng §7.2 gốc — bảng đó ghi `article_summaries` phụ thuộc `article_scores` trực tiếp,
nhưng đó là khoảng trống rút gọn giống `articles_normalized` ở trên, không phải lựa chọn sản
phẩm). Dagster tự lo thứ tự chạy đúng qua đồ thị `deps=` — không cần `internal_asset_deps`/
`multi_asset` nữa, và cũng KHÔNG cần code orchestration gọi dbt thủ công như bên CLI
(`cli.py::_run_dbt_build_for_fct_article_score`) vì `fct_article_score` đã là asset dbt thật
trong `daily_dbt_assets` (dagster-dbt tự subset-chạy dbt build đúng lúc).

**Không có `from __future__ import annotations`** — dagster kiểm tra kiểu tham số `context`
bằng so khớp class trực tiếp, không resolve forward-ref chuỗi; xem giải thích đầy đủ ở
`assets/bronze.py`.
"""

import datetime as dt
import time
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources.llm import LLMResource
from dagster_project.resources.postgres import PostgresResource
from src.intel_bot.config import load_config_dir
from src.intel_bot.filter.keyword_filter import FilterRules
from src.intel_bot.filter.loader import run_filter_partition
from src.intel_bot.ingest.loader import normalize_partition
from src.intel_bot.score.runner import (
    RunnerResult,
    run_score_partition,
    run_summarize_top_k_partition,
)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


@asset(
    key="articles_normalized",
    group_name="silver",
    partitions_def=daily_partitions,
    # task 1.2: thêm raw_github vào deps — normalize_partition() đọc bronze.raw_articles
    # theo ingest_date, không phân biệt source_type, nên xử lý được bản ghi github mà không
    # cần nhánh riêng (payload đã được github_fetcher.repo_to_payload() shape sẵn đúng khoá
    # title/link/summary/updated_parsed mà parse_entry() đọc — xem docstring
    # src/intel_bot/ingest/github_fetcher.py).
    deps=["raw_rss", "raw_github"],
    description=(
        "Chuẩn hoá + dedup cấp 1 bronze.raw_articles -> silver.articles (task 0.5). Không "
        "có trong bảng asset gốc §7.2 — xem docstring module."
    ),
)
def articles_normalized(
    context: AssetExecutionContext, postgres: PostgresResource
) -> MaterializeResult[Any]:
    """`ingest_date` LUÔN lấy từ partition key. `now` dùng cho quy tắc cold-start (§8.2 —
    so bài với thời điểm THẬT, không phải ngày partition) — khác mục đích với "ngày xử lý",
    không vi phạm rào chắn "không dùng datetime.now() để chọn ngày"."""
    ingest_date = dt.date.fromisoformat(context.partition_key)
    max_article_age_days = int(
        load_config_dir()
        .get("app", {})
        .get("normalize", {})
        .get("max_article_age_days", 7)
    )
    now = dt.datetime.now(tz=VN_TZ)

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = normalize_partition(
            connection,
            ingest_date=ingest_date,
            max_article_age_days=max_article_age_days,
            now=now,
        )
    duration_seconds = time.monotonic() - started_at

    return MaterializeResult(
        metadata={
            "rows_read": MetadataValue.int(result.read),
            "rows_inserted": MetadataValue.int(result.inserted),
            "rows_updated": MetadataValue.int(result.updated),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )


@asset(
    key="articles_filtered",
    group_name="silver",
    partitions_def=daily_partitions,
    deps=["stg_articles"],
    description=(
        "Filter tối thiểu (§9.2): đọc silver.articles theo ngày, ghi status/filter_score/"
        "exclusion_reason (task 0.6)."
    ),
)
def articles_filtered(
    context: AssetExecutionContext, postgres: PostgresResource
) -> MaterializeResult[Any]:
    """`filter_date` LUÔN lấy từ partition key."""
    filter_date = dt.date.fromisoformat(context.partition_key)

    cfg = load_config_dir()
    filter_cfg = cfg.get("app", {}).get("filter", {})
    normalize_cfg = cfg.get("app", {}).get("normalize", {})
    blocklist = cfg.get("keywords", {}).get("blocklist", [])
    if not blocklist:
        raise Failure("Thiếu config keywords.yaml: blocklist — không tự bịa danh sách.")

    rules = FilterRules(
        max_article_age_days=int(normalize_cfg.get("max_article_age_days", 7)),
        min_snippet_chars=int(filter_cfg.get("min_snippet_chars", 80)),
        blocklist_keywords=tuple(blocklist),
        max_articles_per_day=int(filter_cfg.get("max_articles_per_day", 200)),
        now=dt.datetime.now(tz=VN_TZ),
    )

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = run_filter_partition(connection, filter_date=filter_date, rules=rules)
    duration_seconds = time.monotonic() - started_at

    return MaterializeResult(
        metadata={
            "rows_read": MetadataValue.int(result.read),
            "eligible": MetadataValue.int(result.eligible),
            "excluded": MetadataValue.int(result.excluded),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )


@asset(
    key="article_scores",
    group_name="silver",
    partitions_def=daily_partitions,
    deps=["articles_filtered"],
    description="Chấm điểm 4 tiêu chí + tags cho bài eligible (task 0.8, §10).",
)
def article_scores(
    context: AssetExecutionContext, postgres: PostgresResource, llm: LLMResource
) -> MaterializeResult[Any]:
    """`partition_date` LUÔN lấy từ partition key. `now` dùng cho `scored_at`/recency —
    mốc thời điểm THẬT của lần chấm này, không phải ngày partition.

    D1: KHÔNG còn tự tóm tắt top-K (khác task 0.8/0.9) — xem asset `article_summaries`."""
    partition_date = dt.date.fromisoformat(context.partition_key)
    now = dt.datetime.now(tz=VN_TZ)

    score_cfg = load_config_dir().get("app", {}).get("score", {})
    daily_budget_usd = Decimal(str(score_cfg.get("daily_budget_usd", "1.00")))

    provider, pricing, batch_size = llm.build()

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = run_score_partition(
            connection,
            partition_date=partition_date,
            provider=provider,
            pricing=pricing,
            daily_budget_usd=daily_budget_usd,
            batch_size=batch_size,
            now=now,
        )
    duration_seconds = time.monotonic() - started_at

    if result.provider_unavailable:
        raise Failure(
            "Provider không dùng được giữa chừng (§10.5) — xem log để biết chi tiết."
        )

    score_metadata: dict[str, MetadataValue] = {
        "scored": MetadataValue.int(result.scored),
        "quarantined": MetadataValue.int(result.quarantined),
        "cost_usd": MetadataValue.float(float(result.total_cost_usd)),
        "budget_stopped": MetadataValue.bool(result.budget_stopped),
        "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
    }
    if result.latency_p50_ms is not None:
        score_metadata["latency_p50_ms"] = MetadataValue.float(result.latency_p50_ms)
    if result.latency_p95_ms is not None:
        score_metadata["latency_p95_ms"] = MetadataValue.float(result.latency_p95_ms)

    return MaterializeResult(metadata=score_metadata)


@asset(
    key="article_summaries",
    group_name="silver",
    partitions_def=daily_partitions,
    # D1: KHÔNG còn deps=["article_scores"] trực tiếp — top-K đọc composite score CHÍNH
    # THỨC (§5.7) từ gold.fct_article_score (dbt), nên phải đợi dbt build model đó xong,
    # không chỉ đợi article_scores (Python) ghi xong. Dagster tự suy đúng thứ tự chạy
    # (article_scores -> fct_article_score -> article_summaries) từ đồ thị deps= — không
    # cần gọi dbt thủ công như cli.py vì fct_article_score đã là asset dbt thật.
    deps=["fct_article_score"],
    description=(
        "Tóm tắt 5 bullet tiếng Việt cho top-K theo composite CHÍNH THỨC từ "
        "gold.fct_article_score (D1, §5.7 — khác task 0.8 dùng composite tạm)."
    ),
)
def article_summaries(
    context: AssetExecutionContext, postgres: PostgresResource, llm: LLMResource
) -> MaterializeResult[Any]:
    """`partition_date` LUÔN lấy từ partition key. `now` dùng cho `created_at`, không phải
    ngày partition."""
    partition_date = dt.date.fromisoformat(context.partition_key)
    now = dt.datetime.now(tz=VN_TZ)

    score_cfg = load_config_dir().get("app", {}).get("score", {})
    daily_budget_usd = Decimal(str(score_cfg.get("daily_budget_usd", "1.00")))
    top_k_summaries = int(score_cfg.get("top_k_summaries", 15))

    provider, pricing, _batch_size = llm.build()

    started_at = time.monotonic()
    result = RunnerResult()
    with postgres.get_connection() as connection:
        run_summarize_top_k_partition(
            connection,
            partition_date=partition_date,
            provider=provider,
            pricing=pricing,
            daily_budget_usd=daily_budget_usd,
            top_k_summaries=top_k_summaries,
            now=now,
            result=result,
        )
    duration_seconds = time.monotonic() - started_at

    if result.provider_unavailable:
        raise Failure(
            "Provider không dùng được giữa chừng (§10.5) — xem log để biết chi tiết."
        )

    return MaterializeResult(
        metadata={
            "summarized": MetadataValue.int(result.summarized),
            "summary_quarantined": MetadataValue.int(result.summary_quarantined),
            "cost_usd": MetadataValue.float(float(result.total_cost_usd)),
            "budget_stopped": MetadataValue.bool(result.budget_stopped),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )
