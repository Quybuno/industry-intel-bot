"""Asset `raw_rss`/`raw_github` (bronze, daily) — bọc lại `run_rss_ingest()` (task 0.4) và
`run_github_ingest()` (task 1.2, PRODUCTION_PLAN §7.2, §8.1) đã có sẵn. KHÔNG viết lại logic
fetch/parse/dedup cấp 0 — asset chỉ đọc config, mở connection qua resource, gọi đúng hàm đã
verify thật ở tầng `src/intel_bot/ingest/`.

`raw_github` được để lại Phase 1 ở task 0.12 (đúng rào chắn lúc đó) — nay thêm ở task 1.2.

**Không có `from __future__ import annotations`** (khác quy ước còn lại của repo) — dagster
kiểm tra kiểu tham số `context` bằng cách so khớp trực tiếp object class, không resolve
chuỗi forward-ref; bật future import làm annotation thành chuỗi và dagster báo lỗi sai
("context phải là AssetExecutionContext...") dù đã đúng kiểu. Đã verify bằng cách bật/tắt
dòng import này và chạy lại — không phải suy đoán.
"""

import asyncio
import datetime as dt
import os
import time
from typing import Any

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.partitions import daily_partitions
from dagster_project.resources.postgres import PostgresResource
from src.intel_bot.config import load_config_dir
from src.intel_bot.ingest.github_fetcher import (
    load_github_query_configs,
    run_github_ingest,
)
from src.intel_bot.ingest.rss_fetcher import load_source_configs, run_rss_ingest


@asset(
    key="raw_rss",
    group_name="bronze",
    partitions_def=daily_partitions,
    description=(
        "Fetch RSS từ config/sources.yaml, ghi bronze.raw_articles + silver.source_health "
        "(task 0.4)."
    ),
)
def raw_rss(
    context: AssetExecutionContext, postgres: PostgresResource
) -> MaterializeResult[Any]:
    """Ingest RSS cho đúng ngày của partition — KHÔNG dùng `datetime.now()` để chọn ngày."""
    ingest_date = dt.date.fromisoformat(context.partition_key)

    ingest_cfg = load_config_dir().get("app", {}).get("ingest", {})
    user_agent = ingest_cfg.get("user_agent")
    if not user_agent:
        raise Failure(
            "Thiếu config ingest.user_agent trong config/app.yaml — không tự bịa User-Agent."
        )
    timeout = float(ingest_cfg.get("timeout_seconds", 30))
    max_concurrent = int(ingest_cfg.get("max_concurrent_requests", 5))

    sources = load_source_configs(only_enabled=True)
    if not sources:
        raise Failure("Không có nguồn nào enabled trong config/sources.yaml.")

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = asyncio.run(
            run_rss_ingest(
                connection,
                sources,
                user_agent=user_agent,
                ingest_date=ingest_date,
                max_concurrent=max_concurrent,
                timeout=timeout,
            )
        )
    duration_seconds = time.monotonic() - started_at

    if result.failed_sources:
        context.log.warning(
            f"Nguồn lỗi khi ingest {ingest_date}: {result.failed_sources}"
        )

    return MaterializeResult(
        metadata={
            "rows_inserted": MetadataValue.int(result.rows_inserted),
            "entries_fetched": MetadataValue.int(result.total_entries_fetched),
            "sources_ok": MetadataValue.int(result.sources_ok),
            "sources_failed": MetadataValue.int(len(result.failed_sources)),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )


@asset(
    key="raw_github",
    group_name="bronze",
    partitions_def=daily_partitions,
    description=(
        "Fetch GitHub Search API (/search/repositories) từ config/github_sources.yaml, ghi "
        "bronze.raw_articles + silver.source_health (task 1.2)."
    ),
)
def raw_github(
    context: AssetExecutionContext, postgres: PostgresResource
) -> MaterializeResult[Any]:
    """Ingest GitHub cho đúng ngày của partition — KHÔNG dùng `datetime.now()` để chọn ngày.

    `GITHUB_TOKEN` rỗng vẫn chạy được (unauthenticated, hạn mức thấp hơn — xem docstring
    `run_github_ingest`) — khác `llm` resource (0.12), KHÔNG raise Failure nếu thiếu, vì
    GitHub Search API tự thân đã hỗ trợ chế độ này, không phải một cấu hình thiếu sót."""
    ingest_date = dt.date.fromisoformat(context.partition_key)

    ingest_cfg = load_config_dir().get("app", {}).get("ingest", {})
    user_agent = ingest_cfg.get("user_agent")
    if not user_agent:
        raise Failure(
            "Thiếu config ingest.user_agent trong config/app.yaml — không tự bịa User-Agent."
        )
    timeout = float(ingest_cfg.get("timeout_seconds", 30))
    per_page = int(ingest_cfg.get("github_per_source", 5))
    github_token = os.environ.get("GITHUB_TOKEN", "")

    queries = load_github_query_configs(only_enabled=True)
    if not queries:
        raise Failure("Không có truy vấn nào enabled trong config/github_sources.yaml.")

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = asyncio.run(
            run_github_ingest(
                connection,
                queries,
                user_agent=user_agent,
                github_token=github_token,
                ingest_date=ingest_date,
                per_page=per_page,
                timeout=timeout,
            )
        )
    duration_seconds = time.monotonic() - started_at

    if result.failed_sources:
        context.log.warning(
            f"Truy vấn lỗi khi ingest github {ingest_date}: {result.failed_sources}"
        )
    if result.rate_limited:
        context.log.warning(
            f"event=github_rate_limited ingest_date={ingest_date} — dừng sạch giữa chừng, "
            "xem README/PROGRESS.md task 1.2 để nâng hạn mức bằng GITHUB_TOKEN."
        )

    return MaterializeResult(
        metadata={
            "rows_inserted": MetadataValue.int(result.rows_inserted),
            "entries_fetched": MetadataValue.int(result.total_entries_fetched),
            "sources_ok": MetadataValue.int(result.sources_ok),
            "sources_failed": MetadataValue.int(len(result.failed_sources)),
            "rate_limited": MetadataValue.bool(result.rate_limited),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )
