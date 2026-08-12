"""Asset `raw_rss` (bronze, daily) — bọc lại `run_rss_ingest()` đã có ở task 0.4
(PRODUCTION_PLAN §7.2, §8.1). KHÔNG viết lại logic fetch/parse/dedup cấp 0 — asset chỉ đọc
config, mở connection qua resource, gọi đúng hàm CLI `ingest` đã dùng.

`raw_github` để lại Phase 1 theo đúng rào chắn task 0.12 mục "raw_github để lại Phase 1".

**Không có `from __future__ import annotations`** (khác quy ước còn lại của repo) — dagster
kiểm tra kiểu tham số `context` bằng cách so khớp trực tiếp object class, không resolve
chuỗi forward-ref; bật future import làm annotation thành chuỗi và dagster báo lỗi sai
("context phải là AssetExecutionContext...") dù đã đúng kiểu. Đã verify bằng cách bật/tắt
dòng import này và chạy lại — không phải suy đoán.
"""

import asyncio
import datetime as dt
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
