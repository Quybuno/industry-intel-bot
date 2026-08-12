"""Asset `published_site` (serve, không partition) — bọc lại `run_publish()` (task 0.11),
rồi ping heartbeat ra ngoài SAU KHI publish thành công (PRODUCTION_PLAN §7.5, task 0.13
mục 7). Đây là bước DUY NHẤT gọi `NotifierResource` ở Phase 0 — không có sensor nào khác
(rào chắn task 0.12: sensor để lại Phase 1).

**Không có `from __future__ import annotations`** — xem giải thích ở `assets/bronze.py`.
"""

import datetime as dt
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.resources.notifier import NotifierResource
from dagster_project.resources.postgres import PostgresResource
from src.intel_bot.config import load_config_dir
from src.intel_bot.publish.runner import run_publish

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


@asset(
    key="published_site",
    group_name="serve",
    deps=["mart_daily_digest"],
    description=(
        "Publish JSON + HTML tĩnh từ gold.mart_daily_digest (task 0.11), sau đó ping "
        "heartbeat (§7.5)."
    ),
)
def published_site(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    notifier: NotifierResource,
) -> MaterializeResult[Any]:
    """Không partition — khớp bảng asset gốc §7.2 (`published_site` | partition `—`):
    `mart_daily_digest` là cửa sổ 48h tự quyết bởi dbt, không có khái niệm "publish lại cho
    một ngày quá khứ cụ thể" (đã ghi rõ ở `run_publish()` — §12.2, task 0.11). Vì vậy
    `generated_for_date` dùng ngày hôm nay THẬT (`datetime.now`), không lấy từ partition
    key — asset này không có partition key để lấy."""
    now = dt.datetime.now(tz=VN_TZ)
    generated_for_date = now.date()

    publish_cfg = load_config_dir().get("app", {}).get("publish", {})
    repo_url = publish_cfg.get("repo_url")
    if not repo_url:
        raise Failure(
            "Thiếu config app.yaml: publish.repo_url — không tự bịa link repo."
        )
    docs_site_dir = Path(publish_cfg.get("docs_site_dir", "docs-site"))
    templates_dir = Path(publish_cfg.get("templates_dir", "templates"))

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = run_publish(
            connection,
            generated_for_date=generated_for_date,
            docs_site_dir=docs_site_dir,
            templates_dir=templates_dir,
            repo_url=repo_url,
            now=now,
        )
    duration_seconds = time.monotonic() - started_at

    # Heartbeat SAU KHI publish thành công — lỗi ping chỉ log warning, không fail asset
    # (rào chắn task 0.13 mục 7; xem docstring NotifierResource.ping_heartbeat).
    notifier.ping_heartbeat(logger=context.log)

    return MaterializeResult(
        metadata={
            "article_count": MetadataValue.int(result.article_count),
            "articles_marked_published": MetadataValue.int(
                result.articles_marked_published
            ),
            "index_html_path": MetadataValue.path(str(result.index_html_path)),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )
