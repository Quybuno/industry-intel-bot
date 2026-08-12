"""Lắp ráp toàn bộ đồ thị asset (task 0.12 mục 4, PRODUCTION_PLAN §7).

Chạy bằng: `uv run dagster dev -f dagster_project/definitions.py` (xem README).

Đọc biến môi trường TRỰC TIẾP bằng `os.environ.get(...)` (giống hệt `cli.py`) thay vì dùng
`dagster.EnvVar` — `EnvVar` trì hoãn resolve tới lúc resource thật sự chạy và sẽ raise nếu
biến hoàn toàn không tồn tại, kể cả với field có default; với `HEARTBEAT_URL`/
`DEEPSEEK_API_KEY` (hợp lệ khi rỗng — xem docstring từng resource) thì đọc trực tiếp cho ra
đúng hành vi "rỗng thì rơi vào nhánh lỗi rõ ràng của resource", không phải lỗi cấu hình khó
hiểu của Dagster.
"""

from __future__ import annotations

import os

from dagster import Definitions
from dotenv import load_dotenv

from dagster_project.assets.bronze import raw_github, raw_rss
from dagster_project.assets.dbt_assets import (
    daily_dbt_assets,
    dbt_resource,
    snapshot_dbt_assets,
)
from dagster_project.assets.serve import published_site
from dagster_project.assets.silver import (
    article_scores,
    article_summaries,
    articles_filtered,
    articles_normalized,
)
from dagster_project.checks import apply_freshness_policies
from dagster_project.resources.llm import LLMResource
from dagster_project.resources.notifier import NotifierResource
from dagster_project.resources.postgres import PostgresResource
from dagster_project.schedules import (
    daily_pipeline_schedule,
    evening_ingest_schedule,
    midday_ingest_schedule,
)
from dagster_project.sensors import (
    cost_sensor,
    freshness_sensor,
    quarantine_sensor,
    run_failure_alert_sensor,
)

# .env đọc TRƯỚC khi dựng resource — cùng quy ước cli.py (load_dotenv() đầu file).
load_dotenv()

defs = Definitions(
    assets=[
        raw_rss,
        raw_github,
        articles_normalized,
        articles_filtered,
        article_scores,
        article_summaries,
        daily_dbt_assets,
        snapshot_dbt_assets,
        published_site,
    ],
    resources={
        "postgres": PostgresResource(database_url=os.environ.get("DATABASE_URL", "")),
        "llm": LLMResource(
            provider_name=os.environ.get("LLM_PROVIDER", ""),
            deepseek_api_key=os.environ.get("DEEPSEEK_API_KEY", ""),
        ),
        "notifier": NotifierResource(
            heartbeat_url=os.environ.get("HEARTBEAT_URL", ""),
            telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
        ),
        "dbt": dbt_resource,
    },
    schedules=[
        daily_pipeline_schedule,
        midday_ingest_schedule,
        evening_ingest_schedule,
    ],
    sensors=[
        run_failure_alert_sensor,
        freshness_sensor,
        quarantine_sensor,
        cost_sensor,
    ],
)

# Freshness policy (task 1.5, §13.4) — gắn SAU khi Definitions gốc đã dựng xong, vì
# map_resolved_asset_specs() hoạt động trên Definitions đã hoàn chỉnh (trả về bản MỚI, không sửa tại
# chỗ). Xem dagster_project/checks.py cho toàn bộ lý do chọn API + giải thích lệch so với
# §13.4.
defs = apply_freshness_policies(defs)
