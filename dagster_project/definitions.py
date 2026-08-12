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

from dagster_project.assets.bronze import raw_rss
from dagster_project.assets.dbt_assets import (
    daily_dbt_assets,
    dbt_resource,
    snapshot_dbt_assets,
)
from dagster_project.assets.serve import published_site
from dagster_project.assets.silver import (
    article_scores_and_summaries,
    articles_filtered,
    articles_normalized,
)
from dagster_project.resources.llm import LLMResource
from dagster_project.resources.notifier import NotifierResource
from dagster_project.resources.postgres import PostgresResource
from dagster_project.schedules import daily_pipeline_schedule

# .env đọc TRƯỚC khi dựng resource — cùng quy ước cli.py (load_dotenv() đầu file).
load_dotenv()

defs = Definitions(
    assets=[
        raw_rss,
        articles_normalized,
        articles_filtered,
        article_scores_and_summaries,
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
        "notifier": NotifierResource(heartbeat_url=os.environ.get("HEARTBEAT_URL", "")),
        "dbt": dbt_resource,
    },
    schedules=[daily_pipeline_schedule],
)
