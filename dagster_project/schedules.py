"""Lịch chạy 05:00 giờ Việt Nam cho toàn bộ đồ thị (task 0.12 mục 6, PRODUCTION_PLAN §7.3).

Chỉ MỘT lịch — 05:00 cho partition "hôm nay". Lịch bổ sung 12:00/18:00 (chỉ raw_*/
stg_articles, ingest thêm không tốn LLM) để lại Phase 1 đúng rào chắn task 0.12.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from dagster_project.partitions import daily_partitions

daily_pipeline_job = define_asset_job(
    name="daily_pipeline_job",
    description="Toàn bộ đồ thị: raw_rss -> ... -> published_site, cho một partition ngày.",
    selection=AssetSelection.all(),
    partitions_def=daily_partitions,
)

daily_pipeline_schedule = build_schedule_from_partitioned_job(
    daily_pipeline_job,
    hour_of_day=5,
    minute_of_hour=0,
    # KHÔNG truyền execution_timezone — hàm này không cho kết hợp cùng lúc với
    # hour_of_day/minute_of_hour (đã tự verify bằng CheckError, không phải đoán). Múi giờ
    # thật lấy từ `timezone="Asia/Ho_Chi_Minh"` đã khai trong `daily_partitions`
    # (partitions.py) — job dùng đúng partitions_def đó nên lịch tự thừa hưởng múi giờ.
)
