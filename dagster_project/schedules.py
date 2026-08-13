"""Lịch chạy giờ Việt Nam (task 0.12 mục 6 + task 1.6, PRODUCTION_PLAN §7.3).

- 05:00 — toàn bộ đồ thị cho partition "hôm nay" (job chính).
- 12:00, 18:00 — CHỈ `raw_rss`/`raw_github`/`articles_normalized`/`stg_articles`, ingest bổ
  sung KHÔNG tốn LLM (không chạm `articles_filtered`/`article_scores` trở đi). Để lại Phase 1
  ở task 0.12 (rào chắn lúc đó), làm ở task 1.6 (nhiệm vụ 6, đúng lịch đã hoãn).
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
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
    #
    # `default_status=RUNNING` (task 1.10) — trước đây STOPPED có chủ đích, đúng lời ghi ở
    # `midday_ingest_schedule` bên dưới: "chưa có Dagster daemon production thật". Giờ daemon
    # đã triển khai thật (docker-compose, xem docs/RUNBOOK.md) — một schedule STOPPED mặc
    # định sẽ không tự chạy sau khi daemon khởi động lại (DONE WHEN "reboot -> lịch 05:00
    # chạy không cần can thiệp" sẽ SAI nếu vẫn để STOPPED, người vận hành phải nhớ bật tay
    # qua UI mỗi lần daemon khởi động lại từ đầu — đúng thứ "cần can thiệp" mà DONE WHEN cấm).
    default_status=DefaultScheduleStatus.RUNNING,
)

#: §7.3: "Chỉ raw_* + stg_articles" — bổ sung `raw_github` (task 1.2, chưa tồn tại lúc §7.3
#: viết) vào nhóm "raw_*" cho nhất quán, KHÔNG có trong bảng gốc nhưng đúng tinh thần (mọi
#: asset bronze). `articles_normalized` (task 0.5/0.12, không có trong bảng gốc rút gọn của
#: plan — xem docstring `assets/silver.py`) BẮT BUỘC phải có trong selection: `stg_articles`
#: chỉ là VIEW đọc `silver.articles`, không có nó thì view luôn phản ánh dữ liệu CŨ, ingest
#: 12:00/18:00 sẽ vô nghĩa (bronze có bài mới nhưng silver/stg_articles thì không).
ingest_only_job = define_asset_job(
    name="ingest_only_job",
    description=(
        "Chỉ ingest + chuẩn hoá (raw_rss, raw_github, articles_normalized, stg_articles) — "
        "KHÔNG chạm articles_filtered/article_scores trở đi, không tốn LLM (§7.3 12:00/18:00)."
    ),
    selection=AssetSelection.keys(
        "raw_rss", "raw_github", "articles_normalized", "stg_articles"
    ),
    partitions_def=daily_partitions,
)

midday_ingest_schedule = build_schedule_from_partitioned_job(
    ingest_only_job,
    name="midday_ingest_schedule",
    hour_of_day=12,
    minute_of_hour=0,
    # `default_status=RUNNING` (task 1.10) — cùng lý do đã ghi ở `daily_pipeline_schedule`
    # phía trên: daemon production giờ đã có thật, giữ 3 lịch nhất quán (cả 3 cùng chạy mặc
    # định), không còn lý do để lịch này khác lịch 05:00.
    default_status=DefaultScheduleStatus.RUNNING,
)

evening_ingest_schedule = build_schedule_from_partitioned_job(
    ingest_only_job,
    name="evening_ingest_schedule",
    hour_of_day=18,
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.RUNNING,
)
