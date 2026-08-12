"""4 sensor cho kênh alert nội bộ (task 1.6, PRODUCTION_PLAN §7.4, §18.3).

**API đã verify thật trên `dagster==1.13.17` trước khi viết (không đoán, rào chắn nhiệm vụ
1):** `@run_failure_sensor` (decorator built-in — đúng khớp "Bất kỳ run fail", tự Dagster
đảm bảo MỖI lần fail chỉ gọi hàm ĐÚNG MỘT LẦN qua cursor nội bộ riêng của run-status sensor,
không cần tự chống lặp thêm cho sensor này — 3 sensor còn lại POLL trạng thái lặp lại mỗi
tick nên MỚI cần cơ chế chống lặp riêng, xem bên dưới) + `@sensor` (generic, tự viết logic
polling) cho 3 sensor còn lại. Resource lấy qua `context.resources.<key>` (yêu cầu khai
`required_resource_keys`) — mẫu chuẩn của Dagster, không phải API preview như freshness
(task 1.5).

**Đối chiếu §7.4 (tên 4 sensor) với §18.3 (đúng 6 điều kiện + mức, nhiệm vụ 3 yêu cầu bám
SÁT bảng này) — 2 bảng KHÔNG khớp 1-1, đã giải quyết như sau, ghi rõ vì đề bài yêu cầu báo
cáo mâu thuẫn thay vì tự chọn ngầm:**
- `run_failure_sensor` → "Dagster run failed" (Critical).
- `freshness_sensor` → "mart_daily_digest rỗng" (Critical) — ĐÚNG CHỮ của §18.3, KHÔNG phải
  ">26h" của §7.4. SLA ">26h" đã có `FreshnessPolicy` từ task 1.5 (hiện trong UI, chưa gắn
  sensor gửi alert theo yêu cầu — nhiệm vụ 1.5 chỉ "định nghĩa policy", KHÔNG gửi alert). Tên
  "freshness_sensor" giữ theo §7.4 nhưng điều kiện lấy theo §18.3 vì nhiệm vụ 3 nói rõ ưu
  tiên bảng đó.
- `quarantine_sensor` → "Quarantine rate > 10%" + "Anomaly bất kỳ ở §13.3" (quarantine rate
  CHÍNH LÀ một trong 6 kiểm tra §13.3, không phải điều kiện thứ 7 — xem
  `src/intel_bot/observability/alerting.py::check_pipeline_health_anomalies`) + "> 30% nguồn
  fail" (không sensor nào trong §7.4 được đặt tên cho điều kiện này — gộp vào đây vì cùng
  nguồn dữ liệu `gold.mart_pipeline_health`, tránh phá vỡ đúng "4 sensor" của nhiệm vụ 1).
- `cost_sensor` → "Cost tháng > 80% ngân sách" (Warning) — CHỈ cảnh báo, KHÔNG "tự chuyển
  sang model rẻ hơn" (hành động tự động §7.4 mô tả cho sensor này) — nhiệm vụ 3 chỉ giao
  "Điều kiện và mức", không giao auto-remediation; đây là tính năng khác, lớn hơn, ngoài
  phạm vi task 1.6.

**Chống spam (nhiệm vụ 4) — CHỈ áp cho 3 sensor polling (freshness/quarantine/cost):**
`context.cursor` — chuỗi JSON `{condition_key: iso_timestamp_gửi_gần_nhất}` — Dagster tự lưu
cursor này vào RUN STORAGE (Postgres/SQLite instance storage CỦA CHÍNH DAGSTER, KHÔNG phải
biến Python trong bộ nhớ tiến trình) giữa các tick, và giữ nguyên qua daemon restart (đọc lại
đúng cursor cũ từ storage khi daemon khởi động lại — không mất trạng thái chống-lặp, không
gửi thừa ngay sau restart). Trước khi gửi mỗi điều kiện, so `now - last_sent_at` với
`alert_dedup_window_hours` (config/app.yaml) — chưa đủ N giờ thì bỏ qua.

**Không có `from __future__ import annotations`** — sensor nhận `context` với kiểu cụ thể
(`SensorEvaluationContext`/`RunFailureSensorContext`), cùng lý do các file asset ở
`dagster_project/assets/` (xem `assets/bronze.py`).
"""

import datetime as dt
import json
import os
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    SensorEvaluationContext,
    SensorResult,
    run_failure_sensor,
    sensor,
)

from dagster_project.resources.notifier import NotifierResource
from dagster_project.resources.postgres import PostgresResource
from src.intel_bot.config import load_config_dir
from src.intel_bot.ingest.github_fetcher import load_github_query_configs
from src.intel_bot.ingest.rss_fetcher import load_source_configs
from src.intel_bot.observability.alerting import (
    AlertCondition,
    check_digest_empty,
    check_monthly_cost_over_budget,
    check_pipeline_health_anomalies,
    check_source_fail_rate,
    load_dbt_vars,
)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

#: Đủ nhanh để verify thật trong cửa sổ 5 phút của DONE WHEN, không dồn dập quá mức cho một
#: pipeline chạy vài lần/ngày (§7.3: 05:00/12:00/18:00).
_POLL_INTERVAL_SECONDS = 60


def _dedup_window_hours() -> int:
    return int(
        load_config_dir()
        .get("app", {})
        .get("observability", {})
        .get("alert_dedup_window_hours", 6)
    )


def _should_alert(
    state: dict[str, str], key: str, *, now: dt.datetime, window_hours: int
) -> bool:
    """True nếu điều kiện `key` CHƯA từng gửi trong cửa sổ chống lặp — xem docstring module."""
    last_sent = state.get(key)
    if last_sent is None:
        return True
    try:
        last_sent_dt = dt.datetime.fromisoformat(last_sent)
    except ValueError:
        return True
    return (now - last_sent_dt) > dt.timedelta(hours=window_hours)


def _process_conditions(
    conditions: list[AlertCondition],
    *,
    cursor: str | None,
    notifier: NotifierResource,
    logger: Any,
    now: dt.datetime,
) -> str:
    """Gửi (qua notifier) đúng những điều kiện chưa bị chống-lặp, trả về cursor JSON mới để
    sensor gán vào `SensorResult(cursor=...)`."""
    state: dict[str, str] = json.loads(cursor) if cursor else {}
    window_hours = _dedup_window_hours()
    for condition in conditions:
        if _should_alert(state, condition.key, now=now, window_hours=window_hours):
            sent = notifier.send_alert(condition.message, logger=logger)
            if sent:
                state[condition.key] = now.isoformat()
        else:
            logger.info(
                f"Bỏ qua alert '{condition.key}' — đã gửi trong {window_hours}h qua "
                "(chống lặp, §18.3)."
            )
    return json.dumps(state)


@run_failure_sensor(
    name="run_failure_sensor",
    monitor_all_code_locations=True,
    minimum_interval_seconds=_POLL_INTERVAL_SECONDS,
    default_status=DefaultSensorStatus.RUNNING,
)
def run_failure_alert_sensor(context: RunFailureSensorContext) -> None:
    """§18.3: "Dagster run failed" → Critical. Dagster tự đảm bảo mỗi lần run fail chỉ gọi
    hàm này ĐÚNG MỘT LẦN (cursor nội bộ của run-status sensor) — không cần chống lặp thêm.

    **Lỗi thật phát hiện khi verify:** `@run_failure_sensor` (khác `@sensor`) KHÔNG có tham
    số `required_resource_keys` (đã verify bằng `inspect.getsource` — không suy đoán) — dùng
    `context.resources.notifier` như 3 sensor kia làm sensor CRASH thật
    (`DagsterUnknownResourceError: Unknown resource 'notifier'`, bắt được qua tick history
    GraphQL lúc test run-failure thật). Sửa bằng cách tự dựng `NotifierResource` thẳng từ
    biến môi trường (giống hệt cách `definitions.py` dựng nó cho `resources={...}`), không
    phụ thuộc cơ chế inject resource của decorator này."""
    notifier = NotifierResource(
        heartbeat_url=os.environ.get("HEARTBEAT_URL", ""),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
    )
    step_failures = context.get_step_failure_events()
    step_keys = sorted({event.step_key for event in step_failures if event.step_key})
    asset_info = (
        ", ".join(step_keys) if step_keys else "(không xác định được asset/step)"
    )
    partition = context.partition_key or "(không có partition)"
    base_url = os.environ.get("DAGSTER_WEBSERVER_URL", "")
    run_link = (
        f"{base_url}/runs/{context.dagster_run.run_id}"
        if base_url
        else "(DAGSTER_WEBSERVER_URL chưa cấu hình, xem .env.example)"
    )
    message = (
        "\U0001f534 CRITICAL: Dagster run THẤT BẠI\n"
        f"Job: {context.dagster_run.job_name}\n"
        f"Asset/step: {asset_info}\n"
        f"Partition: {partition}\n"
        f"Run: {run_link}"
    )
    notifier.send_alert(message, logger=context.log)


@sensor(
    name="freshness_sensor",
    minimum_interval_seconds=_POLL_INTERVAL_SECONDS,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"postgres", "notifier"},
)
def freshness_sensor(context: SensorEvaluationContext) -> SensorResult:
    """§18.3: `mart_daily_digest` rỗng → Critical (xem docstring module về lệch tên/điều
    kiện so với §7.4)."""
    postgres: PostgresResource = context.resources.postgres
    notifier: NotifierResource = context.resources.notifier
    now = dt.datetime.now(tz=VN_TZ)

    with postgres.get_connection() as connection:
        condition = check_digest_empty(connection)

    conditions = [condition] if condition else []
    new_cursor = _process_conditions(
        conditions,
        cursor=context.cursor,
        notifier=notifier,
        logger=context.log,
        now=now,
    )
    skip_reason = (
        "mart_daily_digest có dữ liệu, không có gì bất thường."
        if not conditions
        else None
    )
    return SensorResult(skip_reason=skip_reason, cursor=new_cursor)


@sensor(
    name="quarantine_sensor",
    minimum_interval_seconds=_POLL_INTERVAL_SECONDS,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"postgres", "notifier"},
)
def quarantine_sensor(context: SensorEvaluationContext) -> SensorResult:
    """§18.3: "Quarantine rate > 10%" + "Anomaly bất kỳ ở §13.3" + "> 30% nguồn fail" →
    Warning (xem docstring module về việc gộp 3 điều kiện vào 1 sensor)."""
    postgres: PostgresResource = context.resources.postgres
    notifier: NotifierResource = context.resources.notifier
    now = dt.datetime.now(tz=VN_TZ)

    dbt_vars = load_dbt_vars()
    fail_threshold = float(
        load_config_dir()
        .get("app", {})
        .get("observability", {})
        .get("source_fail_rate_threshold", 0.30)
    )
    total_enabled_sources = len(load_source_configs(only_enabled=True)) + len(
        load_github_query_configs(only_enabled=True)
    )

    with postgres.get_connection() as connection:
        conditions = check_pipeline_health_anomalies(connection, dbt_vars=dbt_vars)
        source_fail_condition = check_source_fail_rate(
            connection,
            total_enabled_sources=total_enabled_sources,
            threshold=fail_threshold,
        )

    if source_fail_condition:
        conditions.append(source_fail_condition)

    new_cursor = _process_conditions(
        conditions,
        cursor=context.cursor,
        notifier=notifier,
        logger=context.log,
        now=now,
    )
    skip_reason = (
        "Không có anomaly/quarantine/source-fail nào vượt ngưỡng."
        if not conditions
        else None
    )
    return SensorResult(skip_reason=skip_reason, cursor=new_cursor)


@sensor(
    name="cost_sensor",
    minimum_interval_seconds=_POLL_INTERVAL_SECONDS,
    default_status=DefaultSensorStatus.RUNNING,
    required_resource_keys={"postgres", "notifier"},
)
def cost_sensor(context: SensorEvaluationContext) -> SensorResult:
    """§18.3: "Cost tháng > 80% ngân sách" → Warning. CHỈ cảnh báo (xem docstring module —
    KHÔNG tự chuyển model rẻ hơn, đó là tính năng khác)."""
    postgres: PostgresResource = context.resources.postgres
    notifier: NotifierResource = context.resources.notifier
    now = dt.datetime.now(tz=VN_TZ)

    monthly_budget_usd = Decimal(
        str(
            load_config_dir()
            .get("app", {})
            .get("observability", {})
            .get("monthly_budget_usd", "0")
        )
    )
    month_start = now.date().replace(day=1)

    with postgres.get_connection() as connection:
        condition = check_monthly_cost_over_budget(
            connection, month_start=month_start, monthly_budget_usd=monthly_budget_usd
        )

    conditions = [condition] if condition else []
    new_cursor = _process_conditions(
        conditions,
        cursor=context.cursor,
        notifier=notifier,
        logger=context.log,
        now=now,
    )
    skip_reason = "Chi phí tháng chưa vượt 80% ngân sách." if not conditions else None
    return SensorResult(skip_reason=skip_reason, cursor=new_cursor)
