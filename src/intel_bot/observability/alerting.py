"""Điều kiện cảnh báo kênh nội bộ (PRODUCTION_PLAN §18.3, task 1.6). Chỉ ĐỌC dữ liệu và trả
về danh sách điều kiện đã kích hoạt — KHÔNG tự gửi gì (gửi là việc của
`NotifierResource.send_alert()`, gọi từ `dagster_project/sensors.py`). Tách riêng khỏi
`dagster_project/` để test được bằng Postgres thật, không cần dựng sensor Dagster.

**Ngưỡng anomaly (quarantine/importance/tag/cost/ingest) đọc THẲNG từ
`dbt_project/dbt_project.yml` (`load_dbt_vars()`) — MỘT nguồn duy nhất, dùng chung với 6 dbt
singular test đã viết ở task 1.4 (rào chắn task 1.6 mục 3: "một ngưỡng, một chỗ", KHÔNG định
nghĩa lần thứ hai).** Logic SO SÁNH (baseline N-ngày, 3σ, v.v.) buộc phải lặp lại giữa SQL
(dbt test) và Python ở đây — không tránh được vì dbt không tự đẩy alert ra ngoài được; đã cân
nhắc đọc thẳng `AssetCheckExecutionRecord` nội bộ của Dagster (kết quả dbt test đã hiện thành
Asset Check) thay thế, nhưng đó là API lưu trữ nội bộ (`DagsterInstance._event_storage_impl`,
NamedTuple `LoadableBy`) không dành cho code ngoài dùng trực tiếp — đọc lại `mart_pipeline_health`
bằng SQL tường minh, cùng công thức, rủi ro thấp hơn.

`source_fail_rate_threshold` (>30% nguồn fail, §18.3) KHÔNG có trong `dbt_project.yml` (task
1.4 không tạo var này — §18.3 mới là nơi định nghĩa ngưỡng 30%, không phải §13.3) — đọc từ
`config/app.yaml` (`observability.source_fail_rate_threshold`, khoá MỚI của task 1.6).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import sqlalchemy as sa
import yaml


@dataclass(frozen=True)
class AlertCondition:
    """Một điều kiện §18.3 đã kích hoạt — đủ thông tin để dựng message gửi đi.

    `key` là khoá ổn định dùng cho chống-lặp (cursor sensor) — KHÔNG được đổi giữa các lần
    gọi cho cùng một loại điều kiện, kể cả khi số liệu trong `message` thay đổi.
    """

    key: str
    severity: str  # "critical" | "warning"
    message: str


def load_dbt_vars(path: str = "dbt_project/dbt_project.yml") -> dict[str, Any]:
    """Đọc `vars:` từ dbt_project.yml — nguồn ngưỡng anomaly DUY NHẤT (task 1.4). KHÔNG định
    nghĩa lại bất kỳ ngưỡng nào ở module này."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    result = raw.get("vars", {})
    if not isinstance(result, dict):
        return {}
    return result


def check_digest_empty(connection: sa.Connection) -> AlertCondition | None:
    """§18.3: `mart_daily_digest` rỗng → Critical."""
    count = connection.execute(
        sa.text("SELECT COUNT(*) FROM gold.mart_daily_digest")
    ).scalar_one()
    if count == 0:
        return AlertCondition(
            key="mart_daily_digest_empty",
            severity="critical",
            message=(
                "\U0001f534 CRITICAL: gold.mart_daily_digest ĐANG RỖNG (0 bài) — trang "
                "publish sẽ không có nội dung."
            ),
        )
    return None


def check_pipeline_health_anomalies(
    connection: sa.Connection, *, dbt_vars: dict[str, Any]
) -> list[AlertCondition]:
    """§13.3 (6 kiểm tra dbt, task 1.4) — bao gồm "quarantine rate > 10%" (§18.3 liệt kê
    riêng nhưng CHÍNH LÀ một trong 6 kiểm tra này, không phải điều kiện thứ 7) → Warning.
    Đọc `gold.mart_pipeline_health`, áp lại đúng công thức 6 dbt singular test
    (`dbt_project/tests/assert_*.sql`) bằng số ngưỡng từ `dbt_vars`."""
    conditions: list[AlertCondition] = []

    window_needed = (
        max(
            int(dbt_vars.get("anomaly_ingest_window_days", 14)),
            int(dbt_vars.get("anomaly_importance_window_days", 7)),
            int(dbt_vars.get("anomaly_cost_window_days", 7)),
        )
        + 1
    )
    rows = connection.execute(
        sa.text(
            """
            SELECT pipeline_date, ingest_count, quarantine_rate, mean_importance,
                   stddev_importance, empty_tag_rate, cost_per_article
            FROM gold.mart_pipeline_health
            ORDER BY pipeline_date DESC
            LIMIT :limit
            """
        ),
        {"limit": window_needed},
    ).all()
    if not rows:
        return conditions
    latest = rows[0]
    # rows[1:] = các ngày ĐỨNG TRƯỚC latest, mới nhất trước — cùng tập hợp "N preceding" mà
    # window function trong dbt test dùng (thứ tự không ảnh hưởng tới mean/stddev).
    history = rows[1:]

    quarantine_threshold = float(
        dbt_vars.get("anomaly_quarantine_rate_threshold", 0.10)
    )
    if (
        latest.quarantine_rate is not None
        and float(latest.quarantine_rate) > quarantine_threshold
    ):
        conditions.append(
            AlertCondition(
                key="quarantine_rate_high",
                severity="warning",
                message=(
                    f"\U0001f7e1 WARNING: quarantine_rate ngày {latest.pipeline_date} = "
                    f"{float(latest.quarantine_rate):.1%} > ngưỡng {quarantine_threshold:.0%}."
                ),
            )
        )

    stddev_min = float(dbt_vars.get("anomaly_importance_stddev_min", 0.8))
    if (
        latest.stddev_importance is not None
        and float(latest.stddev_importance) < stddev_min
    ):
        conditions.append(
            AlertCondition(
                key="importance_stddev_low",
                severity="warning",
                message=(
                    f"\U0001f7e1 WARNING: stddev_importance ngày {latest.pipeline_date} = "
                    f"{float(latest.stddev_importance):.2f} < ngưỡng {stddev_min} — model có "
                    "thể đang dồn điểm, mất khả năng phân biệt."
                ),
            )
        )

    tag_threshold = float(dbt_vars.get("anomaly_empty_tag_rate_threshold", 0.05))
    if (
        latest.empty_tag_rate is not None
        and float(latest.empty_tag_rate) > tag_threshold
    ):
        conditions.append(
            AlertCondition(
                key="empty_tag_rate_high",
                severity="warning",
                message=(
                    f"\U0001f7e1 WARNING: empty_tag_rate ngày {latest.pipeline_date} = "
                    f"{float(latest.empty_tag_rate):.1%} > ngưỡng {tag_threshold:.0%}."
                ),
            )
        )

    importance_window = int(dbt_vars.get("anomaly_importance_window_days", 7))
    importance_drift_threshold = float(
        dbt_vars.get("anomaly_importance_drift_threshold", 1.0)
    )
    if len(history) >= importance_window and latest.mean_importance is not None:
        baseline_rows = [
            r for r in history[:importance_window] if r.mean_importance is not None
        ]
        if baseline_rows:
            baseline_mean = sum(float(r.mean_importance) for r in baseline_rows) / len(
                baseline_rows
            )
            drift = abs(float(latest.mean_importance) - baseline_mean)
            if drift > importance_drift_threshold:
                conditions.append(
                    AlertCondition(
                        key="importance_mean_drift",
                        severity="warning",
                        message=(
                            f"\U0001f7e1 WARNING: mean_importance ngày {latest.pipeline_date} "
                            f"= {float(latest.mean_importance):.2f}, lệch {drift:.2f} so với "
                            f"trung bình {importance_window} ngày trước ({baseline_mean:.2f}) "
                            "— nghi model drift."
                        ),
                    )
                )

    ingest_window = int(dbt_vars.get("anomaly_ingest_window_days", 14))
    ingest_sigma = float(dbt_vars.get("anomaly_ingest_stddev_threshold", 3.0))
    if len(history) >= ingest_window:
        baseline = [float(r.ingest_count) for r in history[:ingest_window]]
        baseline_mean = sum(baseline) / len(baseline)
        variance = (
            sum((x - baseline_mean) ** 2 for x in baseline) / (len(baseline) - 1)
            if len(baseline) > 1
            else 0.0
        )
        baseline_stddev = variance**0.5
        # KHÔNG guard `baseline_stddev > 0` — khớp đúng dbt_project/tests/
        # assert_ingest_count_no_anomaly.sql (task 1.4): baseline rock-solid (stddev=0) mà có
        # deviation thật vẫn là tín hiệu đáng báo, không loại trừ (xem docstring test đó).
        if (
            abs(float(latest.ingest_count) - baseline_mean)
            > ingest_sigma * baseline_stddev
        ):
            conditions.append(
                AlertCondition(
                    key="ingest_count_anomaly",
                    severity="warning",
                    message=(
                        f"\U0001f7e1 WARNING: ingest_count ngày {latest.pipeline_date} = "
                        f"{latest.ingest_count}, lệch > {ingest_sigma}σ so với trung bình "
                        f"{ingest_window} ngày trước ({baseline_mean:.1f} ± "
                        f"{baseline_stddev:.1f})."
                    ),
                )
            )

    cost_window = int(dbt_vars.get("anomaly_cost_window_days", 7))
    cost_drift_pct = float(dbt_vars.get("anomaly_cost_drift_pct", 0.50))
    if len(history) >= cost_window and latest.cost_per_article is not None:
        baseline_cost_rows = [
            r for r in history[:cost_window] if r.cost_per_article is not None
        ]
        if baseline_cost_rows:
            baseline_mean = sum(
                float(r.cost_per_article) for r in baseline_cost_rows
            ) / len(baseline_cost_rows)
            if baseline_mean > 0:
                drift_pct = (
                    abs(float(latest.cost_per_article) - baseline_mean) / baseline_mean
                )
                if drift_pct > cost_drift_pct:
                    conditions.append(
                        AlertCondition(
                            key="cost_per_article_drift",
                            severity="warning",
                            message=(
                                "\U0001f7e1 WARNING: cost_per_article ngày "
                                f"{latest.pipeline_date} = ${float(latest.cost_per_article):.6f}"
                                f", lệch {drift_pct:.0%} so với trung bình {cost_window} ngày "
                                f"trước (${baseline_mean:.6f})."
                            ),
                        )
                    )

    return conditions


def check_source_fail_rate(
    connection: sa.Connection, *, total_enabled_sources: int, threshold: float
) -> AlertCondition | None:
    """§18.3: > `threshold` (30%) nguồn fail trong ngày gần nhất → Warning. `total_enabled_sources`
    đếm từ config (RSS + github), KHÔNG có sẵn dạng cột trong `mart_pipeline_health`."""
    if total_enabled_sources <= 0:
        return None
    row = connection.execute(
        sa.text(
            "SELECT pipeline_date, source_fail_count FROM gold.mart_pipeline_health "
            "ORDER BY pipeline_date DESC LIMIT 1"
        )
    ).first()
    if row is None:
        return None
    ratio = row.source_fail_count / total_enabled_sources
    if ratio > threshold:
        return AlertCondition(
            key="source_fail_rate_high",
            severity="warning",
            message=(
                f"\U0001f7e1 WARNING: {row.source_fail_count}/{total_enabled_sources} nguồn "
                f"lỗi ngày {row.pipeline_date} ({ratio:.0%}) > ngưỡng {threshold:.0%}."
            ),
        )
    return None


def check_monthly_cost_over_budget(
    connection: sa.Connection, *, month_start: dt.date, monthly_budget_usd: Decimal
) -> AlertCondition | None:
    """§18.3: chi phí LLM tích luỹ tháng > 80% ngân sách → Warning. `month_start` nhận qua
    tham số (không gọi `datetime.now()`/`CURRENT_DATE` trong SQL — cùng nguyên tắc AGENTS.md
    mục 3 áp cho toàn dự án, không riêng dbt test)."""
    if monthly_budget_usd <= 0:
        return None
    total = connection.execute(
        sa.text(
            "SELECT COALESCE(SUM(total_cost_usd), 0) FROM gold.mart_pipeline_health "
            "WHERE pipeline_date >= :month_start"
        ),
        {"month_start": month_start},
    ).scalar_one()
    total_cost = Decimal(str(total))
    ratio = total_cost / monthly_budget_usd
    if ratio > Decimal("0.80"):
        return AlertCondition(
            key="monthly_cost_over_budget",
            severity="warning",
            message=(
                f"\U0001f7e1 WARNING: chi phí LLM tháng này = ${total_cost:.4f}, đã dùng "
                f"{ratio:.0%} ngân sách tháng (${monthly_budget_usd}) — §18.3 ngưỡng 80%."
            ),
        )
    return None
