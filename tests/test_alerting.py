"""Test cho `src/intel_bot/observability/alerting.py` (task 1.6) — Postgres THẬT
(PRODUCTION_PLAN §20.2), chèn thẳng dòng test vào `gold.mart_pipeline_health`/
`gold.mart_daily_digest`, dọn dẹp ở cuối, không đụng dữ liệu thật (theo đúng khuôn
`tests/test_publish_runner.py`).

Ngày test dùng dải 2025-01-xx — tách biệt hoàn toàn khỏi dữ liệu thật (2026-08-xx) và khỏi
dải ngày `tests/test_rss_ingest.py`/`tests/test_github_ingest.py` (2000-01-xx).
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from decimal import Decimal

import pytest
import sqlalchemy as sa

from src.intel_bot.observability.alerting import (
    check_digest_empty,
    check_monthly_cost_over_budget,
    check_pipeline_health_anomalies,
    check_source_fail_rate,
    load_dbt_vars,
)

TEST_MONTH_START = dt.date(2025, 1, 1)


def _cleanup(connection: sa.Connection) -> None:
    connection.execute(
        sa.text("DELETE FROM gold.mart_pipeline_health WHERE pipeline_date >= :d"),
        {"d": TEST_MONTH_START},
    )
    connection.commit()


@pytest.fixture()
def clean_health_table(db_connection: sa.Connection) -> Iterator[sa.Connection]:
    _cleanup(db_connection)
    yield db_connection
    _cleanup(db_connection)


def _insert_health_row(
    connection: sa.Connection,
    *,
    pipeline_date: dt.date,
    ingest_count: int = 10,
    quarantine_rate: float | None = 0.0,
    mean_importance: float | None = 5.0,
    stddev_importance: float | None = 2.0,
    empty_tag_rate: float | None = 0.0,
    cost_per_article: float | None = 0.001,
    total_cost_usd: float = 0.01,
    source_fail_count: int = 0,
    scored_count: int = 10,
) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO gold.mart_pipeline_health
                (pipeline_date, ingest_count, eligible_count, excluded_count, excluded_ratio,
                 quarantine_count, total_cost_usd, source_fail_count, scored_count,
                 mean_importance, stddev_importance, empty_tag_count, empty_tag_rate,
                 cost_per_article, quarantine_rate, computed_at)
            VALUES
                (:pipeline_date, :ingest_count, :ingest_count, 0, 0,
                 0, :total_cost_usd, :source_fail_count, :scored_count,
                 :mean_importance, :stddev_importance, 0, :empty_tag_rate,
                 :cost_per_article, :quarantine_rate, now())
            """
        ),
        {
            "pipeline_date": pipeline_date,
            "ingest_count": ingest_count,
            "total_cost_usd": total_cost_usd,
            "source_fail_count": source_fail_count,
            "scored_count": scored_count,
            "mean_importance": mean_importance,
            "stddev_importance": stddev_importance,
            "empty_tag_rate": empty_tag_rate,
            "cost_per_article": cost_per_article,
            "quarantine_rate": quarantine_rate,
        },
    )
    connection.commit()


# ---------------------------------------------------------------------------
# load_dbt_vars — không cần DB
# ---------------------------------------------------------------------------


def test_load_dbt_vars_has_all_anomaly_thresholds_task_1_4() -> None:
    """Đọc đúng file dbt_project.yml thật của repo — vars phải khớp đúng 9 khoá task 1.4 đã
    tạo (không định nghĩa lại — nếu ai đổi tên var mà quên sửa alerting.py, test này rớt)."""
    dbt_vars = load_dbt_vars()
    expected_keys = {
        "anomaly_ingest_window_days",
        "anomaly_ingest_stddev_threshold",
        "anomaly_quarantine_rate_threshold",
        "anomaly_importance_window_days",
        "anomaly_importance_drift_threshold",
        "anomaly_importance_stddev_min",
        "anomaly_empty_tag_rate_threshold",
        "anomaly_cost_window_days",
        "anomaly_cost_drift_pct",
    }
    assert expected_keys.issubset(dbt_vars.keys())


# ---------------------------------------------------------------------------
# check_digest_empty
# ---------------------------------------------------------------------------


def test_check_digest_empty_returns_none_when_digest_has_rows(
    db_connection: sa.Connection,
) -> None:
    """digest thật của repo (dữ liệu 2026-08 đã có từ các task trước) không rỗng."""
    condition = check_digest_empty(db_connection)
    assert condition is None


# ---------------------------------------------------------------------------
# check_pipeline_health_anomalies — ngưỡng tuyệt đối (không cần baseline nhiều ngày)
# ---------------------------------------------------------------------------


def test_quarantine_rate_high_detected(clean_health_table: sa.Connection) -> None:
    dbt_vars = load_dbt_vars()
    threshold = float(dbt_vars["anomaly_quarantine_rate_threshold"])
    _insert_health_row(
        clean_health_table,
        pipeline_date=TEST_MONTH_START,
        quarantine_rate=threshold + 0.05,
    )
    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    keys = {c.key for c in conditions}
    assert "quarantine_rate_high" in keys


def test_importance_stddev_low_detected(clean_health_table: sa.Connection) -> None:
    dbt_vars = load_dbt_vars()
    stddev_min = float(dbt_vars["anomaly_importance_stddev_min"])
    _insert_health_row(
        clean_health_table,
        pipeline_date=TEST_MONTH_START,
        stddev_importance=stddev_min - 0.1,
    )
    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    keys = {c.key for c in conditions}
    assert "importance_stddev_low" in keys


def test_empty_tag_rate_high_detected(clean_health_table: sa.Connection) -> None:
    dbt_vars = load_dbt_vars()
    tag_threshold = float(dbt_vars["anomaly_empty_tag_rate_threshold"])
    _insert_health_row(
        clean_health_table,
        pipeline_date=TEST_MONTH_START,
        empty_tag_rate=tag_threshold + 0.10,
    )
    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    keys = {c.key for c in conditions}
    assert "empty_tag_rate_high" in keys


def test_no_anomaly_on_healthy_row(clean_health_table: sa.Connection) -> None:
    """Dòng khoẻ mạnh (giá trị mặc định của _insert_health_row) -> KHÔNG kích hoạt điều kiện
    tuyệt đối nào (3 kiểm tra baseline không đủ lịch sử -> tự bỏ qua, không phải bug)."""
    dbt_vars = load_dbt_vars()
    _insert_health_row(clean_health_table, pipeline_date=TEST_MONTH_START)
    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    assert conditions == []


def test_empty_mart_pipeline_health_does_not_crash(
    clean_health_table: sa.Connection,
) -> None:
    """Bảng test hoàn toàn rỗng (đã cleanup, chưa insert gì) -> trả về [], không lỗi (nhiệm vụ
    4: thiếu dữ liệu không phải bất thường)."""
    dbt_vars = load_dbt_vars()
    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    assert conditions == []


# ---------------------------------------------------------------------------
# check_pipeline_health_anomalies — baseline N-ngày (windowed)
# ---------------------------------------------------------------------------


def test_importance_mean_drift_not_detected_with_insufficient_history(
    clean_health_table: sa.Connection,
) -> None:
    """`window - 1` dòng baseline + 1 dòng "latest" lệch mạnh — chỉ có `window - 1` dòng ĐỨNG
    TRƯỚC latest (đúng NGAY DƯỚI ngưỡng "nhiều hơn window" — biên đã verify: `window` dòng
    đứng trước ĐÃ ĐỦ để đánh giá, khớp `day_number > window_days` của dbt test), nên PASS dù
    drift rất lớn (nhiệm vụ 4: thiếu dữ liệu không phải bất thường)."""
    dbt_vars = load_dbt_vars()
    window = int(dbt_vars["anomaly_importance_window_days"])
    for i in range(window - 1):
        _insert_health_row(
            clean_health_table,
            pipeline_date=TEST_MONTH_START + dt.timedelta(days=i),
            mean_importance=5.0,
        )
    latest_date = TEST_MONTH_START + dt.timedelta(days=window - 1)
    _insert_health_row(
        clean_health_table, pipeline_date=latest_date, mean_importance=9.9
    )

    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    assert "importance_mean_drift" not in {c.key for c in conditions}


def test_importance_mean_drift_detected_with_enough_history(
    clean_health_table: sa.Connection,
) -> None:
    """`window + 1` dòng baseline ổn định (mean=5.0) + 1 dòng "latest" lệch mạnh — đủ
    `window` dòng ĐỨNG TRƯỚC latest -> phải kích hoạt."""
    dbt_vars = load_dbt_vars()
    window = int(dbt_vars["anomaly_importance_window_days"])
    for i in range(window + 1):
        _insert_health_row(
            clean_health_table,
            pipeline_date=TEST_MONTH_START + dt.timedelta(days=i),
            mean_importance=5.0,
        )
    latest_date = TEST_MONTH_START + dt.timedelta(days=window + 1)
    _insert_health_row(
        clean_health_table, pipeline_date=latest_date, mean_importance=9.9
    )

    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    assert "importance_mean_drift" in {c.key for c in conditions}


def test_ingest_count_anomaly_needs_baseline_stddev_nonzero(
    clean_health_table: sa.Connection,
) -> None:
    """stddev baseline = 0 (mọi ngày y hệt nhau) + hôm nay lệch hẳn -> VẪN kích hoạt (đã tự
    quyết định trong code: KHÔNG loại trường hợp stddev=0 vì baseline rock-solid mà đổi vẫn là
    tín hiệu thật, xem dagster_project/checks... không, xem alerting.py comment)."""
    dbt_vars = load_dbt_vars()
    window = int(dbt_vars["anomaly_ingest_window_days"])
    for i in range(window):
        _insert_health_row(
            clean_health_table,
            pipeline_date=TEST_MONTH_START + dt.timedelta(days=i),
            ingest_count=10,
        )
    latest_date = TEST_MONTH_START + dt.timedelta(days=window)
    _insert_health_row(clean_health_table, pipeline_date=latest_date, ingest_count=1000)
    conditions = check_pipeline_health_anomalies(clean_health_table, dbt_vars=dbt_vars)
    assert "ingest_count_anomaly" in {c.key for c in conditions}


# ---------------------------------------------------------------------------
# check_source_fail_rate
# ---------------------------------------------------------------------------


def test_source_fail_rate_high_detected(clean_health_table: sa.Connection) -> None:
    _insert_health_row(
        clean_health_table, pipeline_date=TEST_MONTH_START, source_fail_count=8
    )
    condition = check_source_fail_rate(
        clean_health_table, total_enabled_sources=10, threshold=0.30
    )
    assert condition is not None
    assert condition.key == "source_fail_rate_high"


def test_source_fail_rate_ok_returns_none(clean_health_table: sa.Connection) -> None:
    _insert_health_row(
        clean_health_table, pipeline_date=TEST_MONTH_START, source_fail_count=1
    )
    condition = check_source_fail_rate(
        clean_health_table, total_enabled_sources=10, threshold=0.30
    )
    assert condition is None


def test_source_fail_rate_zero_total_sources_returns_none(
    clean_health_table: sa.Connection,
) -> None:
    """total_enabled_sources=0 (config rỗng/lỗi đọc) -> None, KHÔNG chia cho 0."""
    condition = check_source_fail_rate(
        clean_health_table, total_enabled_sources=0, threshold=0.30
    )
    assert condition is None


# ---------------------------------------------------------------------------
# check_monthly_cost_over_budget
# ---------------------------------------------------------------------------


def test_monthly_cost_over_budget_detected(clean_health_table: sa.Connection) -> None:
    for i in range(5):
        _insert_health_row(
            clean_health_table,
            pipeline_date=TEST_MONTH_START + dt.timedelta(days=i),
            total_cost_usd=5.0,
        )
    condition = check_monthly_cost_over_budget(
        clean_health_table,
        month_start=TEST_MONTH_START,
        monthly_budget_usd=Decimal("20.00"),
    )
    # tổng = 25.0, ngân sách 20.0 -> 125% > 80% -> kích hoạt
    assert condition is not None
    assert condition.key == "monthly_cost_over_budget"


def test_monthly_cost_under_budget_returns_none(
    clean_health_table: sa.Connection,
) -> None:
    _insert_health_row(
        clean_health_table, pipeline_date=TEST_MONTH_START, total_cost_usd=1.0
    )
    condition = check_monthly_cost_over_budget(
        clean_health_table,
        month_start=TEST_MONTH_START,
        monthly_budget_usd=Decimal("20.00"),
    )
    assert condition is None


def test_monthly_cost_zero_budget_returns_none(
    clean_health_table: sa.Connection,
) -> None:
    """Ngân sách 0 (config chưa điền) -> None, KHÔNG chia cho 0."""
    _insert_health_row(
        clean_health_table, pipeline_date=TEST_MONTH_START, total_cost_usd=100.0
    )
    condition = check_monthly_cost_over_budget(
        clean_health_table,
        month_start=TEST_MONTH_START,
        monthly_budget_usd=Decimal(0),
    )
    assert condition is None
