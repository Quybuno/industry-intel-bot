"""Freshness policy (task 1.5, PRODUCTION_PLAN §13.4) — CHỈ định nghĩa policy và làm nó
hiện trong Dagster UI. KHÔNG cài sensor, KHÔNG gửi cảnh báo đi đâu (rào chắn task 1.5, việc
gửi alert là prompt 15).

**API đã verify thật trước khi viết, KHÔNG đoán (đúng rào chắn nhiệm vụ 5) — 2 vòng:**

1. Introspect `dagster` (`dagster==1.13.17`, khớp AGENTS.md): `dir(dagster)` lọc "fresh" ra
   6 API — `FreshnessPolicy` (mới), `LegacyFreshnessPolicy` (cũ, tên đã đánh dấu "Legacy"),
   `apply_freshness_policy`, `build_last_update_freshness_checks`,
   `build_sensor_for_freshness_checks` (KHÔNG dùng — rào chắn "không sensor"),
   `build_time_partition_freshness_checks`.
2. Thử `build_last_update_freshness_checks` trước (đọc docstring, có vẻ khớp — cho `severity`
   độc lập với ngưỡng thời gian). Load `Definitions` thật thì dagster tự in
   `SupersessionWarning: Function build_last_update_freshness_checks is superseded... Attach
   FreshnessPolicy objects to your assets instead` — bằng chứng RUNTIME, không phải suy đoán
   từ docstring. Đổi sang đường được khuyến nghị: `FreshnessPolicy.time_window()` +
   `apply_freshness_policy()` qua `Definitions.map_resolved_asset_specs()` (verify tồn tại + đúng
   chữ ký bằng `inspect.signature` trước khi dùng).

**Lệch so với §13.4 — cần nói rõ (DONE WHEN mục 5):** `FreshnessPolicy.time_window()` bắt
buộc `fail_window`, `warn_window` là tuỳ chọn — nghĩa là API hiện hành không biểu diễn được
"chỉ có một mức WARN, không bao giờ FAIL" mà đề bài mô tả cho `raw_rss`/`article_scores`
("< 26h, Warning" — KHÔNG có ngưỡng thứ hai nào trong plan). Bắt buộc phải chọn một
`fail_window`. KHÔNG suy diễn liều lượng gần 26h (dễ gây FAIL giả ngay khi WARN vừa xảy ra,
biến "Warning" thành "Critical" ngoài ý plan) — chọn `freshness_fail_ceiling_hours` = 168h
(7 ngày, xem config/app.yaml) làm "trần an toàn": asset chưa từng dự kiến im lặng 7 ngày
liền mà không đã có báo động nào khác (heartbeat §7.5) kêu trước — con số này CHỦ Ý rộng,
không phải một SLA thật, chỉ để API có giá trị hợp lệ. `mart_daily_digest` (Critical, §13.4)
dùng ĐÚNG 26h làm `fail_window`, không có `warn_window` — khớp thẳng plan, không suy diễn gì.

Không dùng `deadline_cron` (tham số của `build_last_update_freshness_checks`, không áp dụng
cho `FreshnessPolicy.time_window`) — SLA "< 26h" trong plan là cửa sổ TRƯỢT theo thời gian
thực, không gắn với một khung giờ cố định trong ngày (chưa có Dagster daemon production thật
chạy lịch 05:00 — xem docs/PROGRESS.md mục 5C).
"""

from __future__ import annotations

from datetime import timedelta

from dagster import Definitions, FreshnessPolicy, apply_freshness_policy

from src.intel_bot.config import load_config_dir

_observability_cfg = load_config_dir().get("app", {}).get("observability", {})
_sla_hours = int(_observability_cfg.get("freshness_sla_hours", 0))
_fail_ceiling_hours = int(_observability_cfg.get("freshness_fail_ceiling_hours", 0))
if _sla_hours <= 0 or _fail_ceiling_hours <= 0:
    raise ValueError(
        "Thiếu config app.yaml: observability.freshness_sla_hours/"
        "freshness_fail_ceiling_hours — không tự bịa SLA."
    )

#: raw_rss/article_scores (§13.4): SLA warn ở đúng giờ plan định nghĩa; fail_window là trần
#: an toàn tự chọn (xem docstring module), KHÔNG phải SLA thật.
_WARN_ONLY_POLICY = FreshnessPolicy.time_window(
    fail_window=timedelta(hours=_fail_ceiling_hours),
    warn_window=timedelta(hours=_sla_hours),
)

#: mart_daily_digest (§13.4): Critical đúng ở giờ plan định nghĩa, không có mức warn riêng.
_CRITICAL_ONLY_POLICY = FreshnessPolicy.time_window(
    fail_window=timedelta(hours=_sla_hours)
)

_WARN_ASSET_KEYS = ["raw_rss", "article_scores"]
_CRITICAL_ASSET_KEYS = ["mart_daily_digest"]


def apply_freshness_policies(defs: Definitions) -> Definitions:
    """Gắn freshness policy lên đúng 3 asset của §13.4 — gọi MỘT LẦN ở definitions.py, sau
    khi `Definitions` gốc đã dựng xong. Trả về `Definitions` MỚI (map_asset_specs không sửa
    tại chỗ)."""
    defs = defs.map_resolved_asset_specs(
        func=lambda spec: apply_freshness_policy(spec, _WARN_ONLY_POLICY),
        selection=_WARN_ASSET_KEYS,
    )
    defs = defs.map_resolved_asset_specs(
        func=lambda spec: apply_freshness_policy(spec, _CRITICAL_ONLY_POLICY),
        selection=_CRITICAL_ASSET_KEYS,
    )
    return defs
