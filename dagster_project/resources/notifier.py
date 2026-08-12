"""Resource notifier — heartbeat ping ra dịch vụ dead-man's-switch bên ngoài
(PRODUCTION_PLAN §7.5, task 0.13 mục 7).

**Đây là cơ chế duy nhất phát hiện "pipeline không hề chạy"** (§7.5) — nếu máy chạy
Dagster tắt/treo, không có gì tự gửi alert được nữa; dịch vụ bên ngoài (healthchecks.io hay
tương đương) tự biết report gọi ping đã ngừng và cảnh báo thay. Vì vậy lỗi ping KHÔNG được
làm fail asset (asset published_site vẫn coi là thành công dù ping lỗi) — asset fail vì
ping lỗi sẽ che mất kết quả publish thật, và im lặng-vì-asset-fail lại đúng là lỗi vận hành
mà §7.5 mô tả.
"""

from __future__ import annotations

from typing import Any, Protocol

import httpx
from dagster import ConfigurableResource


class _WarnLogger(Protocol):
    """Chỉ cần 2 method của logging.Logger/DagsterLogManager — dùng Protocol thay vì `Any`
    để không phụ thuộc cứng vào kiểu logger cụ thể của Dagster."""

    def info(self, msg: str) -> None: ...
    def warning(self, msg: str) -> None: ...


class NotifierResource(ConfigurableResource[Any]):
    """`heartbeat_url` từ biến môi trường `HEARTBEAT_URL` (gán ở `definitions.py`) — rỗng
    thì bỏ qua ping (log warning), KHÔNG bắt buộc phải cấu hình để chạy dev/test.

    `ConfigurableResource[Any]`: xem giải thích ở `PostgresResource`."""

    heartbeat_url: str = ""
    timeout_seconds: float = 10.0

    def ping_heartbeat(self, *, logger: _WarnLogger) -> None:
        """Gọi HTTP GET tới `heartbeat_url`. Lỗi (thiếu URL, timeout, HTTP lỗi) chỉ log
        warning — KHÔNG raise, đúng rào chắn task 0.13 mục 7."""
        if not self.heartbeat_url:
            logger.warning(
                "HEARTBEAT_URL chưa cấu hình — bỏ qua ping heartbeat (§7.5 sẽ không hoạt "
                "động cho tới khi cấu hình biến môi trường này)."
            )
            return
        try:
            response = httpx.get(self.heartbeat_url, timeout=self.timeout_seconds)
            response.raise_for_status()
            logger.info(f"Heartbeat ping OK: {response.status_code}")
        except httpx.HTTPError as exc:
            logger.warning(f"Heartbeat ping lỗi (không làm fail asset, §7.5): {exc}")
