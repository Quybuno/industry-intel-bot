"""Resource notifier — 2 nhiệm vụ ĐỘC LẬP, KHÔNG đi chung đường (§18.3, task 1.6 mục 2):

1. `ping_heartbeat()` — dead-man's-switch bên ngoài (PRODUCTION_PLAN §7.5, task 0.13 mục 7).
   **Cơ chế DUY NHẤT phát hiện "pipeline không hề chạy"** — nếu máy chạy Dagster tắt/treo,
   không có gì tự gửi alert được nữa; dịch vụ bên ngoài (healthchecks.io) tự biết report gọi
   ping đã ngừng và cảnh báo thay QUA EMAIL (kênh của chính dịch vụ đó, không phải Telegram
   dưới đây). Đây là lý do §18.3 tách "Không có heartbeat" (kênh ngoài) khỏi mọi điều kiện
   khác (kênh nội bộ) — khi máy chết, kênh nội bộ (Telegram) cũng chết theo, chỉ kênh ngoài
   độc lập với máy mới báo được.
2. `send_alert()` — kênh nội bộ chủ động (task 1.6, §18.3 "Slack/Telegram") cho 4 sensor
   (`dagster_project/sensors.py`): run failed, digest rỗng, anomaly/quarantine, cost vượt
   ngân sách. Dùng Telegram Bot API (`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`).

Lỗi ở CẢ HAI hàm chỉ log warning, KHÔNG raise (P4) — cảnh báo hỏng không được biến thành sự
cố (rào chắn task 1.6 mục 5, cùng tinh thần §7.5 với heartbeat).

**Phát hiện thật khi verify (không phải suy đoán):** Telegram Bot API đặt token NGAY TRONG
URL (`/bot{token}/sendMessage`, thiết kế của Telegram — không phải lựa chọn ở đây) — code ở
đây KHÔNG tự log URL/token, nhưng thư viện `httpx` tự log dòng
`INFO:httpx:HTTP Request: POST <url> ...` (kèm token trong URL) nếu logger `"httpx"` đang ở
mức INFO trở xuống — quan sát được thật khi bật `logging.basicConfig(level=logging.INFO)` lúc
tự test bằng tay. `send_alert()` vì vậy nâng TẠM mức logger `"httpx"` lên WARNING chỉ trong
lúc gọi (`_suppress_httpx_url_logging()`), khôi phục ngay sau — không đổi cấu hình logging
toàn cục của phần còn lại của app (RSS/github fetcher vẫn log INFO httpx bình thường, URL của
chúng không chứa secret nên không cần chặn).
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from typing import Any, Protocol

import httpx
from dagster import ConfigurableResource


class _WarnLogger(Protocol):
    """Chỉ cần 2 method của logging.Logger/DagsterLogManager — dùng Protocol thay vì `Any`
    để không phụ thuộc cứng vào kiểu logger cụ thể của Dagster."""

    def info(self, msg: str) -> None: ...
    def warning(self, msg: str) -> None: ...


@contextlib.contextmanager
def _suppress_httpx_url_logging() -> Iterator[None]:
    """Nâng tạm mức logger `"httpx"` lên WARNING — xem giải thích đầy đủ ở docstring module
    (token Telegram nằm trong URL, httpx tự log URL ở mức INFO)."""
    httpx_logger = logging.getLogger("httpx")
    original_level = httpx_logger.level
    httpx_logger.setLevel(logging.WARNING)
    try:
        yield
    finally:
        httpx_logger.setLevel(original_level)


class NotifierResource(ConfigurableResource[Any]):
    """`heartbeat_url`/`telegram_bot_token`/`telegram_chat_id` từ biến môi trường tương ứng
    (gán ở `definitions.py`) — rỗng thì bỏ qua (log warning), KHÔNG bắt buộc phải cấu hình để
    chạy dev/test (khác `llm` resource task 0.12 — heartbeat/alert không nguy hiểm nếu thiếu,
    chỉ mất khả năng quan sát, không làm sai dữ liệu).

    `ConfigurableResource[Any]`: xem giải thích ở `PostgresResource`."""

    heartbeat_url: str = ""
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
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

    def send_alert(self, message: str, *, logger: _WarnLogger) -> bool:
        """Gửi `message` qua Telegram Bot API. Trả về True nếu gửi thành công, False nếu bỏ
        qua/lỗi — sensor tự quyết định có cập nhật cursor chống-lặp hay không dựa vào giá trị
        này (gửi lỗi thì KHÔNG cập nhật, để tick sau thử lại thay vì coi như "đã báo rồi").

        KHÔNG log nguyên văn `telegram_bot_token` (rào chắn task 1.6 "KHÔNG log token") — chỉ
        log độ dài khi cần chẩn đoán thiếu cấu hình, không log giá trị."""
        if not self.telegram_bot_token or not self.telegram_chat_id:
            logger.warning(
                "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID chưa cấu hình đủ — bỏ qua gửi alert "
                f"(nội dung: {message[:80]}...)."
            )
            return False
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
        try:
            with _suppress_httpx_url_logging():
                response = httpx.post(
                    url,
                    json={"chat_id": self.telegram_chat_id, "text": message},
                    timeout=self.timeout_seconds,
                )
            response.raise_for_status()
            logger.info("Đã gửi alert Telegram thành công.")
            return True
        except httpx.HTTPError as exc:
            logger.warning(f"Gửi alert Telegram lỗi (không làm fail sensor, P4): {exc}")
            return False
