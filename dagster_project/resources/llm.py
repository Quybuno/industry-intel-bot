"""Resource LLM — chọn provider theo biến môi trường (task 0.12 mục 4).

Logic chọn provider giống HỆT `cli.py::score()` (task 0.8) — cố tình KHÔNG import thẳng từ
`cli.py` vì các hàm ở đó không export (tên riêng, gắn liền option Typer); đây là bản đóng
gói lại CÙNG các hàm `DeepSeekProvider.from_config`/`MockProvider`/`max_per_run` đã có sẵn ở
`src/intel_bot/score/...`, không viết lại business logic nào của provider.

**Khác CLI một điểm có chủ đích:** CLI mặc định `--provider mock` (an toàn cho gõ tay,
xem lại ngay). Resource này BẮT BUỘC biến môi trường `LLM_PROVIDER`, không default —
lịch chạy 05:00 không có người xem, và bài học từ task 0.11 (PROGRESS.md §5B) là dữ liệu
`mock` từng lọt vào gold vì chạy trên bài thật rồi quên đổi provider. Để lịch chạy tự động
âm thầm rơi về mock nếu quên cấu hình là lặp lại đúng lỗi đó — thà dừng rõ ràng (P4).
"""

from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource, Failure

from src.intel_bot.score.cost import ModelPricing
from src.intel_bot.score.providers.base import LLMProvider
from src.intel_bot.score.providers.deepseek import DeepSeekProvider
from src.intel_bot.score.providers.deepseek import max_per_run as deepseek_max_per_run
from src.intel_bot.score.providers.mock import ZERO_PRICING, MockProvider


class LLMResource(ConfigurableResource[Any]):
    """`provider_name` đọc từ biến môi trường `LLM_PROVIDER` (`os.environ.get`, gán ở
    `definitions.py`) — 'mock' hoặc 'deepseek'. `deepseek_api_key` từ `DEEPSEEK_API_KEY`,
    chỉ cần khi provider là deepseek.

    `ConfigurableResource[Any]`: xem giải thích ở `PostgresResource`."""

    provider_name: str
    deepseek_api_key: str = ""
    default_batch_size: int = 10

    def build(self) -> tuple[LLMProvider, ModelPricing, int]:
        """Trả `(provider, pricing, batch_size)` — `batch_size` lấy `max_per_run` của
        provider trong `config/models.yaml` nếu có, không thì dùng `default_batch_size`."""
        if self.provider_name == "mock":
            return MockProvider(), ZERO_PRICING, self.default_batch_size
        if self.provider_name == "deepseek":
            if not self.deepseek_api_key:
                raise Failure(
                    "Thiếu DEEPSEEK_API_KEY trong môi trường — LLM_PROVIDER=deepseek "
                    "nhưng không có key, không tự bịa."
                )
            try:
                provider = DeepSeekProvider.from_config(
                    api_key=self.deepseek_api_key, tier="fast"
                )
            except ValueError as exc:
                raise Failure(str(exc)) from exc
            return (
                provider,
                provider.pricing,
                deepseek_max_per_run() or self.default_batch_size,
            )
        raise Failure(
            f"LLM_PROVIDER không hỗ trợ: '{self.provider_name}' (chỉ 'mock' hoặc 'deepseek')."
        )
