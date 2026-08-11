"""Tính chi phí USD từ token thật + bảng giá trong config/models.yaml.

Toàn bộ tính bằng `Decimal` — KHÔNG dùng float cho tiền tệ (rào chắn task 0.8). Giá đọc từ
config, KHÔNG hardcode tên model hay bảng giá trong code (AGENTS.md mục 4).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MODELS_PATH = Path("config/models.yaml")

_TOKENS_PER_UNIT = Decimal(1_000_000)


@dataclass(frozen=True)
class ModelPricing:
    """Bảng giá một model — Decimal, đọc nguyên văn từ YAML qua `str()` (không qua float)."""

    input_cache_hit_usd_per_1m: Decimal
    input_cache_miss_usd_per_1m: Decimal
    output_usd_per_1m: Decimal
    batch_discount: Decimal


def load_model_pricing(
    provider_name: str, model_id: str, *, models_path: Path | str = DEFAULT_MODELS_PATH
) -> ModelPricing:
    """Đọc bảng giá một model cụ thể từ config/models.yaml.

    Lỗi rõ ràng (khớp tinh thần P4) nếu thiếu provider/model/pricing — KHÔNG đoán giá trị.
    """
    path = Path(models_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    providers: list[dict[str, Any]] = raw.get("providers", [])

    provider = next((p for p in providers if p.get("name") == provider_name), None)
    if provider is None:
        raise ValueError(f"Không tìm thấy provider '{provider_name}' trong {path}")

    models: list[dict[str, Any]] = provider.get("models", [])
    model = next((m for m in models if m.get("id") == model_id), None)
    if model is None:
        raise ValueError(
            f"Không tìm thấy model '{model_id}' của provider '{provider_name}' trong {path}"
        )

    pricing = model.get("pricing")
    if not pricing:
        raise ValueError(f"Model '{model_id}' thiếu mục 'pricing' trong {path}")

    for required_key in (
        "input_cache_hit_usd_per_1m",
        "input_cache_miss_usd_per_1m",
        "output_usd_per_1m",
    ):
        if required_key not in pricing:
            raise ValueError(f"Model '{model_id}' thiếu '{required_key}' trong {path}")

    batch_discount = provider.get("batch_discount", 0)

    return ModelPricing(
        input_cache_hit_usd_per_1m=Decimal(str(pricing["input_cache_hit_usd_per_1m"])),
        input_cache_miss_usd_per_1m=Decimal(
            str(pricing["input_cache_miss_usd_per_1m"])
        ),
        output_usd_per_1m=Decimal(str(pricing["output_usd_per_1m"])),
        batch_discount=Decimal(str(batch_discount)),
    )


def compute_cost_usd(
    *,
    pricing: ModelPricing,
    input_cache_hit_tokens: int,
    input_cache_miss_tokens: int,
    output_tokens: int,
) -> Decimal:
    """Chi phí USD = giá × số token / 1.000.000, áp `batch_discount`. Toàn bộ bằng Decimal."""
    raw_cost = (
        Decimal(input_cache_hit_tokens)
        / _TOKENS_PER_UNIT
        * pricing.input_cache_hit_usd_per_1m
        + Decimal(input_cache_miss_tokens)
        / _TOKENS_PER_UNIT
        * pricing.input_cache_miss_usd_per_1m
        + Decimal(output_tokens) / _TOKENS_PER_UNIT * pricing.output_usd_per_1m
    )
    discount_factor = Decimal(1) - pricing.batch_discount
    return raw_cost * discount_factor
