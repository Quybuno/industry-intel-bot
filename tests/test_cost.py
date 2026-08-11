"""Unit test cho cost.py — Decimal chính xác, không DB/mạng."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from src.intel_bot.score.cost import ModelPricing, compute_cost_usd, load_model_pricing

FAKE_PRICING = ModelPricing(
    input_cache_hit_usd_per_1m=Decimal("1.00"),
    input_cache_miss_usd_per_1m=Decimal("10.00"),
    output_usd_per_1m=Decimal("20.00"),
    batch_discount=Decimal(0),
)


def test_compute_cost_usd_exact_decimal_no_hit_no_miss_no_output() -> None:
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=0,
        output_tokens=0,
    )
    assert cost == Decimal(0)


def test_compute_cost_usd_cache_miss_only_matches_exact_value() -> None:
    # 1,000,000 token cache-miss * $10.00/1M = đúng $10.00
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=1_000_000,
        output_tokens=0,
    )
    assert cost == Decimal("10.00")


def test_compute_cost_usd_cache_hit_only_matches_exact_value() -> None:
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=1_000_000,
        input_cache_miss_tokens=0,
        output_tokens=0,
    )
    assert cost == Decimal("1.00")


def test_compute_cost_usd_output_only_matches_exact_value() -> None:
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=0,
        output_tokens=1_000_000,
    )
    assert cost == Decimal("20.00")


def test_compute_cost_usd_combined_matches_exact_sum() -> None:
    # 500k hit ($0.50) + 200k miss ($2.00) + 100k output ($2.00) = $4.50 chính xác
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=500_000,
        input_cache_miss_tokens=200_000,
        output_tokens=100_000,
    )
    assert cost == Decimal("4.50")


def test_compute_cost_usd_applies_batch_discount_exactly() -> None:
    pricing_with_discount = ModelPricing(
        input_cache_hit_usd_per_1m=Decimal(0),
        input_cache_miss_usd_per_1m=Decimal("10.00"),
        output_usd_per_1m=Decimal(0),
        batch_discount=Decimal("0.5"),
    )
    # 1M token miss * $10.00 = $10.00, giảm 50% => đúng $5.00
    cost = compute_cost_usd(
        pricing=pricing_with_discount,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=1_000_000,
        output_tokens=0,
    )
    assert cost == Decimal("5.00")


def test_compute_cost_usd_zero_discount_equals_no_discount() -> None:
    cost_a = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=1000,
        output_tokens=0,
    )
    pricing_explicit_zero = ModelPricing(
        input_cache_hit_usd_per_1m=FAKE_PRICING.input_cache_hit_usd_per_1m,
        input_cache_miss_usd_per_1m=FAKE_PRICING.input_cache_miss_usd_per_1m,
        output_usd_per_1m=FAKE_PRICING.output_usd_per_1m,
        batch_discount=Decimal("0.0"),
    )
    cost_b = compute_cost_usd(
        pricing=pricing_explicit_zero,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=1000,
        output_tokens=0,
    )
    assert cost_a == cost_b


def test_compute_cost_usd_returns_decimal_type() -> None:
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=1,
        input_cache_miss_tokens=1,
        output_tokens=1,
    )
    assert isinstance(cost, Decimal)


def test_compute_cost_usd_realistic_small_article_nonzero() -> None:
    """Mô phỏng 1 bài thật: ~1100 input token, ~800 output token — chi phí phải > 0."""
    cost = compute_cost_usd(
        pricing=FAKE_PRICING,
        input_cache_hit_tokens=0,
        input_cache_miss_tokens=1100,
        output_tokens=800,
    )
    assert cost > Decimal(0)


# ---------------------------------------------------------------------------
# load_model_pricing — đọc file YAML giả lập (tmp_path), không đụng config/models.yaml thật
# ---------------------------------------------------------------------------

FAKE_MODELS_YAML = """
providers:
  - name: fake_provider
    type: cloud
    batch_discount: 0.25
    models:
      - id: fake-model-a
        tier: fast
        pricing:
          input_cache_hit_usd_per_1m: "0.5"
          input_cache_miss_usd_per_1m: "5.0"
          output_usd_per_1m: "15.0"
"""


@pytest.fixture()
def fake_models_path(tmp_path: Path) -> Path:
    path = tmp_path / "fake_models.yaml"
    path.write_text(FAKE_MODELS_YAML, encoding="utf-8")
    return path


def test_load_model_pricing_reads_exact_decimal_values(fake_models_path: Path) -> None:
    pricing = load_model_pricing(
        "fake_provider", "fake-model-a", models_path=fake_models_path
    )
    assert pricing.input_cache_hit_usd_per_1m == Decimal("0.5")
    assert pricing.input_cache_miss_usd_per_1m == Decimal("5.0")
    assert pricing.output_usd_per_1m == Decimal("15.0")
    assert pricing.batch_discount == Decimal("0.25")


def test_load_model_pricing_unknown_provider_raises(fake_models_path: Path) -> None:
    with pytest.raises(ValueError, match="provider"):
        load_model_pricing(
            "nonexistent_provider", "fake-model-a", models_path=fake_models_path
        )


def test_load_model_pricing_unknown_model_raises(fake_models_path: Path) -> None:
    with pytest.raises(ValueError, match="model"):
        load_model_pricing(
            "fake_provider", "nonexistent-model", models_path=fake_models_path
        )


def test_load_model_pricing_missing_pricing_block_raises(tmp_path: Path) -> None:
    path = tmp_path / "bad_models.yaml"
    path.write_text(
        """
providers:
  - name: fake_provider
    models:
      - id: fake-model-a
        tier: fast
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="pricing"):
        load_model_pricing("fake_provider", "fake-model-a", models_path=path)


def test_load_model_pricing_missing_batch_discount_defaults_to_zero(
    tmp_path: Path,
) -> None:
    path = tmp_path / "no_discount.yaml"
    path.write_text(
        """
providers:
  - name: fake_provider
    models:
      - id: fake-model-a
        tier: fast
        pricing:
          input_cache_hit_usd_per_1m: "1"
          input_cache_miss_usd_per_1m: "1"
          output_usd_per_1m: "1"
""",
        encoding="utf-8",
    )
    pricing = load_model_pricing("fake_provider", "fake-model-a", models_path=path)
    assert pricing.batch_discount == Decimal(0)


def test_load_model_pricing_against_real_deepseek_config_does_not_raise() -> None:
    """Kiểm tra config/models.yaml thật đã điền đúng — không đoán giá, chỉ xác nhận đọc được."""
    pricing = load_model_pricing("deepseek", "deepseek-v4-flash")
    assert pricing.input_cache_miss_usd_per_1m > Decimal(0)
