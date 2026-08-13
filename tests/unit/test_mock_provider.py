"""Unit test cho MockProvider — không gọi mạng, không cần env var, không cần DB."""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.intel_bot.contracts.llm_score import INDUSTRY_TAGS
from src.intel_bot.score.providers.base import (
    ProviderUnavailableError,
    ScoreFailure,
    ScoreRequest,
    ScoreSuccess,
    SummaryFailure,
    SummarySuccess,
    classify_validation_error,
)
from src.intel_bot.score.providers.mock import (
    ZERO_PRICING,
    MockFailureRates,
    MockProvider,
)


def _request(
    article_id: uuid.UUID | None = None, prompt: str = "prompt text " * 20
) -> ScoreRequest:
    return ScoreRequest(
        article_id=article_id or uuid.uuid4(),
        title="Some title",
        snippet="Some snippet",
        prompt=prompt,
        prompt_version="score_v2.0.0",
    )


# ---------------------------------------------------------------------------
# MockFailureRates — validation
# ---------------------------------------------------------------------------


def test_failure_rates_default_all_zero() -> None:
    rates = MockFailureRates()
    assert rates.json_parse_error == 0.0
    assert rates.timeout == 0.0


@pytest.mark.parametrize("bad_rate", [-0.1, 1.1, 2.0])
def test_failure_rates_out_of_unit_interval_raises(bad_rate: float) -> None:
    with pytest.raises(ValueError):
        MockFailureRates(json_parse_error=bad_rate)


def test_failure_rates_sum_over_one_raises() -> None:
    with pytest.raises(ValueError):
        MockFailureRates(json_parse_error=0.5, schema_violation=0.6)


def test_failure_rates_sum_exactly_one_is_valid() -> None:
    rates = MockFailureRates(
        json_parse_error=0.25, schema_violation=0.25, out_of_range=0.25, timeout=0.25
    )
    assert rates.timeout == 0.25


# ---------------------------------------------------------------------------
# MockProvider — thành công mặc định, xác định theo hash(article_id)
# ---------------------------------------------------------------------------


def test_default_provider_always_succeeds() -> None:
    provider = MockProvider()
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreSuccess)


def test_success_result_score_fields_within_1_to_10() -> None:
    provider = MockProvider()
    for _ in range(20):
        outcome = provider.score_batch([_request()])[0]
        assert isinstance(outcome, ScoreSuccess)
        for value in (
            outcome.result.credibility,
            outcome.result.importance,
            outcome.result.depth,
            outcome.result.practicality,
        ):
            assert 1 <= value <= 10


def test_success_result_industry_tags_subset_of_closed_set() -> None:
    provider = MockProvider()
    for _ in range(20):
        outcome = provider.score_batch([_request()])[0]
        assert isinstance(outcome, ScoreSuccess)
        assert set(outcome.result.industry_tags) <= INDUSTRY_TAGS
        assert len(outcome.result.industry_tags) >= 1


def test_deterministic_same_article_id_same_outcome() -> None:
    """Test bắt buộc của task 0.7: gọi 2 lần cùng input → kết quả giống hệt."""
    article_id = uuid.uuid4()
    provider = MockProvider()
    outcome_1 = provider.score_batch([_request(article_id=article_id)])[0]
    outcome_2 = provider.score_batch([_request(article_id=article_id)])[0]
    assert outcome_1 == outcome_2


def test_deterministic_across_separate_provider_instances() -> None:
    """Xác định không phụ thuộc instance provider hay lần gọi — chỉ phụ thuộc article_id."""
    article_id = uuid.uuid4()
    outcome_1 = MockProvider().score_batch([_request(article_id=article_id)])[0]
    outcome_2 = MockProvider().score_batch([_request(article_id=article_id)])[0]
    assert outcome_1 == outcome_2


def test_score_batch_empty_list_returns_empty() -> None:
    assert MockProvider().score_batch([]) == []


def test_score_batch_preserves_article_id() -> None:
    article_id = uuid.uuid4()
    outcome = MockProvider().score_batch([_request(article_id=article_id)])[0]
    assert outcome.article_id == article_id


# ---------------------------------------------------------------------------
# MockProvider — ép lỗi theo tỷ lệ (dùng cho task 0.9)
# ---------------------------------------------------------------------------


def test_forced_json_parse_error_rate_one_always_fails_that_way() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(json_parse_error=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    assert outcome.failure_reason == "json_parse_error"


def test_forced_json_parse_error_raw_response_is_genuinely_invalid_json() -> None:
    """raw_response phải THỰC SỰ là JSON hỏng — không phải nhãn giả, đi qua parser thật."""
    provider = MockProvider(failure_rates=MockFailureRates(json_parse_error=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    with pytest.raises(json.JSONDecodeError):
        json.loads(outcome.raw_response)


def test_forced_schema_violation_rate_one_always_fails_that_way() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(schema_violation=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    assert outcome.failure_reason == "schema_violation"


def test_forced_schema_violation_raw_response_is_valid_json_but_incomplete() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(schema_violation=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    parsed = json.loads(outcome.raw_response)  # JSON hợp lệ về cú pháp
    assert "confidence" not in parsed  # nhưng thiếu trường bắt buộc


def test_forced_out_of_range_rate_one_always_fails_that_way() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(out_of_range=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    assert outcome.failure_reason == "out_of_range"


def test_forced_out_of_range_raw_response_has_score_outside_1_to_10() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(out_of_range=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    parsed = json.loads(outcome.raw_response)
    assert not (1 <= parsed["importance"] <= 10)


def test_forced_timeout_rate_one_always_fails_that_way() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(timeout=1.0))
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreFailure)
    assert outcome.failure_reason == "timeout"
    assert outcome.raw_response == ""


def test_score_batch_never_raises_for_per_record_failures() -> None:
    """§10.1: lỗi từng bản ghi là giá trị trả về, không phải exception."""
    provider = MockProvider(
        failure_rates=MockFailureRates(
            json_parse_error=0.25,
            schema_violation=0.25,
            out_of_range=0.25,
            timeout=0.25,
        )
    )
    requests = [_request() for _ in range(30)]
    outcomes = provider.score_batch(requests)  # không được raise
    assert len(outcomes) == 30
    assert all(isinstance(o, ScoreSuccess | ScoreFailure) for o in outcomes)


# ---------------------------------------------------------------------------
# estimate_cost — token/latency giả lập để test đường tính chi phí
# ---------------------------------------------------------------------------


def test_estimate_cost_returns_positive_decimal_for_nonempty_batch() -> None:
    provider = MockProvider()
    cost = provider.estimate_cost([_request()])
    assert isinstance(cost, Decimal)
    assert cost > 0


def test_estimate_cost_zero_for_empty_batch() -> None:
    assert MockProvider().estimate_cost([]) == Decimal(0)


def test_estimate_cost_scales_with_batch_size() -> None:
    provider = MockProvider()
    single = provider.estimate_cost([_request(prompt="x" * 400)])
    double = provider.estimate_cost(
        [_request(prompt="x" * 400), _request(prompt="x" * 400)]
    )
    assert double > single


def test_success_outcome_carries_simulated_tokens_and_latency() -> None:
    provider = MockProvider(simulated_latency_ms=123)
    outcome = provider.score_batch([_request()])[0]
    assert isinstance(outcome, ScoreSuccess)
    assert outcome.input_tokens > 0
    assert outcome.output_tokens > 0
    assert outcome.latency_ms == 123


# ---------------------------------------------------------------------------
# classify_validation_error — dùng chung cho mock lẫn provider thật (task 0.8)
# ---------------------------------------------------------------------------


def test_classify_validation_error_range_violation() -> None:
    from src.intel_bot.contracts.llm_score import ScoreResult

    try:
        ScoreResult(
            credibility=11,
            importance=5,
            depth=5,
            practicality=5,
            industry_tags=["ai"],
            confidence="high",
        )
        pytest.fail("expected ValidationError")
    except ValidationError as exc:
        assert classify_validation_error(exc) == "out_of_range"


def test_classify_validation_error_missing_field() -> None:
    from src.intel_bot.contracts.llm_score import ScoreResult

    try:
        ScoreResult(
            credibility=5, importance=5, depth=5, practicality=5, industry_tags=["ai"]
        )  # type: ignore[call-arg]
        pytest.fail("expected ValidationError")
    except ValidationError as exc:
        assert classify_validation_error(exc) == "schema_violation"


# ---------------------------------------------------------------------------
# summarize_batch — task 0.8/0.9 (mở rộng Protocol so với task 0.7)
# ---------------------------------------------------------------------------


def test_summarize_batch_default_always_succeeds() -> None:
    outcome = MockProvider().summarize_batch([_request()])[0]
    assert isinstance(outcome, SummarySuccess)
    assert len(outcome.result.summary_vi) == 5


def test_summarize_batch_deterministic_same_article_id() -> None:
    article_id = uuid.uuid4()
    provider = MockProvider()
    outcome_1 = provider.summarize_batch([_request(article_id=article_id)])[0]
    outcome_2 = provider.summarize_batch([_request(article_id=article_id)])[0]
    assert outcome_1 == outcome_2


def test_summarize_batch_forced_json_parse_error() -> None:
    provider = MockProvider(failure_rates=MockFailureRates(json_parse_error=1.0))
    outcome = provider.summarize_batch([_request()])[0]
    assert isinstance(outcome, SummaryFailure)
    assert outcome.failure_reason == "json_parse_error"
    with pytest.raises(json.JSONDecodeError):
        json.loads(outcome.raw_response)


def test_summarize_batch_forced_schema_violation_is_correctly_classified() -> None:
    """summarize dùng validate() thật — 'schema_violation' phải là kết quả PHÂN LOẠI THẬT,
    không phải nhãn gán cứng (SummaryResult không có khái niệm out_of_range)."""
    provider = MockProvider(failure_rates=MockFailureRates(schema_violation=1.0))
    outcome = provider.summarize_batch([_request()])[0]
    assert isinstance(outcome, SummaryFailure)
    assert outcome.failure_reason == "schema_violation"


def test_summarize_batch_empty_list_returns_empty() -> None:
    assert MockProvider().summarize_batch([]) == []


# ---------------------------------------------------------------------------
# raise_unavailable — mô phỏng "cả provider không dùng được" (§10.5, task 0.9)
# ---------------------------------------------------------------------------


def test_raise_unavailable_score_batch_raises_not_returns_failure() -> None:
    provider = MockProvider(raise_unavailable=True)
    with pytest.raises(ProviderUnavailableError):
        provider.score_batch([_request()])


def test_raise_unavailable_summarize_batch_raises() -> None:
    provider = MockProvider(raise_unavailable=True)
    with pytest.raises(ProviderUnavailableError):
        provider.summarize_batch([_request()])


def test_raise_unavailable_does_not_affect_estimate_cost() -> None:
    """estimate_cost không gọi 'provider' thật nên không nên bị raise_unavailable chặn."""
    provider = MockProvider(raise_unavailable=True)
    cost = provider.estimate_cost([_request()])
    assert isinstance(cost, Decimal)


# ---------------------------------------------------------------------------
# ZERO_PRICING — dùng khi CLI chọn --provider mock (task 0.8 mục 3)
# ---------------------------------------------------------------------------


def test_zero_pricing_gives_zero_cost_for_any_token_count() -> None:
    from src.intel_bot.score.cost import compute_cost_usd

    cost = compute_cost_usd(
        pricing=ZERO_PRICING,
        input_cache_hit_tokens=1_000_000,
        input_cache_miss_tokens=1_000_000,
        output_tokens=1_000_000,
    )
    assert cost == Decimal(0)
