"""Provider mock — xác định (deterministic) theo hash(article_id), KHÔNG gọi mạng.

Dùng cho test/CI (§10.2) và để test luồng quarantine ở task 0.9: có tham số ép sinh lỗi
theo tỷ lệ (JSON hỏng, vi phạm schema, điểm ngoài miền, timeout) và ép "provider không
dùng được" (raise thay vì trả ScoreFailure).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal

from pydantic import ValidationError

from src.intel_bot.contracts.llm_score import INDUSTRY_TAGS, ScoreResult, SummaryResult
from src.intel_bot.score.cost import ModelPricing
from src.intel_bot.score.providers.base import (
    FailureReason,
    ProviderUnavailableError,
    ScoreFailure,
    ScoreOutcome,
    ScoreRequest,
    ScoreSuccess,
    SummaryFailure,
    SummaryOutcome,
    SummarySuccess,
    classify_validation_error,
)

#: Nhãn provider giả lập — KHÔNG phải tên model thật, không đọc từ models.yaml (task 0.7
#: không cài đặt provider thật; xem AGENTS.md mục 4 về việc không hardcode tên model thật).
MOCK_MODEL_NAME = "mock"

#: Mock không tốn tiền thật — dùng khi runner.py cần một ModelPricing hợp lệ cho provider
#: mock (để đi qua đúng đường tính cost.compute_cost_usd() bằng Decimal, kết quả luôn 0).
ZERO_PRICING = ModelPricing(
    input_cache_hit_usd_per_1m=Decimal(0),
    input_cache_miss_usd_per_1m=Decimal(0),
    output_usd_per_1m=Decimal(0),
    batch_discount=Decimal(0),
)

_CONFIDENCE_LEVELS = ("high", "medium", "low")
_SORTED_INDUSTRY_TAGS = sorted(INDUSTRY_TAGS)


@dataclass(frozen=True)
class MockFailureRates:
    """Tỷ lệ [0.0, 1.0] ép sinh từng loại lỗi — dùng để test luồng quarantine (task 0.9).

    Xác định theo hash(article_id): với cùng article_id, cùng failure_rates luôn cho
    cùng kết quả (thành công hoặc đúng loại lỗi nào) — không phụ thuộc lần gọi.
    """

    json_parse_error: float = 0.0
    schema_violation: float = 0.0
    out_of_range: float = 0.0
    timeout: float = 0.0

    def __post_init__(self) -> None:
        for name, rate in (
            ("json_parse_error", self.json_parse_error),
            ("schema_violation", self.schema_violation),
            ("out_of_range", self.out_of_range),
            ("timeout", self.timeout),
        ):
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"Tỷ lệ {name} phải trong [0.0, 1.0], nhận {rate}")
        total = (
            self.json_parse_error
            + self.schema_violation
            + self.out_of_range
            + self.timeout
        )
        if total > 1.0:
            raise ValueError(f"Tổng tỷ lệ lỗi vượt quá 1.0: {total}")

    def as_ordered_thresholds(self) -> tuple[tuple[FailureReason, float], ...]:
        """Ranh giới tích luỹ [0,1) cho từng loại lỗi, thứ tự cố định (dùng để chọn theo bucket)."""
        cumulative = 0.0
        thresholds: list[tuple[FailureReason, float]] = []
        for reason, rate in (
            ("json_parse_error", self.json_parse_error),
            ("schema_violation", self.schema_violation),
            ("out_of_range", self.out_of_range),
            ("timeout", self.timeout),
        ):
            cumulative += rate
            thresholds.append((reason, cumulative))  # type: ignore[arg-type]
        return tuple(thresholds)


@dataclass
class MockProvider:
    """Provider giả lập cho `LLMProvider` — không gọi mạng, chạy được không cần API key.

    `failure_rates` ép sinh lỗi từng bản ghi có kiểm soát; mặc định (tất cả 0.0) luôn
    thành công. `raise_unavailable=True` mô phỏng "cả provider không dùng được" (§10.5) —
    `score_batch()`/`summarize_batch()` raise `ProviderUnavailableError` ngay, không trả
    kết quả nào. `mock_price_per_1k_*` KHÔNG phải bảng giá thật.
    """

    failure_rates: MockFailureRates = field(default_factory=MockFailureRates)
    simulated_latency_ms: int = 50
    mock_price_per_1k_input_tokens: Decimal = Decimal("0.001")
    mock_price_per_1k_output_tokens: Decimal = Decimal("0.002")
    raise_unavailable: bool = False

    def score_batch(self, items: list[ScoreRequest]) -> list[ScoreOutcome]:
        """Chấm một batch — xác định theo hash(article_id), không raise cho lỗi từng bản ghi."""
        if self.raise_unavailable:
            raise ProviderUnavailableError(
                "MockProvider: ép giả lập provider không dùng được"
            )
        return [self._score_one(item) for item in items]

    def summarize_batch(self, items: list[ScoreRequest]) -> list[SummaryOutcome]:
        """Tóm tắt một batch — cùng nguyên tắc xác định/ép lỗi như score_batch()."""
        if self.raise_unavailable:
            raise ProviderUnavailableError(
                "MockProvider: ép giả lập provider không dùng được"
            )
        return [self._summarize_one(item) for item in items]

    def estimate_cost(self, items: list[ScoreRequest]) -> Decimal:
        """Ước tính chi phí giả lập — KHÔNG phải giá thật, chỉ để test đường tính chi phí."""
        total = Decimal(0)
        for item in items:
            input_tokens = _estimate_prompt_tokens(item.prompt)
            output_tokens = _simulated_output_tokens(item.article_id)
            total += (
                Decimal(input_tokens) / 1000 * self.mock_price_per_1k_input_tokens
                + Decimal(output_tokens) / 1000 * self.mock_price_per_1k_output_tokens
            )
        return total

    def _score_one(self, item: ScoreRequest) -> ScoreOutcome:
        digest = _digest(item.article_id)
        bucket = _bucket_from_digest(digest)
        forced_reason = _pick_forced_failure(bucket, self.failure_rates)

        if forced_reason == "timeout":
            return ScoreFailure(
                article_id=item.article_id,
                raw_response="",
                failure_reason="timeout",
                model_name=MOCK_MODEL_NAME,
                prompt_version=item.prompt_version,
                attempt_no=1,
            )

        raw_response = _build_raw_score_response(digest, forced_reason)

        try:
            payload = json.loads(raw_response)
            result = ScoreResult.model_validate(payload)
        except json.JSONDecodeError:
            return ScoreFailure(
                article_id=item.article_id,
                raw_response=raw_response,
                failure_reason="json_parse_error",
                model_name=MOCK_MODEL_NAME,
                prompt_version=item.prompt_version,
                attempt_no=1,
            )
        except ValidationError as exc:
            return ScoreFailure(
                article_id=item.article_id,
                raw_response=raw_response,
                failure_reason=classify_validation_error(exc),
                model_name=MOCK_MODEL_NAME,
                prompt_version=item.prompt_version,
                attempt_no=1,
            )

        return ScoreSuccess(
            article_id=item.article_id,
            result=result,
            input_tokens=_estimate_prompt_tokens(item.prompt),
            output_tokens=_simulated_output_tokens(item.article_id),
            latency_ms=self.simulated_latency_ms,
            model_name=MOCK_MODEL_NAME,
            prompt_version=item.prompt_version,
        )

    def _summarize_one(self, item: ScoreRequest) -> SummaryOutcome:
        digest = _digest(item.article_id)
        bucket = _bucket_from_digest(digest)
        forced_reason = _pick_forced_failure(bucket, self.failure_rates)

        if forced_reason == "timeout":
            return SummaryFailure(
                article_id=item.article_id,
                raw_response="",
                failure_reason="timeout",
                model_name=MOCK_MODEL_NAME,
                prompt_version=item.prompt_version,
                attempt_no=1,
            )

        raw_response = _build_raw_summary_response(digest, forced_reason)

        try:
            payload = json.loads(raw_response)
            result = SummaryResult.model_validate(payload)
        except json.JSONDecodeError:
            return SummaryFailure(
                article_id=item.article_id,
                raw_response=raw_response,
                failure_reason="json_parse_error",
                model_name=MOCK_MODEL_NAME,
                prompt_version=item.prompt_version,
                attempt_no=1,
            )
        except ValidationError as exc:
            return SummaryFailure(
                article_id=item.article_id,
                raw_response=raw_response,
                failure_reason=classify_validation_error(exc),
                model_name=MOCK_MODEL_NAME,
                prompt_version=item.prompt_version,
                attempt_no=1,
            )

        return SummarySuccess(
            article_id=item.article_id,
            result=result,
            input_tokens=_estimate_prompt_tokens(item.prompt),
            output_tokens=_simulated_output_tokens(item.article_id),
            latency_ms=self.simulated_latency_ms,
            model_name=MOCK_MODEL_NAME,
            prompt_version=item.prompt_version,
        )


def _digest(article_id: object) -> bytes:
    """Băm SHA-256 xác định của article_id — cùng input luôn cho cùng bytes."""
    return hashlib.sha256(str(article_id).encode("utf-8")).digest()


def _bucket_from_digest(digest: bytes) -> float:
    """Số thực xác định trong [0.0, 1.0) suy từ digest — dùng để chọn nhánh lỗi/điểm."""
    return int.from_bytes(digest[:8], "big") / float(2**64)


def _pick_forced_failure(
    bucket: float, rates: MockFailureRates
) -> FailureReason | None:
    """Chọn loại lỗi bị ép (nếu có) theo bucket xác định — None nghĩa là thành công."""
    for reason, cumulative_threshold in rates.as_ordered_thresholds():
        if bucket < cumulative_threshold:
            return reason
    return None


def _build_raw_score_response(
    digest: bytes, forced_reason: FailureReason | None
) -> str:
    """Dựng raw_response chấm điểm giả lập — hỏng đúng kiểu forced_reason, hoặc hợp lệ."""
    payload: dict[str, object] = {
        "credibility": (digest[8] % 10) + 1,
        "importance": (digest[9] % 10) + 1,
        "depth": (digest[10] % 10) + 1,
        "practicality": (digest[11] % 10) + 1,
        "industry_tags": _pick_tags(digest),
        "confidence": _CONFIDENCE_LEVELS[digest[12] % len(_CONFIDENCE_LEVELS)],
        "is_breaking": digest[13] % 5 == 0,
    }

    if forced_reason == "out_of_range":
        # Ép điểm ngoài 1-10 — 11 hoặc 0 tuỳ digest, để bao phủ cả hai biên.
        payload["importance"] = 11 if digest[9] % 2 == 0 else 0
    elif forced_reason == "schema_violation":
        # Thiếu trường bắt buộc — JSON hợp lệ, nhưng vi phạm contract (không phải out_of_range).
        del payload["confidence"]

    return _maybe_truncate(json.dumps(payload, ensure_ascii=False), forced_reason)


def _build_raw_summary_response(
    digest: bytes, forced_reason: FailureReason | None
) -> str:
    """Dựng raw_response tóm tắt giả lập — hỏng đúng kiểu forced_reason, hoặc hợp lệ."""
    bullets = [
        f"Gạch đầu dòng giả lập số {i} cho digest {str(digest[i]) * 4}"
        for i in range(5)
    ]
    payload: dict[str, object] = {
        "summary_vi": bullets,
        "why_it_matters_vi": "Lý do đáng chú ý giả lập, đủ dài để qua ràng buộc 20-300 ký tự.",
    }

    if forced_reason == "out_of_range":
        # SummaryResult không có trường số nào ràng buộc khoảng giá trị — dùng độ dài bullet
        # vượt ngưỡng 200 ký tự để mô phỏng "ngoài miền" tương đương cho contract tóm tắt.
        payload["summary_vi"] = [bullets[0] * 20, *bullets[1:]]
    elif forced_reason == "schema_violation":
        del payload["why_it_matters_vi"]

    return _maybe_truncate(json.dumps(payload, ensure_ascii=False), forced_reason)


def _maybe_truncate(serialized: str, forced_reason: FailureReason | None) -> str:
    """JSON thực sự hỏng (cắt cụt) khi forced_reason=json_parse_error — đi qua parser thật."""
    if forced_reason == "json_parse_error":
        return serialized[: len(serialized) // 2]
    return serialized


def _pick_tags(digest: bytes) -> list[str]:
    """Chọn 1-2 tag xác định từ tập đóng, dựa trên digest."""
    first_index = digest[14] % len(_SORTED_INDUSTRY_TAGS)
    tags = [_SORTED_INDUSTRY_TAGS[first_index]]
    if digest[15] % 2 == 0:
        second_index = digest[16] % len(_SORTED_INDUSTRY_TAGS)
        second_tag = _SORTED_INDUSTRY_TAGS[second_index]
        if second_tag != tags[0]:
            tags.append(second_tag)
    return tags


def _estimate_prompt_tokens(prompt: str) -> int:
    """Ước lượng token thô (~4 ký tự/token) — chỉ để giả lập, không phải bộ đếm token thật."""
    return max(1, len(prompt) // 4)


def _simulated_output_tokens(article_id: object) -> int:
    """Số output token giả lập, xác định theo article_id — dùng để test đường tính chi phí."""
    digest = _digest(article_id)
    return 30 + (digest[17] % 40)
