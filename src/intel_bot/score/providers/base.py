"""Interface Provider LLM — LLM đối xử như API bên thứ ba không đáng tin (PRODUCTION_PLAN §10.1-10.2).

KHÔNG có provider gọi mạng thật ở đây (task 0.8). Chỉ contract/interface + hàm dùng chung.
Đổi provider = đổi một dòng `models.yaml`, không sửa pipeline (§10.2).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Protocol

from pydantic import ValidationError

from src.intel_bot.contracts.llm_score import ScoreResult, SummaryResult


class ProviderUnavailableError(Exception):
    """Cả provider không dùng được (mất mạng hoàn toàn, key sai, hết quota, ...).

    KHÁC với lỗi từng bản ghi (ScoreFailure/SummaryFailure) — đây là ngoại lệ THẬT, được
    phép raise, vì không có gì để trả về cho batch (§10.5: "Asset fail, alert, KHÔNG mark
    hàng loạt quarantined" — bên gọi phải để nguyên các bài ở status hiện tại, không tự ý
    quarantine hàng loạt khi gặp lỗi này).
    """


#: failure_reason — khớp CHÍNH XÁC với CHECK constraint của silver.score_quarantine (§5.5).
FailureReason = Literal[
    "json_parse_error", "schema_violation", "out_of_range", "timeout"
]

_RANGE_ERROR_TYPES = frozenset(
    {"greater_than", "greater_than_equal", "less_than", "less_than_equal"}
)


def classify_validation_error(
    exc: ValidationError,
) -> Literal["schema_violation", "out_of_range"]:
    """Phân loại `pydantic.ValidationError` để chọn `failure_reason` (§10.5, §5.5).

    Điểm ngoài 1-10 vi phạm ràng buộc ge/le → "out_of_range". Mọi vi phạm khác (thiếu
    trường, sai kiểu, sai enum, ...) → "schema_violation". Dùng chung cho mọi provider
    thật (task 0.8) lẫn mock, để hai bên phân loại lỗi nhất quán.
    """
    error_types = {error["type"] for error in exc.errors()}
    if error_types & _RANGE_ERROR_TYPES:
        return "out_of_range"
    return "schema_violation"


@dataclass(frozen=True)
class ScoreRequest:
    """Một yêu cầu chấm điểm — đủ thông tin để provider dựng prompt và ước tính token."""

    article_id: uuid.UUID
    title: str
    snippet: str
    prompt: str
    prompt_version: str


@dataclass(frozen=True)
class ScoreSuccess:
    """Kết quả THÀNH CÔNG cho một bản ghi — `result` đã pass Pydantic validation."""

    article_id: uuid.UUID
    result: ScoreResult
    input_tokens: int
    output_tokens: int
    latency_ms: int
    model_name: str
    prompt_version: str
    #: Số input_tokens là cache hit (giá rẻ hơn cache miss ở provider có context caching,
    #: vd. DeepSeek). 0 với provider không báo cáo/không hỗ trợ caching (vd. mock) — cost.py
    #: coi phần còn lại của input_tokens là cache miss.
    input_cache_hit_tokens: int = 0


@dataclass(frozen=True)
class ScoreFailure:
    """Kết quả THẤT BẠI cho một bản ghi.

    KHÔNG phải exception — đây là giá trị trả về bình thường của `score_batch()` (§10.1).
    Provider chỉ raise exception khi cả provider không dùng được (vd. mất mạng hoàn toàn),
    không raise cho lỗi ở mức từng bản ghi.
    """

    article_id: uuid.UUID
    raw_response: str
    failure_reason: FailureReason
    model_name: str
    prompt_version: str
    #: Số lần thử thực tế trước khi kết luận thất bại (§10.5: parse/schema retry 1 lần =
    #: attempt_no tối đa 2; out_of_range không retry = 1; timeout retry 2 lần = tối đa 3).
    #: Ghi thẳng vào silver.score_quarantine.attempt_no.
    attempt_no: int = 1


ScoreOutcome = ScoreSuccess | ScoreFailure


@dataclass(frozen=True)
class SummarySuccess:
    """Kết quả tóm tắt THÀNH CÔNG cho một bản ghi — `result` đã pass Pydantic validation."""

    article_id: uuid.UUID
    result: SummaryResult
    input_tokens: int
    output_tokens: int
    latency_ms: int
    model_name: str
    prompt_version: str
    input_cache_hit_tokens: int = 0


@dataclass(frozen=True)
class SummaryFailure:
    """Kết quả tóm tắt THẤT BẠI cho một bản ghi — giá trị trả về, không phải exception."""

    article_id: uuid.UUID
    raw_response: str
    failure_reason: FailureReason
    model_name: str
    prompt_version: str
    attempt_no: int = 1


SummaryOutcome = SummarySuccess | SummaryFailure


class LLMProvider(Protocol):
    """Interface provider LLM (PRODUCTION_PLAN §10.2) — kiến trúc không giả định có GPU."""

    def score_batch(self, items: list[ScoreRequest]) -> list[ScoreOutcome]:
        """Chấm một batch bài.

        Lỗi ở TỪNG bản ghi trả về dưới dạng `ScoreFailure` trong list kết quả — không
        raise. Chỉ raise khi cả provider không dùng được (mất mạng, hết quota, ...).
        """
        ...

    def summarize_batch(self, items: list[ScoreRequest]) -> list[SummaryOutcome]:
        """Tóm tắt một batch bài (chỉ gọi cho top-K, §5.4/§21.2) — cùng nguyên tắc lỗi.

        `items[i].prompt` phải là prompt TÓM TẮT (dựng bằng `build_summary_prompt()`),
        không phải prompt chấm điểm — `ScoreRequest` dùng chung hình dạng cho cả hai loại
        yêu cầu LLM, không tách class riêng vì cấu trúc giống hệt nhau.
        """
        ...

    def estimate_cost(self, items: list[ScoreRequest]) -> Decimal:
        """Ước tính chi phí USD cho batch — dùng cho `cost_sensor` (§21.2), không gọi mạng."""
        ...
