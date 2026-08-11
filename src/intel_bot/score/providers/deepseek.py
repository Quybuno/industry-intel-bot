"""Provider DeepSeek thật — gọi API đồng bộ (KHÔNG có Batch API).

Đã xác minh trực tiếp qua api-docs.deepseek.com ngày 2026-08-11: không có trang/endpoint
batch nào được tài liệu hoá. Vì vậy provider này gọi `/chat/completions` đồng bộ cho từng
bài, khác với luồng "submit → job id → poll" mà task 0.8 mô tả cho Gemini — quyết định đã
được xác nhận với người dùng khi đổi từ Gemini sang DeepSeek.

Tên model, endpoint, giá đọc từ `config/models.yaml`; API key đọc từ biến môi trường
`DEEPSEEK_API_KEY` — KHÔNG hardcode ở đây (AGENTS.md mục 4).

Xử lý lỗi theo ĐÚNG bảng §10.5:
- JSON không parse được → retry 1 lần với chỉ dẫn chặt hơn, vẫn hỏng → quarantine.
- Vi phạm schema → retry 1 lần, vẫn hỏng → quarantine.
- Điểm ngoài 1-10 → quarantine NGAY, không retry, không clamp.
- Timeout / 429 / 5xx → retry 2 lần, backoff luỹ thừa, vẫn hỏng → quarantine (timeout).
- Lỗi xác thực / không kết nối được → raise `ProviderUnavailableError` (không quarantine
  hàng loạt — để runner.py xử lý ở mức batch).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

import httpx
import yaml
from pydantic import BaseModel, ValidationError

from src.intel_bot.contracts.llm_score import ScoreResult, SummaryResult
from src.intel_bot.score.cost import (
    DEFAULT_MODELS_PATH,
    ModelPricing,
    compute_cost_usd,
    load_model_pricing,
)
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

PROVIDER_NAME = "deepseek"

# Số lần retry CHÍNH XÁC theo bảng lỗi §10.5 — hằng số kiến trúc đã "chốt" trong plan,
# khác với giá/tên model nên không đọc từ config.
_JSON_PARSE_MAX_RETRIES = 1
_SCHEMA_VIOLATION_MAX_RETRIES = 1
_TIMEOUT_MAX_RETRIES = 2
_TIMEOUT_BACKOFF_BASE_SECONDS = 1.0

_STRICTER_INSTRUCTION = (
    "\n\n[QUAN TRỌNG] Lần trả lời trước KHÔNG đúng định dạng yêu cầu. Lần này PHẢI trả về"
    " CHÍNH XÁC một object JSON thuần theo đúng schema đã nêu — không thêm chữ nào khác,"
    " không bọc trong markdown, không thiếu trường nào."
)

_T = TypeVar("_T", bound=BaseModel)


class _RetryableTransportError(Exception):
    """Timeout/429/5xx — lỗi tạm thời, retry theo backoff. Nội bộ module, không lộ ra ngoài."""


@dataclass(frozen=True)
class _CallOutcome:
    """Kết quả nội bộ của một lần gọi (có thể đã retry) — trước khi gói thành Score/Summary*."""

    parsed: BaseModel | None
    raw_response_text: str
    input_tokens: int
    cache_hit_tokens: int
    output_tokens: int
    latency_ms: int
    attempt_no: int
    failure_reason: FailureReason | None

    @property
    def success(self) -> bool:
        return self.parsed is not None


def _load_provider_config(
    models_path: Path | str = DEFAULT_MODELS_PATH,
) -> dict[str, Any]:
    """Đọc mục provider 'deepseek' trong config/models.yaml. Lỗi rõ ràng nếu thiếu."""
    path = Path(models_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    providers: list[dict[str, Any]] = raw.get("providers", [])
    provider = next((p for p in providers if p.get("name") == PROVIDER_NAME), None)
    if provider is None:
        raise ValueError(
            f"Không tìm thấy provider '{PROVIDER_NAME}' trong {path}. "
            "Điền mục deepseek (model, giá, api_base_url) vào config/models.yaml trước."
        )
    return provider


def _model_id_for_tier(provider_config: dict[str, Any], *, tier: str) -> str:
    """Chọn model theo tier (vd. 'fast') — KHÔNG hardcode tên model."""
    models: list[dict[str, Any]] = provider_config.get("models", [])
    model = next((m for m in models if m.get("tier") == tier), None)
    if model is None:
        raise ValueError(
            f"Không tìm thấy model tier='{tier}' của provider '{PROVIDER_NAME}' trong models.yaml"
        )
    model_id = model.get("id")
    if not model_id:
        raise ValueError(
            f"Model tier='{tier}' của provider '{PROVIDER_NAME}' thiếu 'id'"
        )
    return str(model_id)


@dataclass
class DeepSeekProvider:
    """Provider DeepSeek thật — cài đặt `LLMProvider` (đồng bộ, không Batch API)."""

    api_key: str
    model_id: str
    api_base_url: str
    pricing: ModelPricing
    timeout_seconds: float = 30.0
    _client: httpx.Client | None = None

    @classmethod
    def from_config(
        cls,
        *,
        api_key: str,
        tier: str = "fast",
        models_path: Path | str = DEFAULT_MODELS_PATH,
    ) -> DeepSeekProvider:
        """Dựng provider từ config/models.yaml — tên model/giá/endpoint KHÔNG hardcode."""
        provider_config = _load_provider_config(models_path)
        model_id = _model_id_for_tier(provider_config, tier=tier)
        api_base_url = provider_config.get("api_base_url")
        if not api_base_url:
            raise ValueError(
                f"Provider '{PROVIDER_NAME}' thiếu 'api_base_url' trong models.yaml"
            )
        pricing = load_model_pricing(PROVIDER_NAME, model_id, models_path=models_path)
        return cls(
            api_key=api_key,
            model_id=model_id,
            api_base_url=str(api_base_url).rstrip("/"),
            pricing=pricing,
        )

    def score_batch(self, items: list[ScoreRequest]) -> list[ScoreOutcome]:
        """Chấm một batch — gọi API thật tuần tự, không raise cho lỗi từng bản ghi."""
        return [self._score_one(item) for item in items]

    def summarize_batch(self, items: list[ScoreRequest]) -> list[SummaryOutcome]:
        """Tóm tắt một batch — gọi API thật tuần tự, không raise cho lỗi từng bản ghi."""
        return [self._summarize_one(item) for item in items]

    def estimate_cost(self, items: list[ScoreRequest]) -> Decimal:
        """Ước tính chi phí TRƯỚC khi gọi — giả định toàn bộ input là cache-miss (an toàn,
        không đánh giá thấp), output ước lượng 300 token (điển hình cho JSON kết quả)."""
        total = Decimal(0)
        for item in items:
            estimated_input_tokens = max(1, len(item.prompt) // 4)
            total += compute_cost_usd(
                pricing=self.pricing,
                input_cache_hit_tokens=0,
                input_cache_miss_tokens=estimated_input_tokens,
                output_tokens=300,
            )
        return total

    def _score_one(self, item: ScoreRequest) -> ScoreOutcome:
        call = self._call_and_validate(item, model_cls=ScoreResult)
        if call.success:
            assert isinstance(call.parsed, ScoreResult)
            return ScoreSuccess(
                article_id=item.article_id,
                result=call.parsed,
                input_tokens=call.input_tokens,
                output_tokens=call.output_tokens,
                latency_ms=call.latency_ms,
                model_name=self.model_id,
                prompt_version=item.prompt_version,
                input_cache_hit_tokens=call.cache_hit_tokens,
            )
        assert call.failure_reason is not None
        return ScoreFailure(
            article_id=item.article_id,
            raw_response=call.raw_response_text,
            failure_reason=call.failure_reason,
            model_name=self.model_id,
            prompt_version=item.prompt_version,
            attempt_no=call.attempt_no,
        )

    def _summarize_one(self, item: ScoreRequest) -> SummaryOutcome:
        call = self._call_and_validate(item, model_cls=SummaryResult)
        if call.success:
            assert isinstance(call.parsed, SummaryResult)
            return SummarySuccess(
                article_id=item.article_id,
                result=call.parsed,
                input_tokens=call.input_tokens,
                output_tokens=call.output_tokens,
                latency_ms=call.latency_ms,
                model_name=self.model_id,
                prompt_version=item.prompt_version,
                input_cache_hit_tokens=call.cache_hit_tokens,
            )
        assert call.failure_reason is not None
        return SummaryFailure(
            article_id=item.article_id,
            raw_response=call.raw_response_text,
            failure_reason=call.failure_reason,
            model_name=self.model_id,
            prompt_version=item.prompt_version,
            attempt_no=call.attempt_no,
        )

    def _call_and_validate(
        self, item: ScoreRequest, *, model_cls: type[_T]
    ) -> _CallOutcome:
        """Gọi API + validate, retry theo ĐÚNG bảng §10.5. Không raise cho lỗi từng bản ghi."""
        prompt_text = item.prompt
        attempt = 0
        timeout_attempts = 0
        content_attempts = 0

        while True:
            attempt += 1
            try:
                response_json, latency_ms = self._call_api(prompt_text)
            except _RetryableTransportError:
                timeout_attempts += 1
                if timeout_attempts > _TIMEOUT_MAX_RETRIES:
                    return _CallOutcome(
                        parsed=None,
                        raw_response_text="",
                        input_tokens=0,
                        cache_hit_tokens=0,
                        output_tokens=0,
                        latency_ms=0,
                        attempt_no=attempt,
                        failure_reason="timeout",
                    )
                time.sleep(
                    _TIMEOUT_BACKOFF_BASE_SECONDS * (2 ** (timeout_attempts - 1))
                )
                continue

            usage = response_json.get("usage", {})
            input_tokens = int(usage.get("prompt_tokens", 0))
            cache_hit_tokens = int(usage.get("prompt_cache_hit_tokens", 0))
            output_tokens = int(usage.get("completion_tokens", 0))
            raw_text = str(response_json["choices"][0]["message"]["content"])

            try:
                payload = json.loads(raw_text)
            except json.JSONDecodeError:
                content_attempts += 1
                if content_attempts > _JSON_PARSE_MAX_RETRIES:
                    return _CallOutcome(
                        parsed=None,
                        raw_response_text=raw_text,
                        input_tokens=input_tokens,
                        cache_hit_tokens=cache_hit_tokens,
                        output_tokens=output_tokens,
                        latency_ms=latency_ms,
                        attempt_no=attempt,
                        failure_reason="json_parse_error",
                    )
                prompt_text = item.prompt + _STRICTER_INSTRUCTION
                continue

            try:
                parsed = model_cls.model_validate(payload)
            except ValidationError as exc:
                reason = classify_validation_error(exc)
                if reason == "out_of_range":
                    # Quarantine NGAY, KHÔNG retry (§10.5) — không clamp.
                    return _CallOutcome(
                        parsed=None,
                        raw_response_text=raw_text,
                        input_tokens=input_tokens,
                        cache_hit_tokens=cache_hit_tokens,
                        output_tokens=output_tokens,
                        latency_ms=latency_ms,
                        attempt_no=attempt,
                        failure_reason="out_of_range",
                    )
                content_attempts += 1
                if content_attempts > _SCHEMA_VIOLATION_MAX_RETRIES:
                    return _CallOutcome(
                        parsed=None,
                        raw_response_text=raw_text,
                        input_tokens=input_tokens,
                        cache_hit_tokens=cache_hit_tokens,
                        output_tokens=output_tokens,
                        latency_ms=latency_ms,
                        attempt_no=attempt,
                        failure_reason="schema_violation",
                    )
                prompt_text = item.prompt + _STRICTER_INSTRUCTION
                continue

            return _CallOutcome(
                parsed=parsed,
                raw_response_text=raw_text,
                input_tokens=input_tokens,
                cache_hit_tokens=cache_hit_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
                attempt_no=attempt,
                failure_reason=None,
            )

    def _call_api(self, prompt_text: str) -> tuple[dict[str, Any], int]:
        """Một lần gọi HTTP thật tới DeepSeek. Raise ProviderUnavailableError nếu cả
        provider không dùng được; raise _RetryableTransportError nếu lỗi tạm thời."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": self.model_id,
            "messages": [{"role": "user", "content": prompt_text}],
            "response_format": {"type": "json_object"},
            "temperature": 0,
        }
        client = self._client or httpx
        start = time.monotonic()
        try:
            response = client.post(
                f"{self.api_base_url}/chat/completions",
                headers=headers,
                json=body,
                timeout=self.timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise _RetryableTransportError(str(exc)) from exc
        except httpx.ConnectError as exc:
            raise ProviderUnavailableError(
                f"DeepSeek: không kết nối được — {exc}"
            ) from exc

        latency_ms = int((time.monotonic() - start) * 1000)

        if response.status_code in (401, 403):
            raise ProviderUnavailableError(
                f"DeepSeek: lỗi xác thực (status={response.status_code}) —"
                " kiểm tra DEEPSEEK_API_KEY"
            )
        if response.status_code == 429 or response.status_code >= 500:
            raise _RetryableTransportError(f"status={response.status_code}")
        if response.status_code >= 400:
            raise ProviderUnavailableError(
                f"DeepSeek: lỗi request không mong đợi (status={response.status_code}):"
                f" {response.text[:500]}"
            )

        return dict(response.json()), latency_ms


def resolve_pricing(
    *, tier: str = "fast", models_path: Path | str = DEFAULT_MODELS_PATH
) -> ModelPricing:
    """Bảng giá của model DeepSeek theo tier — tiện cho runner.py/CLI dùng riêng khi cần
    tính chi phí mà không cần khởi tạo cả provider (vd. không có API key khi chỉ ước tính)."""
    provider_config = _load_provider_config(models_path)
    model_id = _model_id_for_tier(provider_config, tier=tier)
    return load_model_pricing(PROVIDER_NAME, model_id, models_path=models_path)


def max_per_run(models_path: Path | str = DEFAULT_MODELS_PATH) -> int | None:
    """`max_per_run` khai báo cho provider deepseek trong models.yaml, None nếu không có."""
    provider_config = _load_provider_config(models_path)
    value = provider_config.get("max_per_run")
    return int(value) if value is not None else None


__all__ = [
    "PROVIDER_NAME",
    "DeepSeekProvider",
    "resolve_pricing",
]
