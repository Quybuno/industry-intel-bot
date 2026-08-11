"""Data contract cho output LLM — Pydantic v2 (PRODUCTION_PLAN §10.4).

LLM được đối xử như một API bên thứ ba không đáng tin (§10.1): output PHẢI qua contract
này trước khi được dùng ở bất kỳ đâu khác trong pipeline.

Quy tắc validation (§10.5 — chỉ 2 ngoại lệ khác nhau, không được nhầm lẫn):
- Điểm ngoài miền 1-10, thiếu trường bắt buộc, sai kiểu → LỖI VALIDATION (`pydantic.ValidationError`).
  TUYỆT ĐỐI KHÔNG clamp giá trị — clamp là che giấu lỗi (P4).
- Tag lạ ngoài tập đóng industry_tags → NGOẠI LỆ DUY NHẤT: loại bỏ tag đó, ghi warning,
  bản ghi vẫn hợp lệ (không raise).
"""

from __future__ import annotations

import logging
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints, field_validator

from src.intel_bot.observability.logging import log_event

logger = logging.getLogger(__name__)

#: Tập đóng industry_tags (PRODUCTION_PLAN §10.4). Đây là hằng số NGHIỆP VỤ của contract,
#: không phải cấu hình vận hành (model/ngưỡng/URL) — khác nhóm với các giá trị mà AGENTS.md
#: cấm hardcode; tập tag đóng là một phần định nghĩa contract, đổi tập tag = đổi contract.
INDUSTRY_TAGS: frozenset[str] = frozenset(
    {"ai", "construction", "hvac", "manufacturing", "iot"}
)

_SummaryBullet = Annotated[str, StringConstraints(min_length=15, max_length=200)]


class ScoreResult(BaseModel):
    """Kết quả chấm điểm 4 tiêu chí + phân loại — contract cho `article_scores` (§5.4, §10.4)."""

    credibility: int = Field(ge=1, le=10)
    importance: int = Field(ge=1, le=10)
    depth: int = Field(ge=1, le=10)
    practicality: int = Field(ge=1, le=10)
    industry_tags: list[str]
    confidence: Literal["high", "medium", "low"]
    is_breaking: bool = False

    @field_validator("industry_tags")
    @classmethod
    def _drop_unknown_tags(cls, value: list[str]) -> list[str]:
        """Ngoại lệ duy nhất của contract: tag lạ bị loại, KHÔNG làm bản ghi thất bại (§10.5)."""
        kept = [tag for tag in value if tag in INDUSTRY_TAGS]
        dropped = [tag for tag in value if tag not in INDUSTRY_TAGS]
        if dropped:
            log_event(
                logger, "industry_tag_dropped", dropped_tags=dropped, kept_tags=kept
            )
        return kept


class SummaryResult(BaseModel):
    """Tóm tắt 5 bullet tiếng Việt — contract cho `article_summaries` (§5.4, §10.4)."""

    summary_vi: list[_SummaryBullet] = Field(min_length=5, max_length=5)
    why_it_matters_vi: str = Field(min_length=20, max_length=300)
