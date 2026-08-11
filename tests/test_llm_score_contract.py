"""Unit test cho contracts/llm_score.py — Pydantic v2, không DB/mạng, không cần env var."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.intel_bot.contracts.llm_score import ScoreResult, SummaryResult

VALID_SCORE_KWARGS: dict[str, object] = {
    "credibility": 7,
    "importance": 6,
    "depth": 5,
    "practicality": 8,
    "industry_tags": ["ai", "iot"],
    "confidence": "high",
}


def _score(**overrides: object) -> ScoreResult:
    kwargs = {**VALID_SCORE_KWARGS, **overrides}
    return ScoreResult(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ScoreResult — trường hợp hợp lệ
# ---------------------------------------------------------------------------


def test_score_result_valid_construction() -> None:
    result = _score()
    assert result.credibility == 7
    assert result.is_breaking is False


def test_score_result_is_breaking_defaults_false_when_omitted() -> None:
    result = _score()
    assert result.is_breaking is False


def test_score_result_is_breaking_explicit_true() -> None:
    result = _score(is_breaking=True)
    assert result.is_breaking is True


# ---------------------------------------------------------------------------
# ScoreResult — điểm ngoài miền 1-10 PHẢI bị từ chối, KHÔNG được clamp (P4, §10.5)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field_name", ["credibility", "importance", "depth", "practicality"]
)
@pytest.mark.parametrize("bad_value", [0, 11, -1, 100])
def test_score_result_out_of_range_rejected_not_clamped(
    field_name: str, bad_value: int
) -> None:
    with pytest.raises(ValidationError):
        _score(**{field_name: bad_value})


@pytest.mark.parametrize(
    "field_name", ["credibility", "importance", "depth", "practicality"]
)
@pytest.mark.parametrize("boundary_value", [1, 10])
def test_score_result_boundary_values_accepted(
    field_name: str, boundary_value: int
) -> None:
    result = _score(**{field_name: boundary_value})
    assert getattr(result, field_name) == boundary_value


def test_score_result_zero_and_eleven_both_rejected_the_same_way() -> None:
    """Test bắt buộc của task 0.7: điểm = 0 và điểm = 11 → cùng bị từ chối, không bị clamp."""
    with pytest.raises(ValidationError) as exc_zero:
        _score(importance=0)
    with pytest.raises(ValidationError) as exc_eleven:
        _score(importance=11)
    # Cả hai đều là lỗi ràng buộc khoảng giá trị (ge/le), không phải lỗi khác loại.
    assert "importance" in str(exc_zero.value)
    assert "importance" in str(exc_eleven.value)


# ---------------------------------------------------------------------------
# ScoreResult — thiếu trường bắt buộc → lỗi validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "missing_field",
    [
        "credibility",
        "importance",
        "depth",
        "practicality",
        "industry_tags",
        "confidence",
    ],
)
def test_score_result_missing_required_field_raises(missing_field: str) -> None:
    kwargs = {k: v for k, v in VALID_SCORE_KWARGS.items() if k != missing_field}
    with pytest.raises(ValidationError):
        ScoreResult(**kwargs)  # type: ignore[arg-type]


def test_score_result_invalid_confidence_literal_raises() -> None:
    with pytest.raises(ValidationError):
        _score(confidence="very_high")


# ---------------------------------------------------------------------------
# ScoreResult — tag lạ: NGOẠI LỆ DUY NHẤT, loại bỏ nhưng giữ bản ghi (§10.5)
# ---------------------------------------------------------------------------


def test_score_result_unknown_tag_dropped_but_record_kept() -> None:
    result = _score(industry_tags=["ai", "bogus_industry"])
    assert result.industry_tags == ["ai"]


def test_score_result_all_unknown_tags_leaves_empty_list_not_error() -> None:
    result = _score(industry_tags=["totally_made_up"])
    assert result.industry_tags == []


def test_score_result_all_five_closed_tags_accepted_unchanged() -> None:
    tags = ["ai", "construction", "hvac", "manufacturing", "iot"]
    result = _score(industry_tags=tags)
    assert result.industry_tags == tags


# ---------------------------------------------------------------------------
# SummaryResult
# ---------------------------------------------------------------------------

VALID_BULLET = "x" * 50
VALID_SUMMARY_KWARGS: dict[str, object] = {
    "summary_vi": [VALID_BULLET] * 5,
    "why_it_matters_vi": "y" * 50,
}


def _summary(**overrides: object) -> SummaryResult:
    kwargs = {**VALID_SUMMARY_KWARGS, **overrides}
    return SummaryResult(**kwargs)  # type: ignore[arg-type]


def test_summary_result_valid_construction() -> None:
    result = _summary()
    assert len(result.summary_vi) == 5


def test_summary_result_four_bullets_rejected() -> None:
    """Test bắt buộc của task 0.7: summary có 4 phần tử → bị từ chối."""
    with pytest.raises(ValidationError):
        _summary(summary_vi=[VALID_BULLET] * 4)


def test_summary_result_six_bullets_rejected() -> None:
    """Test bắt buộc của task 0.7: summary có 6 phần tử → bị từ chối."""
    with pytest.raises(ValidationError):
        _summary(summary_vi=[VALID_BULLET] * 6)


def test_summary_result_bullet_too_short_rejected() -> None:
    bullets = [VALID_BULLET] * 4 + ["x" * 14]  # 14 < 15 ký tự tối thiểu
    with pytest.raises(ValidationError):
        _summary(summary_vi=bullets)


def test_summary_result_bullet_too_long_rejected() -> None:
    bullets = [VALID_BULLET] * 4 + ["x" * 201]  # 201 > 200 ký tự tối đa
    with pytest.raises(ValidationError):
        _summary(summary_vi=bullets)


def test_summary_result_bullet_boundary_15_chars_accepted() -> None:
    bullets = [VALID_BULLET] * 4 + ["x" * 15]
    result = _summary(summary_vi=bullets)
    assert len(result.summary_vi[-1]) == 15


def test_summary_result_bullet_boundary_200_chars_accepted() -> None:
    bullets = [VALID_BULLET] * 4 + ["x" * 200]
    result = _summary(summary_vi=bullets)
    assert len(result.summary_vi[-1]) == 200


def test_summary_result_why_it_matters_too_short_rejected() -> None:
    with pytest.raises(ValidationError):
        _summary(why_it_matters_vi="x" * 19)


def test_summary_result_why_it_matters_too_long_rejected() -> None:
    with pytest.raises(ValidationError):
        _summary(why_it_matters_vi="x" * 301)


def test_summary_result_why_it_matters_boundary_20_chars_accepted() -> None:
    result = _summary(why_it_matters_vi="x" * 20)
    assert len(result.why_it_matters_vi) == 20


def test_summary_result_why_it_matters_boundary_300_chars_accepted() -> None:
    result = _summary(why_it_matters_vi="x" * 300)
    assert len(result.why_it_matters_vi) == 300


def test_summary_result_missing_summary_vi_raises() -> None:
    with pytest.raises(ValidationError):
        SummaryResult(why_it_matters_vi="y" * 50)  # type: ignore[call-arg]


def test_summary_result_missing_why_it_matters_raises() -> None:
    with pytest.raises(ValidationError):
        SummaryResult(summary_vi=[VALID_BULLET] * 5)  # type: ignore[call-arg]
