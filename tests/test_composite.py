"""Unit test cho composite.py — hàm thuần, không DB/mạng. Xem docstring module về việc
đây là công thức TẠM THỜI, sẽ bị dbt thay thế ở task 0.10."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.intel_bot.score.composite import (
    ScoredArticleForRanking,
    compute_composite_score,
    compute_recency_boost,
)

NOW = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def _article(
    *,
    credibility: int = 5,
    importance: int = 5,
    practicality: int = 5,
    published_at: datetime | None = NOW,
    first_seen_at: datetime = NOW,
    published_at_imputed: bool = False,
) -> ScoredArticleForRanking:
    return ScoredArticleForRanking(
        credibility=credibility,
        importance=importance,
        practicality=practicality,
        published_at=published_at,
        first_seen_at=first_seen_at,
        published_at_imputed=published_at_imputed,
    )


# ---------------------------------------------------------------------------
# compute_recency_boost — §5.7: 12h -> +1.0, 24h -> +0.5, còn lại 0; NULL -> -0.3
# ---------------------------------------------------------------------------


def test_recency_boost_within_12h_is_full() -> None:
    boost = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=5),
        now=NOW,
        published_at_imputed=False,
    )
    assert boost == 1.0


def test_recency_boost_exactly_12h_is_still_full() -> None:
    boost = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=12),
        now=NOW,
        published_at_imputed=False,
    )
    assert boost == 1.0


def test_recency_boost_between_12h_and_24h_is_half() -> None:
    boost = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=18),
        now=NOW,
        published_at_imputed=False,
    )
    assert boost == 0.5


def test_recency_boost_exactly_24h_is_still_half() -> None:
    boost = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=24),
        now=NOW,
        published_at_imputed=False,
    )
    assert boost == 0.5


def test_recency_boost_beyond_24h_is_zero() -> None:
    boost = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=25),
        now=NOW,
        published_at_imputed=False,
    )
    assert boost == 0.0


def test_recency_boost_imputed_penalty_applied() -> None:
    """published_at NULL (dùng first_seen_at) -> trừ 0.3 dù trong khoảng boost nào."""
    boost_recent = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=1),
        now=NOW,
        published_at_imputed=True,
    )
    assert boost_recent == 1.0 - 0.3

    boost_old = compute_recency_boost(
        effective_published_at=NOW - timedelta(hours=48),
        now=NOW,
        published_at_imputed=True,
    )
    assert boost_old == 0.0 - 0.3


# ---------------------------------------------------------------------------
# compute_composite_score — §5.7: importance*0.4 + practicality*0.3 + credibility*0.3 + recency
# ---------------------------------------------------------------------------


def test_composite_score_formula_exact_value() -> None:
    article = _article(credibility=10, importance=10, practicality=10, published_at=NOW)
    score = compute_composite_score(article, now=NOW)
    # 10*0.4 + 10*0.3 + 10*0.3 + 1.0 (trong 12h) = 4 + 3 + 3 + 1 = 11.0
    assert score == 11.0


def test_composite_score_ignores_depth_field_not_present() -> None:
    """ScoredArticleForRanking không có field depth — xác nhận công thức không dùng depth (§5.7)."""
    assert not hasattr(ScoredArticleForRanking, "depth")


def test_composite_score_higher_importance_gives_higher_score() -> None:
    low = _article(importance=1)
    high = _article(importance=10)
    assert compute_composite_score(high, now=NOW) > compute_composite_score(
        low, now=NOW
    )


def test_composite_score_null_published_at_uses_first_seen_at() -> None:
    first_seen = NOW - timedelta(hours=2)
    article = _article(
        published_at=None, first_seen_at=first_seen, published_at_imputed=True
    )
    score = compute_composite_score(article, now=NOW)
    # credibility=importance=practicality=5 => 5*0.4+5*0.3+5*0.3 = 5.0; recency: first_seen
    # cách NOW 2h -> boost 1.0, trừ 0.3 (imputed) => 0.7. Tổng = 5.7.
    assert score == 5.7


def test_composite_score_recent_beats_old_when_scores_equal() -> None:
    recent = _article(published_at=NOW - timedelta(hours=1))
    old = _article(published_at=NOW - timedelta(hours=48))
    assert compute_composite_score(recent, now=NOW) > compute_composite_score(
        old, now=NOW
    )
