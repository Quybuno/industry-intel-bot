"""Composite score TẠM THỜI — CHỈ để chọn top-K bài tóm tắt ở task 0.8, KHÔNG phải công
thức xếp hạng chính thức.

Công thức xếp hạng CHÍNH THỨC thuộc `gold.fct_article_score`, làm bằng dbt ở task 0.10
(PRODUCTION_PLAN §5.7, §11.1 — P5: SQL/dbt lo business logic, Python không nhúng logic
xếp hạng). Bản này tồn tại vì task 0.8 cần chọn top-K NGAY BÂY GIỜ để sinh tóm tắt, trước
khi dbt/gold tồn tại — nó sẽ bị dbt thay thế hoàn toàn, không phải giữ song song.

Khác biệt đã biết so với công thức chính thức (§5.7, "Sửa lỗi 4"): `credibility` chính
thức là 80% source tier (từ `gold.dim_source`, SCD Type 2) + 20% điểm LLM. `dim_source`
chưa tồn tại (chưa có dbt) nên bản này dùng credibility THÔ từ LLM — xấp xỉ, không chính
xác. Đủ tốt để chọn top-K tương đối, không dùng cho mục đích khác.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

# Trọng số theo §5.7 — depth = 0% ở Phase 0-1 (chưa có full-text để chấm sâu đáng tin).
_IMPORTANCE_WEIGHT = 0.4
_PRACTICALITY_WEIGHT = 0.3
_CREDIBILITY_WEIGHT = 0.3

# Recency: published_at trong 12h -> +1.0; 24h -> +0.5; còn lại 0 (§5.7).
_RECENCY_FULL_BOOST_HOURS = 12
_RECENCY_HALF_BOOST_HOURS = 24
_RECENCY_FULL_BOOST = 1.0
_RECENCY_HALF_BOOST = 0.5
_RECENCY_NO_BOOST = 0.0
# published_at NULL (dùng first_seen_at thay) -> trừ 0.3 vì độ tin cậy thấp hơn (§5.7).
_IMPUTED_DATE_PENALTY = 0.3


@dataclass(frozen=True)
class ScoredArticleForRanking:
    """Dữ liệu tối thiểu cần để tính composite tạm — không phải ORM/DB row."""

    credibility: int
    importance: int
    practicality: int
    published_at: datetime | None
    first_seen_at: datetime
    published_at_imputed: bool


def compute_recency_boost(
    *, effective_published_at: datetime, now: datetime, published_at_imputed: bool
) -> float:
    """Recency boost theo §5.7 — `now` nhận qua tham số để hàm thuần, test được."""
    age = now - effective_published_at
    if age <= timedelta(hours=_RECENCY_FULL_BOOST_HOURS):
        boost = _RECENCY_FULL_BOOST
    elif age <= timedelta(hours=_RECENCY_HALF_BOOST_HOURS):
        boost = _RECENCY_HALF_BOOST
    else:
        boost = _RECENCY_NO_BOOST

    if published_at_imputed:
        boost -= _IMPUTED_DATE_PENALTY
    return boost


def compute_composite_score(
    article: ScoredArticleForRanking, *, now: datetime
) -> float:
    """Composite tạm để chọn top-K tóm tắt — xem docstring module về giới hạn của hàm này."""
    effective_published_at = article.published_at or article.first_seen_at
    recency = compute_recency_boost(
        effective_published_at=effective_published_at,
        now=now,
        published_at_imputed=article.published_at_imputed,
    )
    return (
        article.importance * _IMPORTANCE_WEIGHT
        + article.practicality * _PRACTICALITY_WEIGHT
        + article.credibility * _CREDIBILITY_WEIGHT
        + recency
    )
