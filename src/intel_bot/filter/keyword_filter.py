"""Filter tối thiểu Phase 0-1 (PRODUCTION_PLAN §9.2) — 4 quy tắc, KHÔNG embedding, KHÔNG LLM.

`evaluate()` là hàm THUẦN — không I/O. `filter_score` luôn là SỐ (1.0 pass / 0.0 loại),
không phải boolean, để cấu trúc không đổi khi Phase 2 thay bằng cosine similarity (§9.3).
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

#: Khi published_at NULL, bài không có ngày được coi là "cũ nhất" khi xếp hạng cho
#: quy tắc cap — không được ưu tiên giữ lại trước bài có published_at thật.
_EPOCH = datetime.min.replace(tzinfo=UTC)


@dataclass(frozen=True)
class ArticleRow:
    """Các trường của silver.articles cần để quyết định filter — không phải ORM."""

    article_id: uuid.UUID
    title: str
    snippet: str
    published_at: datetime | None


@dataclass(frozen=True)
class FilterRules:
    """Tham số filter — đọc từ config, không hardcode trong code (rào chắn task 0.6)."""

    max_article_age_days: int
    min_snippet_chars: int
    blocklist_keywords: tuple[str, ...]
    max_articles_per_day: int
    now: datetime


@dataclass(frozen=True)
class FilterVerdict:
    """Kết quả quyết định cho một bài: pass/loại, filter_score dạng số, lý do (nếu loại)."""

    passed: bool
    filter_score: float
    exclusion_reason: str | None


def evaluate(article: ArticleRow, rules: FilterRules) -> FilterVerdict:
    """Áp 3 quy tắc per-article, theo thứ tự: too_old → snippet_too_short → keyword_blocked.

    Quy tắc thứ 4 (trần số bài/ngày) áp dụng SAU CÙNG ở `apply_daily_cap()` — nó cần biết
    toàn bộ bài trong ngày để xếp hạng theo published_at, nên không thể là quyết định
    per-article thuần tuý (task 0.6 mục 3).
    """
    if article.published_at is not None:
        cutoff = rules.now - timedelta(days=rules.max_article_age_days)
        if article.published_at < cutoff:
            return FilterVerdict(
                passed=False, filter_score=0.0, exclusion_reason="too_old"
            )

    if len(article.snippet.strip()) < rules.min_snippet_chars:
        return FilterVerdict(
            passed=False, filter_score=0.0, exclusion_reason="snippet_too_short"
        )

    combined_text = f"{article.title} {article.snippet}"
    if _matches_blocklist(combined_text, rules.blocklist_keywords):
        return FilterVerdict(
            passed=False, filter_score=0.0, exclusion_reason="keyword_blocked"
        )

    return FilterVerdict(passed=True, filter_score=1.0, exclusion_reason=None)


def _matches_blocklist(text: str, blocklist_keywords: tuple[str, ...]) -> bool:
    """So khớp không phân biệt hoa/thường, theo ranh giới từ — không khớp chuỗi con.

    Vd: từ khoá "job posting" không khớp "jobs report" (\\b chặn "job" bên trong "jobs").
    """
    normalized_text = re.sub(r"\s+", " ", text.strip())
    for keyword in blocklist_keywords:
        pattern = r"\b" + re.escape(keyword.strip()) + r"\b"
        if re.search(pattern, normalized_text, flags=re.IGNORECASE):
            return True
    return False


def apply_daily_cap(
    evaluated: list[tuple[ArticleRow, FilterVerdict]], *, max_articles_per_day: int
) -> list[FilterVerdict]:
    """Quy tắc 4 — trần số bài/ngày, áp SAU CÙNG, giữ lại bài MỚI NHẤT theo published_at.

    Chỉ áp lên các bài đã pass 3 quy tắc trước (verdict.passed=True); bài đã bị loại giữ
    nguyên verdict. Trả về danh sách verdict cùng thứ tự với `evaluated` đầu vào — hàm thuần,
    không I/O; đầu vào cần có thứ tự ổn định (vd. ORDER BY article_id) để idempotent khi
    published_at trùng nhau giữa nhiều bài (Python `sorted` ổn định, giữ thứ tự gốc khi hoà).
    """
    passing_indices = [i for i, (_, verdict) in enumerate(evaluated) if verdict.passed]
    if len(passing_indices) <= max_articles_per_day:
        return [verdict for _, verdict in evaluated]

    def sort_key(index: int) -> datetime:
        published_at = evaluated[index][0].published_at
        return published_at if published_at is not None else _EPOCH

    ranked_newest_first = sorted(passing_indices, key=sort_key, reverse=True)
    kept = set(ranked_newest_first[:max_articles_per_day])

    result = [verdict for _, verdict in evaluated]
    for index in passing_indices:
        if index not in kept:
            result[index] = FilterVerdict(
                passed=False, filter_score=0.0, exclusion_reason="over_daily_cap"
            )
    return result
