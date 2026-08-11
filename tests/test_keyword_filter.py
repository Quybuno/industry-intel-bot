"""Unit test cho keyword_filter.py — hàm thuần, không DB/mạng."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from src.intel_bot.filter.keyword_filter import (
    ArticleRow,
    FilterRules,
    apply_daily_cap,
    evaluate,
)

NOW = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)

BASE_RULES = FilterRules(
    max_article_age_days=7,
    min_snippet_chars=80,
    blocklist_keywords=("webinar", "sponsored", "job posting", "press release"),
    max_articles_per_day=100,
    now=NOW,
)

LONG_SNIPPET = "x" * 80  # đúng ngưỡng min_snippet_chars mặc định trong BASE_RULES


def _article(
    *,
    title: str = "Some Title",
    snippet: str = LONG_SNIPPET,
    published_at: datetime | None = None,
) -> ArticleRow:
    return ArticleRow(
        article_id=uuid.uuid4(), title=title, snippet=snippet, published_at=published_at
    )


# ---------------------------------------------------------------------------
# Quy tắc 1: too_old (PRODUCTION_PLAN §9.2)
# ---------------------------------------------------------------------------


def test_too_old_pass_recent_article() -> None:
    article = _article(published_at=NOW - timedelta(hours=1))
    verdict = evaluate(article, BASE_RULES)
    assert verdict.passed is True
    assert verdict.filter_score == 1.0
    assert verdict.exclusion_reason is None


def test_too_old_fail_article_beyond_max_age() -> None:
    article = _article(published_at=NOW - timedelta(days=10))
    verdict = evaluate(article, BASE_RULES)
    assert verdict.passed is False
    assert verdict.filter_score == 0.0
    assert verdict.exclusion_reason == "too_old"


def test_too_old_boundary_exactly_at_cutoff_passes() -> None:
    """Đúng bằng ngưỡng (không CŨ HƠN) thì chưa bị loại — so sánh dùng '<' nghiêm ngặt."""
    article = _article(published_at=NOW - timedelta(days=7))
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason is None


def test_too_old_boundary_one_second_past_cutoff_fails() -> None:
    article = _article(published_at=NOW - timedelta(days=7, seconds=1))
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "too_old"


def test_too_old_null_published_at_does_not_trigger_rule() -> None:
    """published_at NULL (imputed ở task 0.5) không bị quy tắc too_old loại ở đây."""
    article = _article(published_at=None)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason != "too_old"


# ---------------------------------------------------------------------------
# Quy tắc 2: snippet_too_short
# ---------------------------------------------------------------------------


def test_snippet_pass_at_minimum_length() -> None:
    article = _article(snippet="x" * 80, published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.passed is True


def test_snippet_fail_shorter_than_minimum() -> None:
    article = _article(snippet="x" * 79, published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.passed is False
    assert verdict.exclusion_reason == "snippet_too_short"


def test_snippet_boundary_one_char_short_fails() -> None:
    article = _article(snippet="x" * 79, published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "snippet_too_short"


def test_snippet_empty_fails() -> None:
    article = _article(snippet="", published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "snippet_too_short"


def test_snippet_whitespace_only_counts_as_too_short() -> None:
    """Snippet toàn khoảng trắng (dù đủ độ dài thô) phải bị coi là quá ngắn sau strip()."""
    article = _article(snippet=" " * 80, published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "snippet_too_short"


# ---------------------------------------------------------------------------
# Quy tắc 3: keyword_blocked — không phân biệt hoa/thường, ranh giới từ
# ---------------------------------------------------------------------------


def test_blocklist_pass_no_match() -> None:
    article = _article(title="Great progress in AI research", published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.passed is True


def test_blocklist_fail_match_in_title() -> None:
    article = _article(title="Join our free webinar today", published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "keyword_blocked"


def test_blocklist_fail_match_in_snippet() -> None:
    article = _article(
        title="Update",
        snippet=("This article is sponsored. " + "x" * 80),
        published_at=NOW,
    )
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "keyword_blocked"


def test_blocklist_case_insensitive() -> None:
    article = _article(title="SPONSORED content ahead", published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "keyword_blocked"


def test_blocklist_multi_word_phrase_matches() -> None:
    article = _article(title="New press release from the company", published_at=NOW)
    verdict = evaluate(article, BASE_RULES)
    assert verdict.exclusion_reason == "keyword_blocked"


def test_blocklist_word_boundary_jobs_report_does_not_match_job_posting() -> None:
    """Test bắt buộc của task 0.6: 'jobs' trong 'jobs report' KHÔNG khớp 'job posting'."""
    rules = FilterRules(
        max_article_age_days=7,
        min_snippet_chars=10,
        blocklist_keywords=("job posting",),
        max_articles_per_day=100,
        now=NOW,
    )
    article = _article(title="Monthly jobs report shows growth", published_at=NOW)
    verdict = evaluate(article, rules)
    assert verdict.passed is True
    assert verdict.exclusion_reason is None


def test_blocklist_word_boundary_does_not_match_substring_inside_longer_word() -> None:
    """'ai' trong blocklist không được khớp 'contains' hay 'again' (chuỗi con)."""
    rules = FilterRules(
        max_article_age_days=7,
        min_snippet_chars=10,
        blocklist_keywords=("ai",),
        max_articles_per_day=100,
        now=NOW,
    )
    article = _article(
        title="Rain again in the area, contains updated forecast", published_at=NOW
    )
    verdict = evaluate(article, rules)
    assert verdict.passed is True


# ---------------------------------------------------------------------------
# Quy tắc 4: over_daily_cap — áp SAU CÙNG, giữ bài mới nhất theo published_at
# ---------------------------------------------------------------------------


def _passing_article(*, hours_ago: float) -> ArticleRow:
    return _article(published_at=NOW - timedelta(hours=hours_ago))


def test_daily_cap_pass_when_fewer_than_cap() -> None:
    articles = [_passing_article(hours_ago=h) for h in (1, 2, 3)]
    evaluated = [(a, evaluate(a, BASE_RULES)) for a in articles]
    result = apply_daily_cap(evaluated, max_articles_per_day=5)
    assert all(v.passed for v in result)


def test_daily_cap_boundary_exactly_at_cap_all_pass() -> None:
    articles = [_passing_article(hours_ago=h) for h in (1, 2, 3)]
    evaluated = [(a, evaluate(a, BASE_RULES)) for a in articles]
    result = apply_daily_cap(evaluated, max_articles_per_day=3)
    assert all(v.passed for v in result)


def test_daily_cap_fail_keeps_newest_excludes_oldest() -> None:
    """3 bài, cap=2 → giữ 2 bài mới nhất (hours_ago=1,2), loại bài cũ nhất (hours_ago=3)."""
    newest = _passing_article(hours_ago=1)
    middle = _passing_article(hours_ago=2)
    oldest = _passing_article(hours_ago=3)
    articles = [newest, middle, oldest]
    evaluated = [(a, evaluate(a, BASE_RULES)) for a in articles]

    result = apply_daily_cap(evaluated, max_articles_per_day=2)

    by_id = dict(zip((a.article_id for a in articles), result, strict=True))
    assert by_id[newest.article_id].passed is True
    assert by_id[middle.article_id].passed is True
    assert by_id[oldest.article_id].passed is False
    assert by_id[oldest.article_id].exclusion_reason == "over_daily_cap"


def test_daily_cap_does_not_touch_already_excluded_articles() -> None:
    """Bài đã bị loại bởi quy tắc khác không bị cap đụng vào / đổi lý do."""
    old_article = _article(published_at=NOW - timedelta(days=10))  # too_old
    recent_article = _passing_article(hours_ago=1)
    evaluated = [
        (old_article, evaluate(old_article, BASE_RULES)),
        (recent_article, evaluate(recent_article, BASE_RULES)),
    ]

    result = apply_daily_cap(evaluated, max_articles_per_day=0)

    by_id = dict(zip((a.article_id for a, _ in evaluated), result, strict=True))
    assert by_id[old_article.article_id].exclusion_reason == "too_old"
    assert by_id[recent_article.article_id].exclusion_reason == "over_daily_cap"


def test_daily_cap_null_published_at_treated_as_oldest() -> None:
    dated = _passing_article(hours_ago=1)
    undated = _article(published_at=None)
    evaluated = [
        (dated, evaluate(dated, BASE_RULES)),
        (undated, evaluate(undated, BASE_RULES)),
    ]

    result = apply_daily_cap(evaluated, max_articles_per_day=1)

    by_id = dict(zip((a.article_id for a, _ in evaluated), result, strict=True))
    assert by_id[dated.article_id].passed is True
    assert by_id[undated.article_id].passed is False
    assert by_id[undated.article_id].exclusion_reason == "over_daily_cap"


def test_daily_cap_is_stable_for_ties_same_input_order_same_output() -> None:
    """Cùng published_at (hoà) → thứ tự đầu vào quyết định ai bị loại, ổn định qua nhiều lần gọi."""
    a = _passing_article(hours_ago=1)
    b = _passing_article(hours_ago=1)
    evaluated = [(a, evaluate(a, BASE_RULES)), (b, evaluate(b, BASE_RULES))]

    result_1 = apply_daily_cap(evaluated, max_articles_per_day=1)
    result_2 = apply_daily_cap(evaluated, max_articles_per_day=1)

    assert result_1 == result_2
