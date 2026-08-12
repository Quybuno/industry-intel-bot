"""Unit test cho normalizer.py — toàn bộ hàm thuần, không DB/mạng."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from src.intel_bot.ingest.normalizer import (
    ARTICLE_ID_NAMESPACE,
    ParsedArticle,
    apply_cold_start_rules,
    canonicalize_url,
    compute_content_hash,
    extract_domain,
    make_article_id,
    parse_entry,
)

# ---------------------------------------------------------------------------
# canonicalize_url — bảng tham số (PRODUCTION_PLAN §8.3, task 0.5 mục 1)
# ---------------------------------------------------------------------------

CANONICALIZE_CASES: list[tuple[str, str, str]] = [
    (
        "www + trailing slash",
        "https://www.example.com/path/",
        "https://example.com/path",
    ),
    (
        "utm_source bị bỏ",
        "https://example.com/a?utm_source=newsletter",
        "https://example.com/a",
    ),
    (
        "nhiều biến thể utm_* bị bỏ",
        "https://example.com/a?utm_source=x&utm_medium=email&utm_campaign=aug&utm_id=1",
        "https://example.com/a",
    ),
    (
        "fbclid bị bỏ",
        "https://example.com/a?fbclid=abc123",
        "https://example.com/a",
    ),
    (
        "gclid bị bỏ",
        "https://example.com/a?gclid=xyz789",
        "https://example.com/a",
    ),
    (
        "ref bị bỏ",
        "https://example.com/a?ref=homepage",
        "https://example.com/a",
    ),
    (
        "source bị bỏ",
        "https://example.com/a?source=digest",
        "https://example.com/a",
    ),
    (
        "fragment bị bỏ",
        "https://example.com/a#section-2",
        "https://example.com/a",
    ),
    (
        "trailing slash ở domain root",
        "https://example.com/",
        "https://example.com",
    ),
    (
        "host viết hoa bị lowercase, path giữ nguyên case",
        "https://EXAMPLE.COM/News/Article",
        "https://example.com/News/Article",
    ),
    (
        "GitHub rút gọn về owner/repo, bỏ path/query/fragment thừa",
        "https://github.com/owner/repo/issues/5?tab=comments#c1",
        "https://github.com/owner/repo",
    ),
    (
        "GitHub www + hoa + .git bị rút gọn, owner/repo giữ nguyên case",
        "https://WWW.GitHub.com/Owner/Repo.git",
        "https://github.com/Owner/Repo",
    ),
    (
        "giữ nguyên query param không nằm trong danh sách bỏ",
        "https://example.com/a?utm_source=x&keep=1",
        "https://example.com/a?keep=1",
    ),
    (
        "kết hợp: hoa + www + tracking + fragment + trailing slash, giữ param khác",
        "https://WWW.Example.com/News/?utm_source=fb&fbclid=123&keep=yes&ref=home#top",
        "https://example.com/News?keep=yes",
    ),
    (
        "scheme http giữ nguyên (không ép thành https)",
        "http://example.com/a",
        "http://example.com/a",
    ),
    (
        "domain khác chứa chuỗi 'github.com' trong query không bị coi là GitHub",
        "https://example.com/redirect?url=github.com/foo",
        "https://example.com/redirect?url=github.com%2Ffoo",
    ),
]


@pytest.mark.parametrize(
    ("case_name", "raw_url", "expected"),
    CANONICALIZE_CASES,
    ids=[c[0] for c in CANONICALIZE_CASES],
)
def test_canonicalize_url(case_name: str, raw_url: str, expected: str) -> None:
    assert canonicalize_url(raw_url) == expected


def test_canonicalize_url_empty_string_returns_unchanged() -> None:
    assert canonicalize_url("") == ""


def test_canonicalize_url_is_idempotent() -> None:
    """canonicalize_url(canonicalize_url(x)) == canonicalize_url(x) — áp lại không đổi thêm."""
    raw = "https://WWW.Example.com/a/?utm_source=x&keep=1#frag"
    once = canonicalize_url(raw)
    twice = canonicalize_url(once)
    assert once == twice


# ---------------------------------------------------------------------------
# extract_domain
# ---------------------------------------------------------------------------


def test_extract_domain_from_canonical_url() -> None:
    assert extract_domain("https://example.com/a") == "example.com"


def test_extract_domain_no_www_since_canonical_already_stripped() -> None:
    assert (
        extract_domain(canonicalize_url("https://www.example.com/a")) == "example.com"
    )


# ---------------------------------------------------------------------------
# compute_content_hash
# ---------------------------------------------------------------------------


def test_content_hash_same_title_and_domain_same_hash() -> None:
    assert compute_content_hash("Hello World", "example.com") == compute_content_hash(
        "Hello World", "example.com"
    )


def test_content_hash_trims_and_lowercases_title() -> None:
    """lower(trim(title)) — khoảng trắng/hoa thường quanh title không ảnh hưởng hash."""
    assert compute_content_hash(
        "  Hello World  ", "example.com"
    ) == compute_content_hash("hello world", "example.com")


def test_content_hash_different_title_different_hash() -> None:
    assert compute_content_hash("Title A", "example.com") != compute_content_hash(
        "Title B", "example.com"
    )


def test_content_hash_different_domain_different_hash() -> None:
    assert compute_content_hash("Same Title", "a.com") != compute_content_hash(
        "Same Title", "b.com"
    )


def test_content_hash_no_accidental_collision_across_boundary() -> None:
    """Không có separator sẽ khiến ('ab','c.com') trùng ('a','bc.com') — kiểm tra không trùng."""
    assert compute_content_hash("ab", "c.com") != compute_content_hash("a", "bc.com")


# ---------------------------------------------------------------------------
# make_article_id — UUIDv5 xác định, KHÔNG dùng uuid4 (P1)
# ---------------------------------------------------------------------------


def test_make_article_id_same_url_same_uuid() -> None:
    url = "https://example.com/a"
    assert make_article_id(url) == make_article_id(url)


def test_make_article_id_different_url_different_uuid() -> None:
    assert make_article_id("https://example.com/a") != make_article_id(
        "https://example.com/b"
    )


def test_make_article_id_is_uuid5_not_random() -> None:
    """Xác định bằng thuật toán UUIDv5 (version=5) — không phải uuid4 ngẫu nhiên."""
    result = make_article_id("https://example.com/a")
    assert result.version == 5


def test_make_article_id_matches_hardcoded_expected_value() -> None:
    """Giá trị cố định tính sẵn — chứng minh kết quả không đổi qua nhiều lần chạy/tiến trình.

    UUIDv5 là hàm băm thuần tuý của (namespace, name), không phụ thuộc runtime/process/máy
    chạy — nếu test này pass ổn định qua các lần CI khác nhau, tức là make_article_id()
    cho cùng kết quả dù chạy ở tiến trình/thời điểm nào.
    """
    url = "https://example.com/a"
    expected = uuid.uuid5(ARTICLE_ID_NAMESPACE, url)
    assert make_article_id(url) == expected


# ---------------------------------------------------------------------------
# parse_entry — bóc tách title/snippet/published_at từ payload bronze
# ---------------------------------------------------------------------------


def test_parse_entry_extracts_title_snippet_published_at() -> None:
    payload = {
        "title": "  Hello   World  ",
        "link": "https://example.com/a",
        "summary": "<p>Some <b>snippet</b></p>",
        "published_parsed": "2026-08-01T10:00:00+00:00",
    }
    result = parse_entry(payload)
    assert result == ParsedArticle(
        title="Hello World",
        link="https://example.com/a",
        snippet="Some snippet",
        published_at=datetime(2026, 8, 1, 10, 0, 0, tzinfo=UTC),
    )


def test_parse_entry_missing_title_returns_none() -> None:
    payload = {"link": "https://example.com/a", "summary": "x"}
    assert parse_entry(payload) is None


def test_parse_entry_missing_link_returns_none() -> None:
    payload = {"title": "Has title", "summary": "x"}
    assert parse_entry(payload) is None


def test_parse_entry_blank_title_returns_none() -> None:
    payload = {"title": "   ", "link": "https://example.com/a"}
    assert parse_entry(payload) is None


def test_parse_entry_falls_back_to_id_when_link_missing() -> None:
    payload = {"title": "T", "id": "https://example.com/guid-only"}
    result = parse_entry(payload)
    assert result is not None
    assert result.link == "https://example.com/guid-only"


def test_parse_entry_unparseable_date_gives_none_published_at_not_none_record() -> None:
    payload = {"title": "T", "link": "https://example.com/a", "published": "not a date"}
    result = parse_entry(payload)
    assert result is not None
    assert result.published_at is None


def test_parse_entry_falls_back_to_raw_rfc822_published_field() -> None:
    """Không có published_parsed (ISO) thì thử parse chuỗi RFC822 gốc `published`."""
    payload = {
        "title": "T",
        "link": "https://example.com/a",
        "published": "Mon, 10 Aug 2026 11:02:38 +0000",
    }
    result = parse_entry(payload)
    assert result is not None
    assert result.published_at == datetime(2026, 8, 10, 11, 2, 38, tzinfo=UTC)


def test_parse_entry_no_date_fields_at_all_gives_none() -> None:
    payload = {"title": "T", "link": "https://example.com/a"}
    result = parse_entry(payload)
    assert result is not None
    assert result.published_at is None


def test_parse_entry_uses_description_when_summary_missing() -> None:
    payload = {
        "title": "T",
        "link": "https://example.com/a",
        "description": "desc text",
    }
    result = parse_entry(payload)
    assert result is not None
    assert result.snippet == "desc text"


# ---------------------------------------------------------------------------
# apply_cold_start_rules — PRODUCTION_PLAN §8.2
# ---------------------------------------------------------------------------

NOW = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)


def test_cold_start_old_article_excluded_too_old() -> None:
    published_at = NOW - timedelta(days=10)
    result = apply_cold_start_rules(published_at, now=NOW, max_article_age_days=7)
    assert result.status == "excluded"
    assert result.exclusion_reason == "too_old"
    assert result.published_at_imputed is False


def test_cold_start_no_date_is_imputed_and_still_ingested() -> None:
    result = apply_cold_start_rules(None, now=NOW, max_article_age_days=7)
    assert result.status == "ingested"
    assert result.exclusion_reason is None
    assert result.published_at_imputed is True


def test_cold_start_recent_article_is_ingested_not_excluded() -> None:
    published_at = NOW - timedelta(hours=2)
    result = apply_cold_start_rules(published_at, now=NOW, max_article_age_days=7)
    assert result.status == "ingested"
    assert result.exclusion_reason is None
    assert result.published_at_imputed is False


def test_cold_start_exactly_at_cutoff_is_not_excluded() -> None:
    """Biên: đúng bằng ngưỡng (không CŨ HƠN) thì chưa bị loại — so sánh dùng '<' nghiêm ngặt."""
    published_at = NOW - timedelta(days=7)
    result = apply_cold_start_rules(published_at, now=NOW, max_article_age_days=7)
    assert result.status == "ingested"
    assert result.exclusion_reason is None


def test_cold_start_just_past_cutoff_is_excluded() -> None:
    published_at = NOW - timedelta(days=7, seconds=1)
    result = apply_cold_start_rules(published_at, now=NOW, max_article_age_days=7)
    assert result.status == "excluded"
    assert result.exclusion_reason == "too_old"
