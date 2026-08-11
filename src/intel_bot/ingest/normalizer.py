"""Chuẩn hoá URL/title, tính content_hash/article_id, bóc tách entry, áp quy tắc cold start.

Toàn bộ hàm trong module này THUẦN (không I/O, không đọc DB/mạng/đồng hồ hệ thống trực
tiếp — `now` luôn nhận qua tham số) để test được và tách khỏi `loader.py` (có I/O).
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_GITHUB_URL_RE = re.compile(
    r"^https?://(?:www\.)?github\.com/([^/]+)/([^/?#]+)", re.IGNORECASE
)

# PRODUCTION_PLAN §8.3: bỏ mọi biến thể utm_*, cộng thêm 4 tham số tracking phổ biến.
_STRIP_PARAM_PREFIXES = ("utm_",)
_STRIP_PARAM_EXACT = frozenset({"fbclid", "gclid", "ref", "source"})

# Namespace UUID riêng của app — xác định (deterministic: cùng input luôn cho cùng UUID),
# KHÔNG phải giá trị ngẫu nhiên — để make_article_id() dùng UUIDv5, không dùng uuid4 (P1:
# chạy bù/chạy lại nhiều lần, nhiều tiến trình phải cho cùng article_id).
ARTICLE_ID_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_DNS, "industry-intel-bot.silver.articles"
)


def canonicalize_url(raw_url: str) -> str:
    """Chuẩn hoá URL để dùng làm khoá dedup cấp 1 (PRODUCTION_PLAN §8.3).

    - Lowercase scheme + host, bỏ tiền tố ``www.``.
    - Bỏ query param ``utm_*`` (mọi biến thể), ``fbclid``, ``gclid``, ``ref``, ``source``;
      giữ nguyên các param khác.
    - Bỏ trailing slash và fragment.
    - URL GitHub rút gọn về ``https://github.com/{owner}/{repo}``.
    """
    if not raw_url or not raw_url.strip():
        return raw_url

    stripped = raw_url.strip()

    github_match = _GITHUB_URL_RE.match(stripped)
    if github_match:
        owner, repo = github_match.group(1), github_match.group(2).removesuffix(".git")
        return f"https://github.com/{owner}/{repo}"

    parsed = urlparse(stripped)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    netloc = netloc.removeprefix("www.")

    kept_params = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith(_STRIP_PARAM_PREFIXES)
        and key.lower() not in _STRIP_PARAM_EXACT
    ]
    new_query = urlencode(kept_params)

    path = parsed.path.rstrip("/")

    return urlunparse((scheme, netloc, path, "", new_query, ""))


def extract_domain(canonical_url: str) -> str:
    """Domain (netloc) của một canonical_url — input cho `compute_content_hash`."""
    return urlparse(canonical_url).netloc


def normalize_title(title: str) -> str:
    """Gộp khoảng trắng liên tiếp, strip đầu/cuối."""
    if not title:
        return ""
    return re.sub(r"\s+", " ", title.strip())


def strip_html(text: str) -> str:
    """Bỏ thẻ HTML đơn giản khỏi snippet RSS."""
    if not text:
        return ""
    return re.sub(r"<[^>]+>", "", text).strip()


def compute_content_hash(title: str, domain: str) -> str:
    """SHA-256 của ``lower(trim(title))`` nối domain — dùng cho dedup cấp 2 (PRODUCTION_PLAN §8.4).

    Chỉ TÍNH và LƯU ở đây — logic gộp/giữ bản điểm cao nhất là business logic, làm ở dbt (P5).
    """
    normalized_title = title.strip().lower()
    normalized_domain = domain.strip().lower()
    payload = f"{normalized_title}|{normalized_domain}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def make_article_id(canonical_url: str) -> uuid.UUID:
    """UUIDv5 xác định từ canonical_url — cùng URL luôn cho cùng article_id (P1). KHÔNG dùng uuid4."""
    return uuid.uuid5(ARTICLE_ID_NAMESPACE, canonical_url)


@dataclass(frozen=True)
class ParsedArticle:
    """Kết quả bóc tách một entry bronze — chưa canonicalize URL, chưa áp quy tắc cold start."""

    title: str
    link: str
    snippet: str
    published_at: datetime | None


def parse_entry(payload: dict[str, Any]) -> ParsedArticle | None:
    """Bóc tách title/snippet/published_at từ payload JSONB (bronze.raw_articles.payload).

    Trả None nếu thiếu title hoặc thiếu link — bản ghi không đủ điều kiện đi tiếp.
    """
    title = normalize_title(str(payload.get("title") or ""))
    link = str(payload.get("link") or payload.get("id") or "").strip()
    if not title or not link:
        return None

    raw_snippet = payload.get("summary") or payload.get("description") or ""
    snippet = strip_html(str(raw_snippet))
    published_at = _parse_published_at(payload)

    return ParsedArticle(
        title=title, link=link, snippet=snippet, published_at=published_at
    )


def _parse_published_at(payload: dict[str, Any]) -> datetime | None:
    """Thử nhiều định dạng ngày theo thứ tự ưu tiên; không parse được trường nào thì trả None."""
    for key in ("published_parsed", "updated_parsed"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            parsed = _try_parse_iso(value)
            if parsed is not None:
                return parsed

    for key in ("published", "updated"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            parsed = _try_parse_rfc822(value) or _try_parse_iso(value)
            if parsed is not None:
                return parsed

    return None


def _try_parse_iso(value: str) -> datetime | None:
    """Thử parse ISO 8601 (định dạng feedparser struct_time đã được chuẩn hoá ở task 0.4)."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _try_parse_rfc822(value: str) -> datetime | None:
    """Thử parse RFC 822 (định dạng pubDate/updated gốc của RSS/Atom)."""
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(frozen=True)
class ColdStartResult:
    """Kết quả áp quy tắc cold start cho một bản ghi."""

    status: str
    exclusion_reason: str | None
    published_at_imputed: bool


def apply_cold_start_rules(
    published_at: datetime | None,
    *,
    now: datetime,
    max_article_age_days: int,
) -> ColdStartResult:
    """Áp quy tắc cold start (PRODUCTION_PLAN §8.2).

    - ``published_at`` không NULL và cũ hơn `max_article_age_days` so với `now`
      → status='excluded', exclusion_reason='too_old' (vẫn ghi vào silver, chỉ không đi tiếp).
    - ``published_at`` NULL → published_at_imputed=True (nơi khác dùng first_seen_at làm mốc).

    `now` nhận qua tham số (không tự gọi `datetime.now()` bên trong) để hàm thuần, test được.
    """
    if published_at is None:
        return ColdStartResult(
            status="ingested", exclusion_reason=None, published_at_imputed=True
        )

    cutoff = now - timedelta(days=max_article_age_days)
    if published_at < cutoff:
        return ColdStartResult(
            status="excluded", exclusion_reason="too_old", published_at_imputed=False
        )

    return ColdStartResult(
        status="ingested", exclusion_reason=None, published_at_imputed=False
    )
