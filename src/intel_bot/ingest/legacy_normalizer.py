"""`compute_content_hash` chữ ký cũ (title, url) — CHỈ để `github_fetcher.py`,
`github_trending_fetcher.py`, `legacy_rss.py` (v1) còn chạy đúng như trước.

Task 0.5 đổi chữ ký `compute_content_hash` thành `(title, domain)` trong
`normalizer.py` — không tương thích ngược, nên giữ bản cũ riêng ở đây thay vì
sửa hành vi ngầm cho code v1 không ai gọi tới từ CLI nữa.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

from src.intel_bot.ingest.normalizer import (
    canonicalize_url,
    extract_domain,
    normalize_title,
)


def compute_content_hash_legacy(title: str, url: str) -> str:
    """Hash của lower(trim(title)) + domain(url) — bản v1, domain tự suy ra từ url."""
    normalized = normalize_title(title).lower()
    domain = extract_domain(canonicalize_url(url))
    payload = f"{normalized}|{domain}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def parse_rss_published(entry: dict[str, Any]) -> datetime | None:
    """Lấy published datetime từ một entry feedparser (v1) — dùng cho legacy_rss.py."""
    for key in ("published_parsed", "updated_parsed"):
        parsed = entry.get(key)
        if parsed:
            try:
                year, month, day, hour, minute, second = parsed[:6]
                return datetime(year, month, day, hour, minute, second, tzinfo=UTC)
            except (TypeError, ValueError):
                continue
    for key in ("published", "updated"):
        raw = entry.get(key)
        if raw:
            try:
                from email.utils import parsedate_to_datetime

                return parsedate_to_datetime(raw)
            except (TypeError, ValueError):
                continue
    return None


def parse_github_pushed_at(pushed_at: str | None) -> datetime | None:
    """Parse trường `pushed_at` của GitHub API (v1) — dùng cho github_fetcher.py."""
    if not pushed_at:
        return None
    try:
        return datetime.fromisoformat(pushed_at)
    except ValueError:
        return None
