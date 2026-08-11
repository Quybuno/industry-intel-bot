"""Fetch/parse RSS đồng bộ kiểu v1 — CHỈ để giữ cho pipeline ORM cũ
(`jobs/ingest_job.py`, `reddit_fetcher.py`) còn import được, không dùng trong
luồng ingest mới của task 0.4 (xem `rss_fetcher.py` cho luồng async ghi bronze).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import feedparser  # type: ignore[import-untyped]
import requests

from src.intel_bot.ingest.legacy_normalizer import (
    compute_content_hash_legacy,
    parse_rss_published,
)
from src.intel_bot.ingest.normalizer import (
    canonicalize_url,
    normalize_title,
    strip_html,
)

logger = logging.getLogger(__name__)

_LEGACY_UA = "industry-intel-bot/0.1 (+https://example.com/contact)"
_LEGACY_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}


def fetch_feed_legacy(
    url: str, *, timeout: int = 30, retries: int = 3
) -> feedparser.FeedParserDict | None:
    """Fetch + parse đồng bộ, retry đơn giản — bản sao rss_fetcher.py trước task 0.4."""
    headers = {"User-Agent": _LEGACY_UA, **_LEGACY_HEADERS}
    for attempt in range(retries):
        if attempt > 0:
            time.sleep(2 ** (attempt - 1))
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Fetch failed %s: %s", url, exc)
            continue
        feed = feedparser.parse(r.content)
        if getattr(feed, "bozo", False) or not feed.entries:
            logger.warning("Invalid/empty feed for %s", url)
            continue
        return feed
    logger.error("All fetch attempts failed for %s", url)
    return None


def parse_rss_entries_legacy(
    feed: feedparser.FeedParserDict,
    source: dict[str, Any],
    *,
    limit: int | None = None,
    source_type: str = "rss",
) -> list[dict[str, Any]]:
    """Chuyển entry sang dict cho `Article` ORM cũ (canonical_url, content_hash, ...)."""
    entries = feed.entries[:limit] if limit else feed.entries
    articles: list[dict[str, Any]] = []
    for entry in entries:
        link = entry.get("link") or entry.get("id")
        if not link:
            continue
        title = normalize_title(entry.get("title", ""))
        raw_snippet = entry.get("summary") or entry.get("description") or ""
        snippet = strip_html(raw_snippet)
        canonical = canonicalize_url(link)
        content_hash = compute_content_hash_legacy(title, canonical)
        articles.append(
            {
                "canonical_url": canonical,
                "content_hash": content_hash,
                "source_id": source["id"],
                "source_type": source_type,
                "title": title,
                "snippet": snippet,
                "published_at": parse_rss_published(entry),
                "author": entry.get("author"),
            }
        )
    return articles
