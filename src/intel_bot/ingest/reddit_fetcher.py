"""Reddit extractor backed by Reddit RSS feeds, no DB side effects.

Ghi chú: đây là code v1 (pipeline ORM cũ, không liên quan tới bronze/silver của v2).
`rss_fetcher.py` đã được viết lại cho task 0.4 (async, ghi bronze.raw_articles) nên
fetch/parse đồng bộ kiểu cũ được giữ lại ở `legacy_rss.py` để module này (v1) không vỡ.
"""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote_plus

import feedparser  # type: ignore[import-untyped]

from src.intel_bot.ingest.legacy_rss import fetch_feed_legacy, parse_rss_entries_legacy

REDDIT_BASE = "https://www.reddit.com"


def build_reddit_rss_url(url_or_query: str, *, limit: int = 25) -> str:
    """Build a Reddit RSS URL from a subreddit/listing, search query, or full URL."""
    raw = (url_or_query or "").strip()

    if raw.startswith("http://") or raw.startswith("https://"):
        if ".rss" in raw:
            separator = "&" if "?" in raw else "?"
            return f"{raw}{separator}limit={limit}"
        base, _, query = raw.partition("?")
        suffix = "" if base.endswith("/") else "/"
        rss_url = f"{base}{suffix}.rss"
        joiner = "&" if query else ""
        return f"{rss_url}?{query}{joiner}limit={limit}"

    if raw.startswith("r/"):
        raw = raw[2:]

    if raw.startswith("search:"):
        query = quote_plus(raw.removeprefix("search:").strip())
        return f"{REDDIT_BASE}/search.rss?q={query}&sort=new&limit={limit}"

    parts = [part for part in raw.split("/") if part]
    subreddit = parts[0] if parts else "technology"
    listing = parts[1] if len(parts) > 1 else "hot"
    return f"{REDDIT_BASE}/r/{subreddit}/{listing}/.rss?limit={limit}"


def fetch_reddit_feed(
    url_or_query: str,
    *,
    limit: int = 25,
    timeout: int = 30,
    retries: int = 3,
) -> Optional[feedparser.FeedParserDict]:
    """Fetch and parse Reddit RSS through the shared RSS fetcher."""
    return fetch_feed_legacy(
        build_reddit_rss_url(url_or_query, limit=limit),
        timeout=timeout,
        retries=retries,
    )


def parse_reddit_entries(
    feed: feedparser.FeedParserDict,
    source: dict[str, Any],
    *,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Convert Reddit RSS entries into normalized article dicts."""
    return parse_rss_entries_legacy(feed, source, limit=limit, source_type="reddit")
