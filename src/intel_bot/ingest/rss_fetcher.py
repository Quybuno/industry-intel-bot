"""RSS feed fetcher — HTTP + feedparser, no DB side effects."""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import feedparser
import requests

from src.intel_bot.ingest.normalizer import (
    canonicalize_url,
    compute_content_hash,
    normalize_title,
    parse_rss_published,
    strip_html,
)

logger = logging.getLogger(__name__)

DEFAULT_UA = 'industry-intel-bot/0.1 (+https://example.com/contact)'
FEED_USER_AGENTS = [
    DEFAULT_UA,
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
]
FEED_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}


def _build_user_agents(ua: Optional[str]) -> list[str]:
    """Ordered, deduplicated list of user agents to try, preferred one first."""
    candidates = [ua, *FEED_USER_AGENTS] if ua else FEED_USER_AGENTS
    seen: set[str] = set()
    result = []
    for agent in candidates:
        if agent and agent not in seen:
            seen.add(agent)
            result.append(agent)
    return result


def _try_fetch_with_ua(url: str, agent: str, timeout: int) -> Optional[feedparser.FeedParserDict]:
    """Attempt a single fetch+parse with one user agent. Returns None on any failure."""
    headers = {'User-Agent': agent, **FEED_HEADERS}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
    except Exception as exc:
        logger.warning('Fetch failed %s (UA=%s): %s', url, agent, exc)
        return None

    feed = feedparser.parse(r.content)
    if getattr(feed, 'bozo', False):
        logger.warning(
            'Invalid feed parse for %s (UA=%s): %s',
            url, agent, getattr(feed, 'bozo_exception', 'unknown'),
        )
        return None
    if not feed.entries:
        logger.warning('Feed parsed but empty: %s (UA=%s)', url, agent)
        return None

    logger.info('Fetched %s: %d entries (UA=%s)', url, len(feed.entries), agent)
    return feed


def fetch_feed(
    url: str,
    *,
    timeout: int = 30,
    retries: int = 3,
    ua: Optional[str] = None,
) -> Optional[feedparser.FeedParserDict]:
    """Fetch and parse RSS/Atom feed, retrying with UA rotation on failure."""
    user_agents = _build_user_agents(ua)

    for attempt in range(retries):
        if attempt > 0:
            time.sleep(2 ** (attempt - 1))

        for agent in user_agents:
            feed = _try_fetch_with_ua(url, agent, timeout)
            if feed is not None:
                return feed

    logger.error('All fetch attempts failed for %s', url)
    return None


def parse_rss_entries(
    feed: feedparser.FeedParserDict,
    source: dict[str, Any],
    *,
    limit: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Convert feedparser entries into normalized article dicts."""
    entries = feed.entries[:limit] if limit else feed.entries
    articles: list[dict[str, Any]] = []

    for entry in entries:
        link = entry.get('link') or entry.get('id')
        if not link:
            continue

        title = normalize_title(entry.get('title', ''))
        raw_snippet = entry.get('summary') or entry.get('description') or ''
        snippet = strip_html(raw_snippet)
        canonical = canonicalize_url(link)
        content_hash = compute_content_hash(title, canonical)

        articles.append({
            'canonical_url': canonical,
            'content_hash': content_hash,
            'source_id': source['id'],
            'source_type': 'rss',
            'title': title,
            'snippet': snippet,
            'published_at': parse_rss_published(entry),
            'author': entry.get('author'),
        })

    return articles
