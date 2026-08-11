"""GitHub Trending extractor - HTML scrape, no DB side effects."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from src.intel_bot.ingest.legacy_normalizer import compute_content_hash_legacy
from src.intel_bot.ingest.normalizer import canonicalize_url, normalize_title

logger = logging.getLogger(__name__)

GITHUB_TRENDING_URL = "https://github.com/trending"
TRENDING_UA = "industry-intel-bot/0.1 (+https://example.com/contact)"


def _build_trending_url(url_or_query: str) -> str:
    raw = (url_or_query or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw

    if not raw:
        return GITHUB_TRENDING_URL

    language = ""
    params: dict[str, str] = {}
    for token in raw.split():
        if token.startswith("since="):
            params["since"] = token.split("=", 1)[1]
        elif token.startswith("spoken_language_code="):
            params["spoken_language_code"] = token.split("=", 1)[1]
        elif not language:
            language = token

    url = f"{GITHUB_TRENDING_URL}/{language}" if language else GITHUB_TRENDING_URL
    return f"{url}?{urlencode(params)}" if params else url


def fetch_github_trending(
    url_or_query: str = "",
    *,
    timeout: int = 30,
    retries: int = 3,
) -> str:
    """Fetch GitHub Trending HTML."""
    headers = {"User-Agent": TRENDING_UA, "Accept": "text/html"}
    url = _build_trending_url(url_or_query)
    last_error: Optional[Exception] = None

    for attempt in range(retries):
        if attempt > 0:
            time.sleep(2 ** (attempt - 1))
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
            logger.warning(
                "GitHub Trending fetch failed (attempt=%d): %s", attempt + 1, exc
            )

    raise RuntimeError(
        f"GitHub Trending fetch failed for {url_or_query!r}: {last_error}"
    )


def parse_github_trending(html: str, source: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert GitHub Trending repository rows into normalized article dicts."""
    soup = BeautifulSoup(html, "html.parser")
    articles: list[dict[str, Any]] = []

    for row in soup.select("article.Box-row"):
        link = row.select_one("h2 a")
        if not link:
            continue

        repo_path = (
            normalize_title(link.get_text(" ", strip=True))
            .replace(" / ", "/")
            .replace(" ", "")
        )
        if not repo_path:
            continue

        raw_url = f"https://github.com/{repo_path}"
        description_node = row.select_one("p")
        description = (
            normalize_title(description_node.get_text(" ", strip=True))
            if description_node
            else ""
        )
        language_node = row.select_one('[itemprop="programmingLanguage"]')
        language = (
            normalize_title(language_node.get_text(" ", strip=True))
            if language_node
            else ""
        )
        stars_node = row.select_one('a[href$="/stargazers"]')
        stars = (
            normalize_title(stars_node.get_text(" ", strip=True)) if stars_node else ""
        )
        today_node = row.select_one("span.d-inline-block.float-sm-right")
        today = (
            normalize_title(today_node.get_text(" ", strip=True)) if today_node else ""
        )
        snippet = " | ".join(
            part for part in (description, language, stars, today) if part
        )
        canonical = canonicalize_url(raw_url)
        content_hash = compute_content_hash_legacy(repo_path, canonical)

        articles.append(
            {
                "canonical_url": canonical,
                "content_hash": content_hash,
                "source_id": source["id"],
                "source_type": "github_trending",
                "title": repo_path,
                "snippet": snippet[:4000],
                "published_at": datetime.now(timezone.utc),
                "author": repo_path.split("/", 1)[0],
            }
        )

    return articles
