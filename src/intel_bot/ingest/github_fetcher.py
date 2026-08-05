"""GitHub repository search fetcher — no DB side effects."""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests

from src.intel_bot.config import settings
from src.intel_bot.ingest.normalizer import (
    canonicalize_url,
    compute_content_hash,
    normalize_title,
    parse_github_pushed_at,
)

logger = logging.getLogger(__name__)

GITHUB_API = 'https://api.github.com'


def search_repositories(
    query: str,
    *,
    per_page: int = 5,
    timeout: int = 30,
    retries: int = 3,
) -> list[dict[str, Any]]:
    """Search GitHub repos with retry."""
    headers: dict[str, str] = {'Accept': 'application/vnd.github+json'}
    if settings.GITHUB_TOKEN:
        headers['Authorization'] = f'Bearer {settings.GITHUB_TOKEN}'

    params = {'q': query, 'sort': 'updated', 'order': 'desc', 'per_page': per_page}
    last_error: Optional[Exception] = None

    for attempt in range(retries):
        if attempt > 0:
            time.sleep(2 ** (attempt - 1))
        try:
            r = requests.get(
                f'{GITHUB_API}/search/repositories',
                headers=headers,
                params=params,
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json().get('items', [])
        except Exception as exc:
            last_error = exc
            logger.warning('GitHub search failed (attempt=%d): %s', attempt + 1, exc)

    raise RuntimeError(f'GitHub search failed for query={query!r}: {last_error}')


def parse_github_repos(
    repos: list[dict[str, Any]],
    source: dict[str, Any],
) -> list[dict[str, Any]]:
    """Convert GitHub API repo items into normalized article dicts."""
    articles: list[dict[str, Any]] = []

    for repo in repos:
        url = repo.get('html_url')
        if not url:
            continue

        title = normalize_title(repo.get('full_name') or repo.get('name', ''))
        desc = repo.get('description') or ''
        stars = repo.get('stargazers_count', 0)
        snippet = f'{desc} (⭐ {stars})' if desc else f'⭐ {stars} stars'
        canonical = canonicalize_url(url)
        content_hash = compute_content_hash(title, canonical)

        articles.append({
            'canonical_url': canonical,
            'content_hash': content_hash,
            'source_id': source['id'],
            'source_type': 'github',
            'title': title,
            'snippet': snippet[:4000],
            'published_at': parse_github_pushed_at(repo.get('pushed_at')),
            'author': (repo.get('owner') or {}).get('login'),
        })

    return articles
