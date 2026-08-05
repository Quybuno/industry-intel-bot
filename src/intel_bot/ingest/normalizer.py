"""URL canonicalization, title normalization, content hashing."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl

_GITHUB_RE = re.compile(r'^https?://(?:www\.)?github\.com/([^/]+)/([^/?#]+)', re.I)
_STRIP_PARAMS = frozenset({'fbclid', 'ref', 'gclid', 'mc_cid', 'mc_eid'})


def canonicalize_url(url: str) -> str:
    """Normalize URL: lowercase scheme/host, strip www, drop tracking params, trim trailing slash."""
    if not url:
        return url

    parsed = urlparse(url.strip())
    scheme = (parsed.scheme or 'https').lower()
    netloc = parsed.netloc.lower()
    if netloc.startswith('www.'):
        netloc = netloc[4:]

    gh = _GITHUB_RE.match(url.strip())
    if gh:
        owner, repo = gh.group(1), gh.group(2).removesuffix('.git')
        return f'https://github.com/{owner}/{repo}'

    qs = parse_qsl(parsed.query, keep_blank_values=True)
    filtered = [(k, v) for k, v in qs if not k.lower().startswith('utm_') and k.lower() not in _STRIP_PARAMS]
    new_query = '&'.join(f'{k}={v}' for k, v in filtered)
    path = parsed.path.rstrip('/') or parsed.path
    return urlunparse((scheme, netloc, path, '', new_query, ''))


def normalize_title(title: str) -> str:
    """Collapse whitespace and strip."""
    if not title:
        return ''
    return re.sub(r'\s+', ' ', title.strip())


def domain_from_url(url: str) -> str:
    parsed = urlparse(canonicalize_url(url))
    return parsed.netloc.lower()


def compute_content_hash(title: str, url: str) -> str:
    """Hash of lowercase(trim(title)) + domain(url) for cross-source dedup."""
    normalized = normalize_title(title).lower()
    domain = domain_from_url(url)
    payload = f'{normalized}|{domain}'
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]


def parse_rss_published(entry: dict[str, Any]) -> Optional[datetime]:
    """Extract published datetime from a feedparser entry."""
    for key in ('published_parsed', 'updated_parsed'):
        parsed = entry.get(key)
        if parsed:
            try:
                return datetime(*parsed[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
    for key in ('published', 'updated'):
        raw = entry.get(key)
        if raw:
            try:
                from email.utils import parsedate_to_datetime
                return parsedate_to_datetime(raw)
            except (TypeError, ValueError):
                continue
    return None


def parse_github_pushed_at(pushed_at: Optional[str]) -> Optional[datetime]:
    if not pushed_at:
        return None
    try:
        return datetime.fromisoformat(pushed_at.replace('Z', '+00:00'))
    except ValueError:
        return None


def strip_html(text: str) -> str:
    """Remove simple HTML tags from RSS summaries."""
    if not text:
        return ''
    return re.sub(r'<[^>]+>', '', text).strip()
