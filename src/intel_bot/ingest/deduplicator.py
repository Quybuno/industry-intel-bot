"""Deduplication: canonical_url (DB unique) + content_hash (cross-source warning)."""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.orm import Session

from src.intel_bot.db.models import Article

logger = logging.getLogger(__name__)


def url_exists(session: Session, canonical_url: str) -> bool:
    return session.query(Article.id).filter(Article.canonical_url == canonical_url).first() is not None


def find_by_content_hash(session: Session, content_hash: str) -> Optional[Article]:
    if not content_hash:
        return None
    return session.query(Article).filter(Article.content_hash == content_hash).first()


def check_duplicate(
    session: Session,
    canonical_url: str,
    content_hash: str,
    source_id: str,
) -> tuple[bool, Optional[str]]:
    """
    Returns (should_insert, skip_reason).
    skip_reason: 'url_exists' | 'content_hash_exists'
    """
    if url_exists(session, canonical_url):
        return False, 'url_exists'

    existing = find_by_content_hash(session, content_hash)
    if existing:
        logger.warning(
            'Duplicate content_hash across sources: hash=%s new_source=%s existing_source=%s title=%s',
            content_hash,
            source_id,
            existing.source_id,
            existing.title,
        )
        return False, 'content_hash_exists'

    return True, None
