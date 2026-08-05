"""Domain CRUD repositories for articles, source health, and job runs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.intel_bot.db.models import Article, JobRun, SourceHealth


class ArticleRepository:
    def __init__(self, session: Session):
        self.session = session

    def insert_raw(
        self,
        *,
        canonical_url: str,
        content_hash: str,
        source_id: str,
        source_type: str,
        title: str,
        snippet: str,
        published_at: Optional[datetime] = None,
        author: Optional[str] = None,
    ) -> Article:
        art = Article(
            canonical_url=canonical_url,
            content_hash=content_hash,
            source_id=source_id,
            source_type=source_type,
            title=title[:1024] if title else None,
            snippet=snippet[:4000] if snippet else None,
            published_at=published_at,
            status='raw',
        )
        self.session.add(art)
        self.session.flush()
        return art

    def list_raw(self, limit: Optional[int] = None) -> list[Article]:
        q = (
            self.session.query(Article)
            .filter(Article.status == 'raw')
            .order_by(Article.first_seen_at.asc())
        )
        if limit:
            q = q.limit(limit)
        return q.all()

    def set_filtered(self, article: Article, industry_tags: list[str]) -> None:
        article.status = 'filtered'
        article.industry_tags = industry_tags
        article.rejection_reason = None

    def set_rejected(self, article: Article, reason: str) -> None:
        article.status = 'rejected'
        article.rejection_reason = reason


class SourceHealthRepository:
    def __init__(self, session: Session):
        self.session = session

    def record_success(self, source_id: str) -> None:
        sh = self.session.get(SourceHealth, source_id)
        now = datetime.now(timezone.utc)
        if not sh:
            sh = SourceHealth(
                source_id=source_id,
                consecutive_failures=0,
                last_success_at=now,
                last_error=None,
            )
            self.session.add(sh)
        else:
            sh.consecutive_failures = 0
            sh.last_success_at = now
            sh.last_error = None

    def record_failure(self, source_id: str, error: str) -> None:
        sh = self.session.get(SourceHealth, source_id)
        if not sh:
            sh = SourceHealth(source_id=source_id, consecutive_failures=1, last_error=error)
            self.session.add(sh)
        else:
            sh.consecutive_failures = (sh.consecutive_failures or 0) + 1
            sh.last_error = error[:2000] if error else 'unknown'


class JobRunRepository:
    def __init__(self, session: Session):
        self.session = session

    def start(self, job_name: str, metadata: Optional[dict[str, Any]] = None) -> JobRun:
        run = JobRun(
            job_name=job_name,
            status='running',
            run_metadata=metadata or {},
        )
        self.session.add(run)
        self.session.flush()
        return run

    def finish(
        self,
        run: JobRun,
        *,
        status: str,
        items_processed: int = 0,
        items_failed: int = 0,
        error_summary: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> JobRun:
        run.status = status
        run.finished_at = datetime.now(timezone.utc)
        run.items_processed = items_processed
        run.items_failed = items_failed
        run.error_summary = error_summary
        if metadata:
            merged = dict(run.run_metadata or {})
            merged.update(metadata)
            run.run_metadata = merged
        self.session.flush()
        return run
