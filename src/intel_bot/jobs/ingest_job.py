"""Ingest job orchestrator: fetch RSS + GitHub, normalize, dedup, persist."""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Optional

from src.intel_bot.config import load_yaml
from src.intel_bot.db.models import JobRun
from src.intel_bot.db.repositories import ArticleRepository, JobRunRepository, SourceHealthRepository
from src.intel_bot.db.session import ensure_tables, get_session
from src.intel_bot.ingest.deduplicator import check_duplicate
from src.intel_bot.ingest.github_fetcher import parse_github_repos, search_repositories
from src.intel_bot.ingest.github_trending_fetcher import fetch_github_trending, parse_github_trending
from src.intel_bot.ingest.reddit_fetcher import fetch_reddit_feed, parse_reddit_entries
from src.intel_bot.ingest.legacy_rss import fetch_feed_legacy, parse_rss_entries_legacy
from src.intel_bot.ingest.source_defaults import default_rss_sources
from src.intel_bot.observability.logging import log_event, setup_logging

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    inserted: int = 0
    skipped: int = 0
    sources_ok: int = 0
    sources_failed: int = 0
    errors: list[str] = field(default_factory=list)


def load_ingest_config(config_path: str = 'config/sources.yaml', app_path: str = 'config/app.yaml') -> tuple[list[dict], dict]:
    sources_data = load_yaml(config_path)
    app_data = load_yaml(app_path)
    configured_sources = [s for s in sources_data.get('sources', []) if s.get('enabled', True)]
    configured_non_rss = [s for s in configured_sources if s.get('type') != 'rss']
    sources = [*default_rss_sources(), *configured_non_rss]
    ingest_cfg = app_data.get('ingest', {})
    return sources, ingest_cfg


def _fetch_feed_source(
    source: dict[str, Any],
    ingest_cfg: dict,
    *,
    source_type: str,
) -> tuple[dict, Optional[list[dict]], Optional[str]]:
    """HTTP-only fetch for parallel phase."""
    timeout = ingest_cfg.get('timeout_seconds', 30)
    retries = ingest_cfg.get('retries', 3)
    url = source.get('url_or_query')
    feed = fetch_feed_legacy(url, timeout=timeout, retries=retries)
    if not feed:
        return source, None, 'fetch_failed'
    return source, parse_rss_entries_legacy(feed, source, source_type=source_type), None


def _fetch_reddit_source(source: dict[str, Any], ingest_cfg: dict) -> tuple[dict, Optional[list[dict]], Optional[str]]:
    """HTTP-only Reddit fetch for parallel phase."""
    feed = fetch_reddit_feed(
        source.get('url_or_query', ''),
        limit=ingest_cfg.get('reddit_per_source', 25),
        timeout=ingest_cfg.get('timeout_seconds', 30),
        retries=ingest_cfg.get('retries', 3),
    )
    if not feed:
        return source, None, 'fetch_failed'
    return source, parse_reddit_entries(feed, source), None


def _fetch_github_trending_source(source: dict[str, Any], ingest_cfg: dict) -> tuple[dict, Optional[list[dict]], Optional[str]]:
    """HTTP-only GitHub Trending fetch for parallel phase."""
    try:
        html = fetch_github_trending(
            source.get('url_or_query', ''),
            timeout=ingest_cfg.get('timeout_seconds', 30),
            retries=ingest_cfg.get('retries', 3),
        )
        return source, parse_github_trending(html, source), None
    except Exception as exc:
        logger.warning('GitHub Trending fetch failed for %s: %s', source.get('id'), exc)
        return source, None, str(exc)


def _fetch_extract_source(source: dict[str, Any], ingest_cfg: dict) -> tuple[dict, Optional[list[dict]], Optional[str]]:
    """Dispatch one configured source to the matching extractor."""
    source_type = source.get('type')
    if source_type in {'rss', 'google_news'}:
        return _fetch_feed_source(source, ingest_cfg, source_type=source_type)
    if source_type == 'reddit':
        return _fetch_reddit_source(source, ingest_cfg)
    if source_type == 'github_trending':
        return _fetch_github_trending_source(source, ingest_cfg)
    return source, None, f'unsupported_source_type:{source_type}'


def _ingest_articles(
    session,
    articles: list[dict[str, Any]],
    source_id: str,
) -> tuple[int, int]:
    """Insert articles with dedup. Returns (inserted, skipped)."""
    article_repo = ArticleRepository(session)
    inserted = skipped = 0

    for art in articles:
        should_insert, reason = check_duplicate(
            session,
            art['canonical_url'],
            art['content_hash'],
            source_id,
        )
        if not should_insert:
            skipped += 1
            continue
        try:
            article_repo.insert_raw(
                canonical_url=art['canonical_url'],
                content_hash=art['content_hash'],
                source_id=art['source_id'],
                source_type=art['source_type'],
                title=art['title'],
                snippet=art['snippet'],
                published_at=art.get('published_at'),
                author=art.get('author'),
            )
            inserted += 1
        except Exception as exc:
            session.rollback()
            logger.warning('Insert failed for %s: %s', art['canonical_url'], exc)
            skipped += 1

    return inserted, skipped


def run_ingest_job(
    *,
    limit: Optional[int] = None,
    config_path: str = 'config/sources.yaml',
    app_path: str = 'config/app.yaml',
) -> IngestResult:
    """
    Full ingest pipeline per PRODUCTION_PLAN §7.2:
    job_runs → fetch → normalize → dedup → insert → source_health → finalize.
    """
    setup_logging()
    ensure_tables()

    sources, ingest_cfg = load_ingest_config(config_path, app_path)
    max_workers = ingest_cfg.get('max_concurrent_requests', 5)
    per_github = ingest_cfg.get('github_per_source', 5)

    result = IngestResult()
    parallel_sources = [
        s for s in sources
        if s.get('type') in {'rss', 'google_news', 'reddit', 'github_trending'}
    ]
    github_sources = [s for s in sources if s.get('type') == 'github']

    with get_session() as session:
        job_repo = JobRunRepository(session)
        job_run = job_repo.start('ingest', metadata={'sources_total': len(sources)})
        job_run_id = job_run.id
        session.commit()

    try:
        # Phase 1: parallel HTTP extract (RSS, Google News, Reddit, GitHub Trending)
        fetched_sources: list[tuple[dict, Optional[list[dict]], Optional[str]]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_extract_source, src, ingest_cfg): src
                for src in parallel_sources
            }
            for future in as_completed(futures):
                fetched_sources.append(future.result())

        # Phase 2: DB writes (sequential, one session)
        with get_session() as session:
            health_repo = SourceHealthRepository(session)
            job_repo = JobRunRepository(session)

            for source, articles, error in fetched_sources:
                sid = source['id']
                name = source.get('name', sid)
                if error or not articles:
                    health_repo.record_failure(sid, error or 'no_entries')
                    result.sources_failed += 1
                    result.errors.append(f'{name}: {error or "no_entries"}')
                    continue

                if limit:
                    articles = articles[:limit]

                inserted, skipped = _ingest_articles(session, articles, sid)
                health_repo.record_success(sid)
                result.inserted += inserted
                result.skipped += skipped
                result.sources_ok += 1
                log_event(logger, 'source_ingest_complete', source=sid, inserted=inserted, skipped=skipped)
                session.commit()

            # GitHub sources (sequential — rate limit friendly)
            for source in github_sources:
                sid = source['id']
                name = source.get('name', sid)
                query = source.get('url_or_query')
                if not query:
                    continue
                logger.info('GitHub search: %s (%s)', name, query)
                try:
                    repos = search_repositories(
                        query,
                        per_page=per_github,
                        timeout=ingest_cfg.get('timeout_seconds', 30),
                        retries=ingest_cfg.get('retries', 3),
                    )
                    articles = parse_github_repos(repos, source)
                    if limit:
                        articles = articles[:limit]
                    inserted, skipped = _ingest_articles(session, articles, sid)
                    health_repo.record_success(sid)
                    result.inserted += inserted
                    result.skipped += skipped
                    result.sources_ok += 1
                    log_event(logger, 'source_ingest_complete', source=sid, inserted=inserted, skipped=skipped)
                except Exception as exc:
                    health_repo.record_failure(sid, str(exc))
                    result.sources_failed += 1
                    result.errors.append(f'{name}: {exc}')
                    logger.exception('GitHub ingest failed for %s', sid)
                session.commit()

            # Finalize job_run
            total_sources = len(parallel_sources) + len(github_sources)
            if result.sources_failed == 0:
                status = 'success'
            elif result.sources_ok > 0:
                status = 'partial'
            else:
                status = 'failed'

            job_run = session.get(JobRun, job_run_id)
            job_repo.finish(
                job_run,
                status=status,
                items_processed=result.inserted,
                items_failed=result.sources_failed,
                error_summary='; '.join(result.errors[:5]) if result.errors else None,
                metadata={
                    'sources_total': total_sources,
                    'sources_ok': result.sources_ok,
                    'sources_failed': result.sources_failed,
                    'skipped_duplicates': result.skipped,
                },
            )
            session.commit()

    except Exception as exc:
        logger.exception('Ingest job failed')
        with get_session() as session:
            job_repo = JobRunRepository(session)
            job_run = session.get(JobRun, job_run_id)
            if job_run:
                job_repo.finish(job_run, status='failed', error_summary=str(exc))
        raise

    log_event(
        logger,
        'ingest_complete',
        inserted=result.inserted,
        skipped=result.skipped,
        sources_ok=result.sources_ok,
        sources_failed=result.sources_failed,
    )
    return result
