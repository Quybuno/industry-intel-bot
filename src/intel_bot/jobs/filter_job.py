"""Filter job: keyword + embedding stages on raw articles."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from src.intel_bot.config import load_yaml
from src.intel_bot.db.models import JobRun, Source
from src.intel_bot.db.repositories import ArticleRepository, JobRunRepository
from src.intel_bot.db.session import ensure_tables, get_session
from src.intel_bot.filter.embedding_filter import EmbeddingFilter
from src.intel_bot.filter.legacy_keyword_filter import keyword_pass, load_keyword_groups
from src.intel_bot.observability.logging import log_event, setup_logging

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    processed: int = 0
    filtered: int = 0
    rejected: int = 0
    keyword_rejected: int = 0
    embedding_rejected: int = 0
    errors: int = 0
    rejection_reasons: dict[str, int] = field(default_factory=dict)


def load_filter_config(app_path: str = "config/app.yaml") -> dict[str, Any]:
    return load_yaml(app_path).get("filter", {})


def _build_source_industries_map(session) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for src in session.query(Source).all():
        mapping[src.id] = src.industries or []
    if mapping:
        return mapping

    data = load_yaml("config/sources.yaml")
    for s in data.get("sources", []):
        mapping[s["id"]] = s.get("industries", [])
    return mapping


def _article_text(article) -> str:
    title = article.title or ""
    snippet = article.snippet or ""
    return f"{title}. {snippet}".strip(". ")


def run_filter_job(
    *,
    limit: Optional[int] = None,
    app_path: str = "config/app.yaml",
) -> FilterResult:
    """
    Filter pipeline per PRODUCTION_PLAN §7.3:
    raw → keyword → embedding → filtered | rejected.
    """
    setup_logging()
    ensure_tables()

    cfg = load_filter_config(app_path)
    keywords_path = cfg.get("keywords_path", "config/keywords.yaml")
    profile_path = cfg.get("interest_profile_path", "config/interest_profile.txt")
    batch_size = limit or cfg.get("batch_size", 500)

    keyword_groups = load_keyword_groups(load_yaml(keywords_path))
    embedder = EmbeddingFilter(
        profile_path,
        threshold=cfg.get("embedding_threshold", 0.35),
        fallback_threshold=cfg.get("embedding_fallback_threshold", 0.15),
        model_name=cfg.get("embedding_model", "BAAI/bge-small-en-v1.5"),
    )

    result = FilterResult()

    with get_session() as session:
        job_repo = JobRunRepository(session)
        job_run = job_repo.start("filter", metadata={"batch_size": batch_size})
        job_run_id = job_run.id
        session.commit()

    try:
        with get_session() as session:
            article_repo = ArticleRepository(session)
            job_repo = JobRunRepository(session)
            source_industries = _build_source_industries_map(session)

            articles = article_repo.list_raw(limit=batch_size)
            logger.info("Filtering %d raw articles", len(articles))

            for article in articles:
                result.processed += 1
                try:
                    text = _article_text(article)
                    src_inds = source_industries.get(article.source_id or "", [])

                    kw_ok, matched_groups, kw_reason = keyword_pass(
                        text, src_inds, keyword_groups
                    )
                    if not kw_ok:
                        article_repo.set_rejected(article, kw_reason or "keyword_miss")
                        result.rejected += 1
                        result.keyword_rejected += 1
                        result.rejection_reasons[kw_reason or "keyword_miss"] = (
                            result.rejection_reasons.get(kw_reason or "keyword_miss", 0)
                            + 1
                        )
                        continue

                    emb_ok, score, mode = embedder.passes(text)
                    if not emb_ok:
                        reason = f"embedding_low:{score:.3f}"
                        article_repo.set_rejected(article, reason)
                        result.rejected += 1
                        result.embedding_rejected += 1
                        result.rejection_reasons["embedding_low"] = (
                            result.rejection_reasons.get("embedding_low", 0) + 1
                        )
                        logger.debug(
                            "Rejected embedding_low id=%s score=%.3f mode=%s",
                            article.id,
                            score,
                            mode,
                        )
                        continue

                    tags = list(dict.fromkeys(matched_groups))
                    article_repo.set_filtered(article, tags)
                    result.filtered += 1

                except Exception as exc:
                    result.errors += 1
                    logger.warning("Filter error for article %s: %s", article.id, exc)

            status = "success"
            if result.errors > 0 and result.filtered > 0:
                status = "partial"
            elif result.errors > 0 and result.filtered == 0:
                status = "failed"

            job_run = session.get(JobRun, job_run_id)
            job_repo.finish(
                job_run,
                status=status,
                items_processed=result.processed,
                items_failed=result.rejected + result.errors,
                metadata={
                    "filtered": result.filtered,
                    "rejected": result.rejected,
                    "keyword_rejected": result.keyword_rejected,
                    "embedding_rejected": result.embedding_rejected,
                    "errors": result.errors,
                    "rejection_reasons": result.rejection_reasons,
                },
            )
            session.commit()

    except Exception as exc:
        logger.exception("Filter job failed")
        with get_session() as session:
            job_repo = JobRunRepository(session)
            job_run = session.get(JobRun, job_run_id)
            if job_run:
                job_repo.finish(job_run, status="failed", error_summary=str(exc))
        raise

    log_event(
        logger,
        "filter_complete",
        processed=result.processed,
        filtered=result.filtered,
        rejected=result.rejected,
    )
    return result
