"""Integration test cho filter/loader.py — Postgres thật (PRODUCTION_PLAN §20.2), không mock DB.

Dùng first_seen_date riêng cho test để không đụng dữ liệu thật trong silver.articles.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from src.intel_bot.filter.keyword_filter import FilterRules
from src.intel_bot.filter.loader import run_filter_partition

TEST_FILTER_DATE = dt.date(2000, 1, 2)  # ngày riêng cho test — không đụng dữ liệu thật
NOW = dt.datetime(2000, 1, 2, 12, 0, 0, tzinfo=dt.UTC)

RULES = FilterRules(
    max_article_age_days=7,
    min_snippet_chars=80,
    blocklist_keywords=("webinar", "sponsored"),
    max_articles_per_day=2,
    now=NOW,
)

LONG_SNIPPET = (
    "Đây là snippet đủ dài để không bị loại bởi quy tắc snippet_too_short. " * 2
)


def _insert_article(
    connection: sa.Connection,
    *,
    slug: str,
    title: str = "Tiêu đề bình thường",
    snippet: str = LONG_SNIPPET,
    published_at: dt.datetime | None,
) -> uuid.UUID:
    article_id = uuid.uuid5(uuid.NAMESPACE_URL, f"https://fixture.test/filter/{slug}")
    connection.execute(
        sa.text(
            """
            INSERT INTO silver.articles (
                article_id, canonical_url, raw_url, content_hash, source_id, title, snippet,
                published_at, first_seen_at, first_seen_date, status, published_at_imputed
            ) VALUES (
                :article_id, :canonical_url, :canonical_url, :content_hash, 'test_filter_source',
                :title, :snippet, :published_at, :first_seen_at, :first_seen_date, 'ingested', false
            )
            """
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {
            "article_id": article_id,
            "canonical_url": f"https://fixture.test/filter/{slug}",
            "content_hash": f"hash-{slug}",
            "title": title,
            "snippet": snippet,
            "published_at": published_at,
            "first_seen_at": NOW,
            "first_seen_date": TEST_FILTER_DATE,
        },
    )
    return article_id


def _cleanup(connection: sa.Connection) -> None:
    connection.execute(
        sa.text("DELETE FROM silver.articles WHERE first_seen_date = :d"),
        {"d": TEST_FILTER_DATE},
    )
    connection.commit()


@pytest.fixture()
def seeded_articles(db_connection: sa.Connection) -> Iterator[dict[str, uuid.UUID]]:
    """Seed 4 bài: 1 pass, 1 too_old, 1 snippet_too_short, 1 keyword_blocked."""
    _cleanup(db_connection)

    ids = {
        "ok": _insert_article(
            db_connection, slug="ok", published_at=NOW - dt.timedelta(hours=1)
        ),
        "old": _insert_article(
            db_connection, slug="old", published_at=NOW - dt.timedelta(days=10)
        ),
        "short": _insert_article(
            db_connection,
            slug="short",
            snippet="quá ngắn",
            published_at=NOW - dt.timedelta(hours=2),
        ),
        "blocked": _insert_article(
            db_connection,
            slug="blocked",
            title="Free webinar this Friday",
            published_at=NOW - dt.timedelta(hours=3),
        ),
    }
    db_connection.commit()

    yield ids
    _cleanup(db_connection)


def test_run_filter_partition_writes_expected_status_and_reasons(
    db_connection: sa.Connection, seeded_articles: dict[str, uuid.UUID]
) -> None:
    result = run_filter_partition(
        db_connection, filter_date=TEST_FILTER_DATE, rules=RULES
    )

    assert result.read == 4
    assert result.eligible == 1
    assert result.excluded == 3
    assert result.excluded_by_reason == {
        "too_old": 1,
        "snippet_too_short": 1,
        "keyword_blocked": 1,
    }

    rows = db_connection.execute(
        sa.text(
            "SELECT article_id, status, exclusion_reason, filter_score FROM silver.articles "
            "WHERE first_seen_date = :d"
        ),
        {"d": TEST_FILTER_DATE},
    ).all()
    by_id = {r.article_id: r for r in rows}

    assert by_id[seeded_articles["ok"]].status == "eligible"
    assert by_id[seeded_articles["ok"]].filter_score == 1.0
    assert by_id[seeded_articles["old"]].status == "excluded"
    assert by_id[seeded_articles["old"]].exclusion_reason == "too_old"
    assert by_id[seeded_articles["short"]].exclusion_reason == "snippet_too_short"
    assert by_id[seeded_articles["blocked"]].exclusion_reason == "keyword_blocked"


def test_run_filter_partition_applies_daily_cap_last(
    db_connection: sa.Connection,
) -> None:
    """RULES.max_articles_per_day=2: 3 bài pass 3 quy tắc đầu → chỉ 2 mới nhất được eligible."""
    _cleanup(db_connection)
    ids = [
        _insert_article(
            db_connection, slug=f"cap-{i}", published_at=NOW - dt.timedelta(hours=i)
        )
        for i in range(3)
    ]
    db_connection.commit()

    result = run_filter_partition(
        db_connection, filter_date=TEST_FILTER_DATE, rules=RULES
    )

    assert result.eligible == 2
    assert result.excluded_by_reason == {"over_daily_cap": 1}

    rows = db_connection.execute(
        sa.text(
            "SELECT article_id, status FROM silver.articles WHERE article_id = ANY(:ids)"
        ),
        {"ids": ids},
    ).all()
    by_id = {r.article_id: r.status for r in rows}
    assert by_id[ids[0]] == "eligible"  # mới nhất (0h trước)
    assert by_id[ids[1]] == "eligible"  # 1h trước
    assert by_id[ids[2]] == "excluded"  # cũ nhất (2h trước) — bị cap loại

    _cleanup(db_connection)


def test_run_filter_partition_three_times_is_idempotent(
    db_connection: sa.Connection, seeded_articles: dict[str, uuid.UUID]
) -> None:
    """Chạy filter 3 lần trên cùng partition → status/filter_score/exclusion_reason không đổi."""
    run_filter_partition(db_connection, filter_date=TEST_FILTER_DATE, rules=RULES)
    snapshot_1 = db_connection.execute(
        sa.text(
            "SELECT article_id, status, exclusion_reason, filter_score FROM silver.articles "
            "WHERE first_seen_date = :d ORDER BY article_id"
        ),
        {"d": TEST_FILTER_DATE},
    ).all()

    for _ in range(2):
        run_filter_partition(db_connection, filter_date=TEST_FILTER_DATE, rules=RULES)

    snapshot_3 = db_connection.execute(
        sa.text(
            "SELECT article_id, status, exclusion_reason, filter_score FROM silver.articles "
            "WHERE first_seen_date = :d ORDER BY article_id"
        ),
        {"d": TEST_FILTER_DATE},
    ).all()

    assert snapshot_1 == snapshot_3
