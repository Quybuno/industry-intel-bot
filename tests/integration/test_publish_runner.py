"""Integration test cho tầng publish — Postgres THẬT (PRODUCTION_PLAN §20.2), theo đúng
quy ước test dùng DB thật của repo (xem tests/test_score_runner.py).

`gold.mart_daily_digest` là bảng do dbt build lại toàn bộ mỗi lần chạy (không có FK ràng
buộc từ silver) — test chèn thẳng một dòng test với article_id/score_id riêng (namespace
UUID cố định), dọn dẹp ở cuối, không đụng dữ liệu thật do dbt sinh.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from src.intel_bot.publish.digest_reader import fetch_digest_rows, mark_published
from src.intel_bot.publish.runner import run_publish

TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"
TEST_SLUG = "test-publish-runner"
ARTICLE_ID = uuid.uuid5(uuid.NAMESPACE_URL, f"article/{TEST_SLUG}")
SCORE_ID = uuid.uuid5(uuid.NAMESPACE_URL, f"score/{TEST_SLUG}")
NOW = dt.datetime(2026, 1, 5, 9, 0, 0, tzinfo=dt.UTC)


def _insert_silver_article(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO silver.articles (
                article_id, canonical_url, raw_url, content_hash, source_id, title, snippet,
                published_at, first_seen_at, first_seen_date, status, published_at_imputed
            ) VALUES (
                :article_id, :url, :url, :hash, 'test_publish_source', 'Tiêu đề test publish',
                'Snippet test publish', :now, :now, :day, 'scored', false
            )
            ON CONFLICT (canonical_url) DO NOTHING
            """
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {
            "article_id": ARTICLE_ID,
            "url": f"https://fixture.test/{TEST_SLUG}",
            "hash": f"hash-{TEST_SLUG}",
            "now": NOW,
            "day": NOW.date(),
        },
    )


def _insert_gold_digest_row(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO gold.mart_daily_digest (
                score_id, article_id, canonical_url, title, snippet, industry_tags,
                source_id, source_domain, source_tier, published_at, published_at_imputed,
                first_seen_at, credibility_blended, importance, practicality, depth,
                recency_boost, composite_score, summary_vi, why_it_matters_vi,
                industry_group, digest_built_at
            ) VALUES (
                :score_id, :article_id, :url, 'Tiêu đề test publish', 'Snippet test publish',
                ARRAY['ai'], 'test_publish_source', 'fixture.test', 1, :now, false, :now,
                8.4, 8, 6, 5, 0.5, 7.42, :summary_vi, 'Lý do quan trọng test.', 'ai', :now
            )
            """
        ).bindparams(
            sa.bindparam("score_id", type_=postgresql.UUID),
            sa.bindparam("article_id", type_=postgresql.UUID),
            sa.bindparam("summary_vi", type_=postgresql.JSONB),
        ),
        {
            "score_id": SCORE_ID,
            "article_id": ARTICLE_ID,
            "url": f"https://fixture.test/{TEST_SLUG}",
            "now": NOW,
            "summary_vi": [f"Bullet {i}" for i in range(5)],
        },
    )


def _cleanup(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            "DELETE FROM gold.mart_daily_digest WHERE score_id = :score_id"
        ).bindparams(sa.bindparam("score_id", type_=postgresql.UUID)),
        {"score_id": SCORE_ID},
    )
    connection.execute(
        sa.text(
            "DELETE FROM silver.articles WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": ARTICLE_ID},
    )
    connection.commit()


@pytest.fixture()
def fixture_digest_row(db_connection: sa.Connection) -> Iterator[None]:
    _cleanup(db_connection)
    _insert_silver_article(db_connection)
    _insert_gold_digest_row(db_connection)
    db_connection.commit()
    yield
    _cleanup(db_connection)


def test_fetch_digest_rows_reads_inserted_row(
    db_connection: sa.Connection, fixture_digest_row: None
) -> None:
    rows = fetch_digest_rows(db_connection)
    matching = [r for r in rows if r.article_id == ARTICLE_ID]
    assert len(matching) == 1
    row = matching[0]
    assert row.score_id == SCORE_ID
    assert row.title == "Tiêu đề test publish"
    assert len(row.summary_vi) == 5
    assert row.industry_group == "ai"


def test_mark_published_updates_last_published_at_only_for_given_ids(
    db_connection: sa.Connection, fixture_digest_row: None
) -> None:
    published_at = dt.datetime(2026, 1, 5, 10, 0, 0, tzinfo=dt.UTC)
    updated = mark_published(
        db_connection, article_ids=[ARTICLE_ID], published_at=published_at
    )
    db_connection.commit()
    assert updated == 1

    stored = db_connection.execute(
        sa.text(
            "SELECT last_published_at FROM silver.articles WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": ARTICLE_ID},
    ).scalar_one()
    assert stored == published_at


def test_mark_published_with_empty_list_is_noop(db_connection: sa.Connection) -> None:
    assert mark_published(db_connection, article_ids=[], published_at=NOW) == 0


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_run_publish_twice_produces_identical_files(
    db_connection: sa.Connection, fixture_digest_row: None, tmp_path: Path
) -> None:
    """DONE WHEN: chạy publish 2 lần (kể cả với `now` khác nhau, xem docstring runner.py)
    → articles.json và index.html giống hệt nhau theo hash."""
    docs_site_dir = tmp_path / "docs-site"

    result_1 = run_publish(
        db_connection,
        generated_for_date=dt.date(2026, 1, 5),
        docs_site_dir=docs_site_dir,
        templates_dir=TEMPLATES_DIR,
        repo_url="https://github.com/quybuno/industry-intel-bot",
        now=dt.datetime(2026, 1, 5, 5, 0, 0, tzinfo=dt.UTC),
    )
    hash_html_1 = _sha256(result_1.index_html_path)
    hash_json_1 = _sha256(result_1.articles_json_path)
    hash_archive_1 = _sha256(result_1.archive_json_path)

    result_2 = run_publish(
        db_connection,
        generated_for_date=dt.date(2026, 1, 5),
        docs_site_dir=docs_site_dir,
        templates_dir=TEMPLATES_DIR,
        repo_url="https://github.com/quybuno/industry-intel-bot",
        now=dt.datetime(
            2026, 1, 5, 18, 30, 0, tzinfo=dt.UTC
        ),  # now KHÁC lần 1 có chủ đích
    )
    hash_html_2 = _sha256(result_2.index_html_path)
    hash_json_2 = _sha256(result_2.articles_json_path)
    hash_archive_2 = _sha256(result_2.archive_json_path)

    assert hash_html_1 == hash_html_2
    assert hash_json_1 == hash_json_2
    assert hash_archive_1 == hash_archive_2
    assert result_1.article_count == result_2.article_count >= 1


def test_run_publish_writes_valid_json_and_archive_matches_articles(
    db_connection: sa.Connection, fixture_digest_row: None, tmp_path: Path
) -> None:
    docs_site_dir = tmp_path / "docs-site"
    result = run_publish(
        db_connection,
        generated_for_date=dt.date(2026, 1, 5),
        docs_site_dir=docs_site_dir,
        templates_dir=TEMPLATES_DIR,
        repo_url="https://github.com/quybuno/industry-intel-bot",
        now=NOW,
    )

    assert result.archive_json_path == docs_site_dir / "archive" / "2026-01-05.json"
    with result.articles_json_path.open(encoding="utf-8") as fh:
        articles_payload = json.load(fh)
    with result.archive_json_path.open(encoding="utf-8") as fh:
        archive_payload = json.load(fh)
    assert articles_payload == archive_payload
    assert articles_payload["article_count"] >= 1
    assert result.index_html_path.exists()
    assert result.articles_marked_published >= 1
