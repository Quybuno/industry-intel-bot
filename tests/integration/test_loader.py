"""Integration test cho loader.py — Postgres thật (PRODUCTION_PLAN §20.2), không mock DB.

Dùng ingest_date riêng cho test để không đụng dữ liệu thật trong bronze/silver.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from src.intel_bot.ingest.loader import normalize_partition

TEST_INGEST_DATE = dt.date(
    2000, 1, 1
)  # ngày dành riêng cho test — không đụng dữ liệu thật
TEST_SOURCE_ID = "test_loader_source"
NOW = dt.datetime(2000, 1, 1, 12, 0, 0, tzinfo=dt.UTC)


def _payload_hash(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _insert_bronze_row(
    connection: sa.Connection, *, raw_url: str, payload: dict[str, Any]
) -> None:
    connection.execute(
        sa.text(
            """
            INSERT INTO bronze.raw_articles
                (ingest_date, source_id, source_type, raw_url, payload, payload_hash, fetched_at)
            VALUES
                (:ingest_date, :source_id, 'rss', :raw_url, :payload, :payload_hash, :fetched_at)
            ON CONFLICT (ingest_date, payload_hash) DO NOTHING
            """
        ).bindparams(sa.bindparam("payload", type_=postgresql.JSONB)),
        {
            "ingest_date": TEST_INGEST_DATE,
            "source_id": TEST_SOURCE_ID,
            "raw_url": raw_url,
            "payload": payload,
            "payload_hash": _payload_hash(payload),
            "fetched_at": NOW,
        },
    )


def _cleanup(connection: sa.Connection) -> None:
    connection.execute(
        sa.text("DELETE FROM silver.articles WHERE first_seen_date = :d"),
        {"d": TEST_INGEST_DATE},
    )
    connection.execute(
        sa.text("DELETE FROM bronze.raw_articles WHERE ingest_date = :d"),
        {"d": TEST_INGEST_DATE},
    )
    connection.commit()


@pytest.fixture()
def seeded_bronze(db_connection: sa.Connection) -> Iterator[sa.Connection]:
    """Seed 3 dòng bronze: 1 bài mới, 1 bài 10 ngày tuổi, 1 bài thiếu title (bị loại parse)."""
    _cleanup(db_connection)

    recent_payload = {
        "title": "Bài mới trong hạn",
        "link": "https://fixture.test/recent-article",
        "summary": "Snippet gần đây.",
        "published_parsed": "2000-01-01T10:00:00+00:00",
    }
    old_payload = {
        "title": "Bài quá cũ",
        "link": "https://fixture.test/old-article",
        "summary": "Snippet cũ.",
        "published_parsed": "1999-12-20T10:00:00+00:00",  # 12 ngày trước NOW
    }
    invalid_payload = {
        "link": "https://fixture.test/no-title",
        "summary": "Không có title.",
    }

    _insert_bronze_row(
        db_connection, raw_url=recent_payload["link"], payload=recent_payload
    )
    _insert_bronze_row(db_connection, raw_url=old_payload["link"], payload=old_payload)
    _insert_bronze_row(
        db_connection, raw_url=invalid_payload["link"], payload=invalid_payload
    )
    db_connection.commit()

    yield db_connection
    _cleanup(db_connection)


def test_normalize_partition_writes_silver_and_excludes_correctly(
    seeded_bronze: sa.Connection,
) -> None:
    """3 dòng bronze → 2 dòng silver (dòng thiếu title bị loại ở normalizer, không ghi)."""
    result = normalize_partition(
        seeded_bronze, ingest_date=TEST_INGEST_DATE, max_article_age_days=7, now=NOW
    )

    assert result.read == 3
    assert result.inserted == 2
    assert result.updated == 0
    assert result.excluded_by_reason.get("missing_title_or_link") == 1
    assert result.excluded_by_reason.get("too_old") == 1

    rows = seeded_bronze.execute(
        sa.text(
            "SELECT canonical_url, status, exclusion_reason FROM silver.articles "
            "WHERE first_seen_date = :d ORDER BY canonical_url"
        ),
        {"d": TEST_INGEST_DATE},
    ).all()
    assert len(rows) == 2
    by_url = {r.canonical_url: r for r in rows}
    assert by_url["https://fixture.test/old-article"].status == "excluded"
    assert by_url["https://fixture.test/old-article"].exclusion_reason == "too_old"
    assert by_url["https://fixture.test/recent-article"].status == "ingested"
    assert by_url["https://fixture.test/recent-article"].exclusion_reason is None


def test_normalize_partition_three_times_is_idempotent(
    seeded_bronze: sa.Connection,
) -> None:
    """Chạy normalize 3 lần trên cùng partition → COUNT(*) không đổi VÀ first_seen_at không đổi."""
    normalize_partition(
        seeded_bronze, ingest_date=TEST_INGEST_DATE, max_article_age_days=7, now=NOW
    )
    count_after_first, first_seen_after_first = seeded_bronze.execute(
        sa.text(
            "SELECT COUNT(*), MIN(first_seen_at) FROM silver.articles WHERE first_seen_date = :d"
        ),
        {"d": TEST_INGEST_DATE},
    ).one()

    for _ in range(2):
        normalize_partition(
            seeded_bronze, ingest_date=TEST_INGEST_DATE, max_article_age_days=7, now=NOW
        )

    count_after_third, first_seen_after_third = seeded_bronze.execute(
        sa.text(
            "SELECT COUNT(*), MIN(first_seen_at) FROM silver.articles WHERE first_seen_date = :d"
        ),
        {"d": TEST_INGEST_DATE},
    ).one()

    assert count_after_first == 2
    assert count_after_third == count_after_first
    assert first_seen_after_third == first_seen_after_first
