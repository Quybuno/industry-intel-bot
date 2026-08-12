"""Integration test: fixture RSS XML → bronze.raw_articles. KHÔNG gọi mạng thật
(dùng httpx.MockTransport), có gọi Postgres thật (PRODUCTION_PLAN §20.2).
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa

from src.intel_bot.db.bronze import count_raw_articles
from src.intel_bot.ingest.rss_fetcher import SourceConfig, fetch_all_sources

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "rss" / "sample_valid.xml"
TEST_INGEST_DATE = dt.date(
    2000, 1, 1
)  # ngày dành riêng cho test — không đụng dữ liệu thật
TEST_SOURCE_ID = "test_fixture_source"


def _mock_transport() -> httpx.MockTransport:
    """Transport giả lập luôn trả về fixture XML — không có request mạng thật nào xảy ra."""
    fixture_bytes = FIXTURE_PATH.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=fixture_bytes, headers={"ETag": '"fixture-etag-v1"'}
        )

    return httpx.MockTransport(handler)


def _cleanup(connection: sa.Connection) -> None:
    """Xoá sạch dữ liệu test theo TEST_INGEST_DATE — chạy trước và sau mỗi test."""
    connection.execute(
        sa.text("DELETE FROM bronze.raw_articles WHERE ingest_date = :d"),
        {"d": TEST_INGEST_DATE},
    )
    connection.execute(
        sa.text("DELETE FROM silver.source_health WHERE fetch_date = :d"),
        {"d": TEST_INGEST_DATE},
    )
    connection.commit()


@pytest.fixture()
def fixture_source() -> list[SourceConfig]:
    return [
        SourceConfig(
            source_id=TEST_SOURCE_ID,
            url="https://fixture.test/feed.xml",
            domain="fixture.test",
            tier=1,
            industries=["ai"],
            is_enabled=True,
        )
    ]


@pytest.fixture()
def clean_test_partition(db_connection: sa.Connection) -> Iterator[sa.Connection]:
    _cleanup(db_connection)
    yield db_connection
    _cleanup(db_connection)


@pytest.mark.asyncio
async def test_fetch_all_sources_writes_bronze_from_fixture(
    clean_test_partition: sa.Connection, fixture_source: list[SourceConfig]
) -> None:
    """Fixture RSS 3 entry → 3 dòng mới trong bronze.raw_articles, đếm row đúng."""
    async with httpx.AsyncClient(transport=_mock_transport()) as client:
        result = await fetch_all_sources(
            clean_test_partition, client, fixture_source, ingest_date=TEST_INGEST_DATE
        )

    assert result.total_entries_fetched == 3
    assert result.rows_inserted == 3
    assert result.sources_ok == 1
    assert result.failed_sources == []
    assert count_raw_articles(clean_test_partition, ingest_date=TEST_INGEST_DATE) == 3


@pytest.mark.asyncio
async def test_ingest_twice_same_day_is_idempotent(
    clean_test_partition: sa.Connection, fixture_source: list[SourceConfig]
) -> None:
    """Chạy ingest cùng partition (ingest_date) 2 lần → số dòng bronze không đổi (P1)."""
    async with httpx.AsyncClient(transport=_mock_transport()) as client:
        await fetch_all_sources(
            clean_test_partition, client, fixture_source, ingest_date=TEST_INGEST_DATE
        )
    first_count = count_raw_articles(clean_test_partition, ingest_date=TEST_INGEST_DATE)

    async with httpx.AsyncClient(transport=_mock_transport()) as client:
        result_second = await fetch_all_sources(
            clean_test_partition, client, fixture_source, ingest_date=TEST_INGEST_DATE
        )
    second_count = count_raw_articles(
        clean_test_partition, ingest_date=TEST_INGEST_DATE
    )

    assert first_count == 3
    assert second_count == first_count
    # Payload y hệt lần 1 → cùng payload_hash → ON CONFLICT DO NOTHING, không dòng mới.
    assert result_second.rows_inserted == 0
