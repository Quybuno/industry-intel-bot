"""Test cho `src/intel_bot/ingest/github_fetcher.py` (task 1.2).

Unit test cho hàm thuần (`repo_to_payload`, `is_rate_limited_response`) — không mạng, không
DB. Integration test: fixture JSON GitHub Search → bronze.raw_articles, KHÔNG gọi mạng thật
(`httpx.MockTransport`), có gọi Postgres thật (PRODUCTION_PLAN §20.2), theo đúng khuôn
`tests/test_rss_ingest.py`.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import sqlalchemy as sa

from src.intel_bot.db.bronze import count_raw_articles
from src.intel_bot.ingest.github_fetcher import (
    GITHUB_API_BASE_URL,
    GithubQueryConfig,
    fetch_all_queries,
    is_rate_limited_response,
    repo_to_payload,
)
from src.intel_bot.ingest.normalizer import parse_entry

TEST_INGEST_DATE = dt.date(
    2000, 1, 2
)  # ngày dành riêng cho test — khác test_rss_ingest.py (2000-01-01), không đụng nhau
TEST_SOURCE_ID = "test_fixture_github_source"

#: 2 repo mẫu — hình dạng THẬT của GitHub Search API `/search/repositories` (đã verify thật
#: qua api.github.com ngày 2026-08-12, chỉ giữ lại các trường liên quan để fixture gọn).
SAMPLE_REPO_ITEMS: list[dict[str, Any]] = [
    {
        "full_name": "vllm-project/vllm",
        "html_url": "https://github.com/vllm-project/vllm",
        "description": "A high-throughput and memory-efficient inference engine for LLMs",
        "pushed_at": "2026-08-12T08:07:07Z",
        "stargazers_count": 88843,
        "topics": ["llm", "inference"],
    },
    {
        "full_name": "huggingface/transformers",
        "html_url": "https://github.com/huggingface/transformers",
        "description": "Transformers: the model-definition framework",
        "pushed_at": "2026-08-12T08:06:30Z",
        "stargazers_count": 163927,
        "topics": ["llm", "nlp"],
    },
]


def _mock_transport(
    *, status_code: int = 200, headers: dict[str, str] | None = None
) -> httpx.MockTransport:
    """Transport giả lập luôn trả về SAMPLE_REPO_ITEMS — không có request mạng thật nào."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={"total_count": len(SAMPLE_REPO_ITEMS), "items": SAMPLE_REPO_ITEMS},
            headers=headers or {},
        )

    return httpx.MockTransport(handler)


def _cleanup(connection: sa.Connection) -> None:
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
def fixture_query() -> list[GithubQueryConfig]:
    return [
        GithubQueryConfig(
            source_id=TEST_SOURCE_ID,
            query="topic:llm stars:>200",
            industries=["ai"],
            is_enabled=True,
        )
    ]


@pytest.fixture()
def clean_test_partition(db_connection: sa.Connection) -> Iterator[sa.Connection]:
    _cleanup(db_connection)
    yield db_connection
    _cleanup(db_connection)


# ---------------------------------------------------------------------------
# Unit test — hàm thuần
# ---------------------------------------------------------------------------


def test_repo_to_payload_adds_aliases_without_dropping_original_fields() -> None:
    """payload gốc GIỮ NGUYÊN mọi trường GitHub trả về (P3), chỉ THÊM alias."""
    item = SAMPLE_REPO_ITEMS[0]
    payload = repo_to_payload(item)

    # Alias mới cho parse_entry() đọc được.
    assert payload["title"] == "vllm-project/vllm"
    assert payload["link"] == "https://github.com/vllm-project/vllm"
    assert payload["summary"] == item["description"]
    assert payload["updated_parsed"] == "2026-08-12T08:07:07Z"

    # Trường gốc không bị mất/đổi.
    assert payload["full_name"] == item["full_name"]
    assert payload["stargazers_count"] == 88843
    assert payload["topics"] == ["llm", "inference"]


def test_repo_to_payload_handles_missing_description() -> None:
    """`description` None (GitHub trả None cho repo chưa điền mô tả) → summary rỗng, không lỗi."""
    item = {**SAMPLE_REPO_ITEMS[0], "description": None}
    payload = repo_to_payload(item)
    assert payload["summary"] == ""


def test_repo_to_payload_is_parseable_by_shared_normalizer_without_branch() -> None:
    """Payload đã shape qua `repo_to_payload()` phải được `parse_entry()` (dùng chung với
    RSS, KHÔNG có nhánh riêng cho github) đọc đúng title/link/snippet/published_at."""
    payload = repo_to_payload(SAMPLE_REPO_ITEMS[1])
    parsed = parse_entry(payload)

    assert parsed is not None
    assert parsed.title == "huggingface/transformers"
    assert parsed.link == "https://github.com/huggingface/transformers"
    assert parsed.snippet == "Transformers: the model-definition framework"
    assert parsed.published_at == dt.datetime(2026, 8, 12, 8, 6, 30, tzinfo=dt.UTC)


def test_is_rate_limited_response_true_for_403_and_429() -> None:
    assert is_rate_limited_response(httpx.Response(403, json={"message": "rate limit"}))
    assert is_rate_limited_response(httpx.Response(429, json={"message": "too many"}))


def test_is_rate_limited_response_true_when_remaining_header_zero() -> None:
    response = httpx.Response(200, json={}, headers={"X-RateLimit-Remaining": "0"})
    assert is_rate_limited_response(response)


def test_is_rate_limited_response_false_for_normal_200() -> None:
    response = httpx.Response(200, json={}, headers={"X-RateLimit-Remaining": "29"})
    assert not is_rate_limited_response(response)


# ---------------------------------------------------------------------------
# Integration test — fixture JSON -> bronze thật (Postgres thật, mạng giả)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_all_queries_writes_bronze_from_fixture(
    clean_test_partition: sa.Connection, fixture_query: list[GithubQueryConfig]
) -> None:
    """Fixture 2 repo → 2 dòng mới trong bronze.raw_articles, source_type='github'."""
    async with httpx.AsyncClient(
        transport=_mock_transport(), base_url=GITHUB_API_BASE_URL
    ) as client:
        result = await fetch_all_queries(
            clean_test_partition, client, fixture_query, ingest_date=TEST_INGEST_DATE
        )

    assert result.total_entries_fetched == 2
    assert result.rows_inserted == 2
    assert result.sources_ok == 1
    assert result.failed_sources == []
    assert result.rate_limited is False
    assert count_raw_articles(clean_test_partition, ingest_date=TEST_INGEST_DATE) == 2

    row = clean_test_partition.execute(
        sa.text(
            "SELECT source_type, source_id, raw_url FROM bronze.raw_articles "
            "WHERE ingest_date = :d ORDER BY id LIMIT 1"
        ),
        {"d": TEST_INGEST_DATE},
    ).one()
    assert row.source_type == "github"
    assert row.source_id == TEST_SOURCE_ID
    assert row.raw_url == "https://github.com/vllm-project/vllm"


@pytest.mark.asyncio
async def test_ingest_twice_same_day_is_idempotent(
    clean_test_partition: sa.Connection, fixture_query: list[GithubQueryConfig]
) -> None:
    """Chạy ingest cùng partition 2 lần → số dòng bronze không đổi (P1)."""
    async with httpx.AsyncClient(
        transport=_mock_transport(), base_url=GITHUB_API_BASE_URL
    ) as client:
        await fetch_all_queries(
            clean_test_partition, client, fixture_query, ingest_date=TEST_INGEST_DATE
        )
    first_count = count_raw_articles(clean_test_partition, ingest_date=TEST_INGEST_DATE)

    async with httpx.AsyncClient(
        transport=_mock_transport(), base_url=GITHUB_API_BASE_URL
    ) as client:
        result_second = await fetch_all_queries(
            clean_test_partition, client, fixture_query, ingest_date=TEST_INGEST_DATE
        )
    second_count = count_raw_articles(
        clean_test_partition, ingest_date=TEST_INGEST_DATE
    )

    assert first_count == 2
    assert second_count == first_count
    assert result_second.rows_inserted == 0


@pytest.mark.asyncio
async def test_rate_limited_response_stops_cleanly_without_raising(
    clean_test_partition: sa.Connection,
) -> None:
    """Vượt hạn mức (403) ở truy vấn đầu → dừng sạch, KHÔNG raise, không chạy truy vấn thứ
    hai, `rate_limited=True` để caller (asset Dagster) log rõ (P4)."""
    queries = [
        GithubQueryConfig(
            source_id="q1",
            query="topic:llm stars:>200",
            industries=["ai"],
            is_enabled=True,
        ),
        GithubQueryConfig(
            source_id="q2",
            query="topic:iot stars:>200",
            industries=["iot"],
            is_enabled=True,
        ),
    ]
    transport = _mock_transport(status_code=403, headers={"X-RateLimit-Remaining": "0"})
    async with httpx.AsyncClient(
        transport=transport, base_url=GITHUB_API_BASE_URL
    ) as client:
        result = await fetch_all_queries(
            clean_test_partition, client, queries, ingest_date=TEST_INGEST_DATE
        )

    assert result.rate_limited is True
    assert result.rows_inserted == 0
    assert result.sources_ok == 0
    assert result.failed_sources == ["q1"]  # q2 không hề chạy — dừng ngay sau q1
    assert count_raw_articles(clean_test_partition, ingest_date=TEST_INGEST_DATE) == 0


@pytest.mark.asyncio
async def test_one_query_error_does_not_abort_other_queries(
    clean_test_partition: sa.Connection,
) -> None:
    """Một truy vấn lỗi thường (không phải rate limit, vd. 422 query sai cú pháp) không làm
    hỏng các truy vấn khác trong cùng lần chạy — khác hành vi dừng sạch của rate limit."""
    queries = [
        GithubQueryConfig(
            source_id="bad_query", query="((invalid", industries=["ai"], is_enabled=True
        ),
        GithubQueryConfig(
            source_id="good_query",
            query="topic:llm stars:>200",
            industries=["ai"],
            is_enabled=True,
        ),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        query_param = request.url.params.get("q", "")
        if query_param == "((invalid":
            return httpx.Response(422, json={"message": "Validation Failed"})
        return httpx.Response(
            200,
            json={"total_count": len(SAMPLE_REPO_ITEMS), "items": SAMPLE_REPO_ITEMS},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=GITHUB_API_BASE_URL
    ) as client:
        result = await fetch_all_queries(
            clean_test_partition, client, queries, ingest_date=TEST_INGEST_DATE
        )

    assert result.rate_limited is False
    assert result.failed_sources == ["bad_query"]
    assert result.sources_ok == 1
    assert result.rows_inserted == 2
