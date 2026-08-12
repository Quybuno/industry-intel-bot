"""GitHub Search fetcher — httpx.AsyncClient + endpoint `/search/repositories`, ghi thẳng
vào bronze.raw_articles (task 1.2, PRODUCTION_PLAN §4.1, §7.2, §8.1).

Kiến trúc v2 viết mới hoàn toàn — KHÔNG liên quan `github_fetcher.py`/
`github_trending_fetcher.py` v1 (đã xoá ở D3, xem docs/PROGRESS.md mục 4/10).

Rate limit đã verify thật qua docs.github.com (2026-08-12, không đoán):
- Authenticated (PAT): 30 request/phút cho mọi endpoint search TRỪ `/search/code`.
- `/search/code`: 10 request/phút (bắt buộc xác thực) — module này KHÔNG dùng endpoint này.
- Unauthenticated: 10 request/phút.
Nguồn: https://docs.github.com/en/rest/search/search
       https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api

Không lọc, không chuẩn hoá payload theo nghiệp vụ (việc của `normalize_partition()`).
Lỗi bắt theo từng truy vấn — một truy vấn lỗi không làm hỏng job; vượt hạn mức (403/429,
hoặc `X-RateLimit-Remaining` về 0) dừng sạch phần còn lại của lần chạy, KHÔNG raise (P4).

Payload ghi vào bronze GIỮ NGUYÊN toàn bộ trường gốc GitHub trả về (P3 — "nguyên vẹn"),
CHỈ THÊM 4 khoá alias (`title`/`link`/`summary`/`updated_parsed`) để
`src.intel_bot.ingest.normalizer.parse_entry()` đọc được mà KHÔNG cần nhánh riêng cho
nguồn github — cùng tinh thần feedparser tự có cặp `published`/`published_parsed` song song
(entry RSS gốc cũng không "thuần" 100% byte gốc, đã được `entry_to_payload()` chuẩn hoá
kiểu dữ liệu từ task 0.4). Xem `repo_to_payload()`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import sqlalchemy as sa
import yaml

from src.intel_bot.db.bronze import insert_raw_article, upsert_source_health
from src.intel_bot.ingest.rss_fetcher import compute_payload_hash

logger = logging.getLogger(__name__)

SOURCE_TYPE = "github"

#: Base URL của GitHub REST API — endpoint search cụ thể ghép vào khi gọi.
GITHUB_API_BASE_URL = "https://api.github.com"

#: Verify thật qua https://docs.github.com/en/rest/about-the-rest-api/api-versions
#: (2026-08-12): bản mới nhất "2026-03-10" (bản cũ "2022-11-28" là mặc định nếu bỏ header
#: này — không phải giá trị đoán, xem docstring module). Đây là hằng số kỹ thuật của giao
#: thức HTTP (giống Accept header), không thuộc danh sách cấm hardcode ở AGENTS.md mục 3
#: (tên model LLM, bảng giá, ngưỡng lọc, URL nguồn, số ngày cửa sổ, trần số bài/ngày).
GITHUB_API_VERSION = "2026-03-10"

#: HTTP status coi là "vượt hạn mức" (primary hoặc secondary rate limit của GitHub) —
#: dừng sạch phần truy vấn còn lại của lần chạy, không raise (P4).
_RATE_LIMIT_STATUS_CODES = frozenset({403, 429})


@dataclass(frozen=True)
class GithubQueryConfig:
    """Một truy vấn GitHub Search đọc từ `config/github_sources.yaml`."""

    source_id: str
    query: str
    industries: list[str]
    is_enabled: bool


@dataclass
class GithubFetchOutcome:
    """Kết quả fetch một truy vấn — dữ liệu trung gian trước khi ghi DB."""

    query_config: GithubQueryConfig
    http_status: int | None
    entries: list[dict[str, Any]]
    error: str | None
    rate_limited: bool = False


@dataclass
class GithubIngestResult:
    """Kết quả tổng hợp một lần chạy ingest GitHub."""

    total_entries_fetched: int = 0
    rows_inserted: int = 0
    sources_ok: int = 0
    failed_sources: list[str] = field(default_factory=list)
    #: True nếu lần chạy dừng SỚM giữa chừng vì vượt hạn mức (P4) — còn truy vấn chưa chạy.
    rate_limited: bool = False


def load_github_query_configs(
    path: str = "config/github_sources.yaml", *, only_enabled: bool = True
) -> list[GithubQueryConfig]:
    """Đọc danh sách truy vấn GitHub Search từ file YAML — không hardcode từ khoá/topic."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    configs = []
    for item in raw.get("queries", []):
        is_enabled = bool(item.get("is_enabled", True))
        if only_enabled and not is_enabled:
            continue
        configs.append(
            GithubQueryConfig(
                source_id=item["source_id"],
                query=item["query"],
                industries=list(item.get("industries", [])),
                is_enabled=is_enabled,
            )
        )
    return configs


def repo_to_payload(item: dict[str, Any]) -> dict[str, Any]:
    """Gắn thêm alias title/link/summary/updated_parsed lên nguyên văn object repo GitHub
    trả về — xem giải thích đầy đủ ở docstring module. KHÔNG bớt trường nào của payload gốc.
    """
    payload = dict(item)
    payload["title"] = str(item.get("full_name") or "")
    payload["link"] = str(item.get("html_url") or "")
    payload["summary"] = str(item.get("description") or "")
    pushed_at = item.get("pushed_at")
    if isinstance(pushed_at, str) and pushed_at:
        payload["updated_parsed"] = pushed_at
    return payload


def is_rate_limited_response(response: httpx.Response) -> bool:
    """True nếu response báo hiệu vượt hạn mức — primary (403 + X-RateLimit-Remaining=0)
    hoặc secondary (403/429, GitHub không luôn kèm header remaining cho loại này)."""
    if response.status_code in _RATE_LIMIT_STATUS_CODES:
        return True
    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining is not None:
        try:
            return int(remaining) <= 0
        except ValueError:
            return False
    return False


async def _fetch_one_query(
    client: httpx.AsyncClient,
    query_config: GithubQueryConfig,
    *,
    per_page: int,
    timeout: float,
) -> GithubFetchOutcome:
    """Fetch + parse một truy vấn. Không bao giờ raise ra ngoài — lỗi gói vào `outcome.error`."""
    try:
        response = await client.get(
            "/search/repositories",
            params={
                "q": query_config.query,
                "sort": "updated",
                "order": "desc",
                "per_page": per_page,
            },
            timeout=timeout,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "event=github_fetch_failed source_id=%s error=%s",
            query_config.source_id,
            exc,
        )
        return GithubFetchOutcome(
            query_config=query_config, http_status=None, entries=[], error=str(exc)
        )

    if is_rate_limited_response(response):
        logger.warning(
            "event=github_rate_limited source_id=%s status=%s remaining=%s",
            query_config.source_id,
            response.status_code,
            response.headers.get("X-RateLimit-Remaining"),
        )
        return GithubFetchOutcome(
            query_config=query_config,
            http_status=response.status_code,
            entries=[],
            error=f"rate_limited (HTTP {response.status_code})",
            rate_limited=True,
        )

    if response.status_code != 200:
        logger.warning(
            "event=github_fetch_http_error source_id=%s status=%s",
            query_config.source_id,
            response.status_code,
        )
        return GithubFetchOutcome(
            query_config=query_config,
            http_status=response.status_code,
            entries=[],
            error=f"HTTP {response.status_code}",
        )

    data = response.json()
    items = data.get("items", [])
    if not isinstance(items, list):
        logger.warning(
            "event=github_parse_failed source_id=%s error=%s",
            query_config.source_id,
            "response.items không phải list",
        )
        return GithubFetchOutcome(
            query_config=query_config,
            http_status=response.status_code,
            entries=[],
            error="response.items không phải list",
        )

    entries = [repo_to_payload(item) for item in items]
    return GithubFetchOutcome(
        query_config=query_config,
        http_status=response.status_code,
        entries=entries,
        error=None,
    )


async def fetch_all_queries(
    connection: sa.Connection,
    client: httpx.AsyncClient,
    queries: list[GithubQueryConfig],
    *,
    ingest_date: date,
    per_page: int = 5,
    timeout: float = 30.0,
) -> GithubIngestResult:
    """Fetch TUẦN TỰ (không đồng thời — ngân sách rate limit chia sẻ chung giữa mọi truy
    vấn, chạy tuần tự để dừng chính xác đúng lúc vượt hạn mức) từng truy vấn, ghi bronze +
    source_health. Vượt hạn mức dừng sạch phần còn lại — KHÔNG raise (P4).

    Nhận `client` qua tham số (không tự tạo bên trong) để test được bằng
    `httpx.MockTransport`, không cần gọi mạng thật.
    """
    result = GithubIngestResult()
    fetched_at = datetime.now(tz=UTC)

    for query_config in queries:
        outcome = await _fetch_one_query(
            client, query_config, per_page=per_page, timeout=timeout
        )

        if outcome.rate_limited:
            result.rate_limited = True
            result.failed_sources.append(query_config.source_id)
            upsert_source_health(
                connection,
                source_id=query_config.source_id,
                fetch_date=ingest_date,
                http_status=outcome.http_status,
                entry_count=None,
                error_message=outcome.error,
                etag=None,
                last_modified=None,
                fetched_at=fetched_at,
            )
            connection.commit()
            logger.warning(
                "event=github_ingest_stopped_rate_limit remaining_queries=%s",
                len(queries) - queries.index(query_config) - 1,
            )
            break

        if outcome.error is not None:
            upsert_source_health(
                connection,
                source_id=query_config.source_id,
                fetch_date=ingest_date,
                http_status=outcome.http_status,
                entry_count=None,
                error_message=outcome.error,
                etag=None,
                last_modified=None,
                fetched_at=fetched_at,
            )
            connection.commit()
            result.failed_sources.append(query_config.source_id)
            continue

        inserted_for_query = 0
        for payload in outcome.entries:
            raw_url = str(payload.get("link") or payload.get("html_url") or "")
            payload_hash = compute_payload_hash(payload)
            was_inserted = insert_raw_article(
                connection,
                ingest_date=ingest_date,
                source_id=query_config.source_id,
                source_type=SOURCE_TYPE,
                raw_url=raw_url,
                payload=payload,
                payload_hash=payload_hash,
                fetched_at=fetched_at,
            )
            if was_inserted:
                inserted_for_query += 1

        upsert_source_health(
            connection,
            source_id=query_config.source_id,
            fetch_date=ingest_date,
            http_status=outcome.http_status,
            entry_count=len(outcome.entries),
            error_message=None,
            etag=None,
            last_modified=None,
            fetched_at=fetched_at,
        )
        connection.commit()

        result.total_entries_fetched += len(outcome.entries)
        result.rows_inserted += inserted_for_query
        result.sources_ok += 1

    return result


def _build_headers(user_agent: str, github_token: str) -> dict[str, str]:
    """Header chuẩn cho REST API GitHub — `Authorization` chỉ thêm khi có token thật (PAT
    đọc từ biến môi trường `GITHUB_TOKEN`, không bao giờ hardcode/commit)."""
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    return headers


async def run_github_ingest(
    connection: sa.Connection,
    queries: list[GithubQueryConfig],
    *,
    user_agent: str,
    github_token: str,
    ingest_date: date,
    per_page: int = 5,
    timeout: float = 30.0,
) -> GithubIngestResult:
    """Wrapper cho asset Dagster: tự tạo httpx.AsyncClient thật rồi gọi `fetch_all_queries`.

    `github_token` rỗng vẫn chạy được (GitHub Search API cho phép unauthenticated, hạn mức
    thấp hơn — 10 request/phút thay vì 30, xem docstring module) — không raise, chỉ log rõ
    đang chạy ở chế độ hạn mức thấp hơn.
    """
    if not github_token:
        logger.info(
            'event=github_ingest_unauthenticated msg="GITHUB_TOKEN rỗng — chạy '
            'unauthenticated, hạn mức 10 request/phút thay vì 30"'
        )
    async with httpx.AsyncClient(
        base_url=GITHUB_API_BASE_URL,
        headers=_build_headers(user_agent, github_token),
        follow_redirects=True,
    ) as client:
        return await fetch_all_queries(
            connection,
            client,
            queries,
            ingest_date=ingest_date,
            per_page=per_page,
            timeout=timeout,
        )
