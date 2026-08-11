"""RSS fetcher — httpx.AsyncClient + feedparser, ghi thẳng vào bronze.raw_articles.

Không lọc, không chuẩn hoá payload (việc của task 0.5). Lỗi bắt theo từng nguồn —
một nguồn chết không làm hỏng job.
"""

from __future__ import annotations

import asyncio
import calendar
import hashlib
import json
import logging
import time as time_module
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import feedparser  # type: ignore[import-untyped]
import httpx
import sqlalchemy as sa
import yaml

from src.intel_bot.db.bronze import (
    get_last_conditional_headers,
    insert_raw_article,
    upsert_source_health,
)

logger = logging.getLogger(__name__)

SOURCE_TYPE = "rss"


@dataclass(frozen=True)
class SourceConfig:
    """Một nguồn RSS đọc từ `config/sources.yaml`."""

    source_id: str
    url: str
    domain: str
    tier: int
    industries: list[str]
    is_enabled: bool


@dataclass
class SourceFetchOutcome:
    """Kết quả fetch một nguồn — dữ liệu trung gian trước khi ghi DB."""

    source: SourceConfig
    http_status: int | None
    entries: list[dict[str, Any]]
    etag: str | None
    last_modified: str | None
    error: str | None
    not_modified: bool = False


@dataclass
class SourceValidation:
    """Kết quả kiểm tra một nguồn cho lệnh `validate-sources`."""

    source_id: str
    domain: str
    http_status: int | None
    entry_count: int
    has_date_field: bool
    ok: bool
    error: str | None


@dataclass
class RssIngestResult:
    """Kết quả tổng hợp một lần chạy ingest RSS."""

    total_entries_fetched: int = 0
    rows_inserted: int = 0
    sources_ok: int = 0
    failed_sources: list[str] = field(default_factory=list)


def load_source_configs(
    path: str = "config/sources.yaml", *, only_enabled: bool = True
) -> list[SourceConfig]:
    """Đọc danh sách nguồn RSS từ file YAML — không hardcode nguồn trong code Python."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    configs = []
    for item in raw.get("sources", []):
        is_enabled = bool(item.get("is_enabled", True))
        if only_enabled and not is_enabled:
            continue
        configs.append(
            SourceConfig(
                source_id=item["source_id"],
                url=item["url"],
                domain=item["domain"],
                tier=int(item["tier"]),
                industries=list(item.get("industries", [])),
                is_enabled=is_enabled,
            )
        )
    return configs


def compute_payload_hash(payload: dict[str, Any]) -> str:
    """SHA-256 của payload đã chuẩn hoá (`json.dumps(sort_keys=True)`).

    Cùng nội dung, khác thứ tự key trong dict Python vẫn cho cùng một hash.
    """
    normalized = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _json_safe(value: object) -> object:
    """Đệ quy chuyển các kiểu feedparser không JSON-hoá được (vd. `time.struct_time`)."""
    if isinstance(value, time_module.struct_time):
        return datetime.fromtimestamp(
            calendar.timegm(value), tz=UTC
        ).isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def entry_to_payload(entry: Any) -> dict[str, Any]:
    """Chuyển một entry feedparser thành dict JSONB được — giữ NGUYÊN toàn bộ trường."""
    safe = _json_safe(dict(entry))
    assert isinstance(safe, dict)
    return safe


def has_date_field(payload: dict[str, Any]) -> bool:
    """Entry có trường ngày thô (published/updated) hay không — dùng cho validate-sources."""
    return bool(payload.get("published") or payload.get("updated"))


async def _fetch_one_source(
    client: httpx.AsyncClient,
    source: SourceConfig,
    *,
    conditional_headers: dict[str, str],
    semaphore: asyncio.Semaphore,
    timeout: float,
) -> SourceFetchOutcome:
    """Fetch + parse một nguồn. Không bao giờ raise ra ngoài — lỗi gói vào `outcome.error`."""
    async with semaphore:
        try:
            response = await client.get(
                source.url, headers=conditional_headers, timeout=timeout
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "event=rss_fetch_failed source_id=%s error=%s", source.source_id, exc
            )
            return SourceFetchOutcome(
                source=source,
                http_status=None,
                entries=[],
                etag=None,
                last_modified=None,
                error=str(exc),
            )

        if response.status_code == 304:
            logger.info("event=rss_not_modified source_id=%s", source.source_id)
            return SourceFetchOutcome(
                source=source,
                http_status=304,
                entries=[],
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
                error=None,
                not_modified=True,
            )

        if response.status_code != 200:
            logger.warning(
                "event=rss_fetch_http_error source_id=%s status=%s",
                source.source_id,
                response.status_code,
            )
            return SourceFetchOutcome(
                source=source,
                http_status=response.status_code,
                entries=[],
                etag=None,
                last_modified=None,
                error=f"HTTP {response.status_code}",
            )

        feed = feedparser.parse(response.content)
        if feed.bozo and not feed.entries:
            error_text = str(getattr(feed, "bozo_exception", "unparseable feed"))
            logger.warning(
                "event=rss_parse_failed source_id=%s error=%s",
                source.source_id,
                error_text,
            )
            return SourceFetchOutcome(
                source=source,
                http_status=response.status_code,
                entries=[],
                etag=None,
                last_modified=None,
                error=error_text,
            )

        entries = [entry_to_payload(entry) for entry in feed.entries]
        return SourceFetchOutcome(
            source=source,
            http_status=response.status_code,
            entries=entries,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            error=None,
        )


async def validate_sources(
    client: httpx.AsyncClient,
    sources: list[SourceConfig],
    *,
    timeout: float = 30.0,
    max_concurrent: int = 5,
) -> list[SourceValidation]:
    """Kiểm tra từng nguồn: HTTP 200, parse được, ≥1 entry, có trường ngày. Không ghi DB.

    Nhận `client` qua tham số (không tự tạo bên trong) để test được bằng
    `httpx.MockTransport`, không cần gọi mạng thật.
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    outcomes = await asyncio.gather(
        *[
            _fetch_one_source(
                client,
                source,
                conditional_headers={},
                semaphore=semaphore,
                timeout=timeout,
            )
            for source in sources
        ]
    )

    validations = []
    for outcome in outcomes:
        entry_count = len(outcome.entries)
        any_date = any(has_date_field(e) for e in outcome.entries)
        ok = outcome.error is None and entry_count > 0 and any_date
        validations.append(
            SourceValidation(
                source_id=outcome.source.source_id,
                domain=outcome.source.domain,
                http_status=outcome.http_status,
                entry_count=entry_count,
                has_date_field=any_date,
                ok=ok,
                error=outcome.error,
            )
        )
    return validations


async def fetch_all_sources(
    connection: sa.Connection,
    client: httpx.AsyncClient,
    sources: list[SourceConfig],
    *,
    ingest_date: date,
    max_concurrent: int = 5,
    timeout: float = 30.0,
) -> RssIngestResult:
    """Fetch đồng thời (tối đa `max_concurrent`) các nguồn RSS, ghi bronze + source_health.

    Lỗi ở một nguồn không làm hỏng các nguồn khác — mỗi outcome xử lý và commit độc lập.
    Nhận `client` qua tham số (không tự tạo bên trong) để test được bằng
    `httpx.MockTransport`, không cần gọi mạng thật.
    """
    result = RssIngestResult()
    semaphore = asyncio.Semaphore(max_concurrent)

    tasks = []
    for source in sources:
        etag, last_modified = get_last_conditional_headers(
            connection, source_id=source.source_id
        )
        conditional_headers: dict[str, str] = {}
        if etag:
            conditional_headers["If-None-Match"] = etag
        if last_modified:
            conditional_headers["If-Modified-Since"] = last_modified
        tasks.append(
            _fetch_one_source(
                client,
                source,
                conditional_headers=conditional_headers,
                semaphore=semaphore,
                timeout=timeout,
            )
        )
    outcomes = await asyncio.gather(*tasks)

    fetched_at = datetime.now(tz=UTC)
    for outcome in outcomes:
        source = outcome.source

        if outcome.error is not None:
            upsert_source_health(
                connection,
                source_id=source.source_id,
                fetch_date=ingest_date,
                http_status=outcome.http_status,
                entry_count=None,
                error_message=outcome.error,
                etag=None,
                last_modified=None,
                fetched_at=fetched_at,
            )
            connection.commit()
            result.failed_sources.append(source.source_id)
            continue

        if outcome.not_modified:
            upsert_source_health(
                connection,
                source_id=source.source_id,
                fetch_date=ingest_date,
                http_status=304,
                entry_count=0,
                error_message=None,
                etag=outcome.etag,
                last_modified=outcome.last_modified,
                fetched_at=fetched_at,
            )
            connection.commit()
            result.sources_ok += 1
            continue

        inserted_for_source = 0
        for payload in outcome.entries:
            raw_url = str(payload.get("link") or payload.get("id") or source.url)
            payload_hash = compute_payload_hash(payload)
            was_inserted = insert_raw_article(
                connection,
                ingest_date=ingest_date,
                source_id=source.source_id,
                source_type=SOURCE_TYPE,
                raw_url=raw_url,
                payload=payload,
                payload_hash=payload_hash,
                fetched_at=fetched_at,
            )
            if was_inserted:
                inserted_for_source += 1

        upsert_source_health(
            connection,
            source_id=source.source_id,
            fetch_date=ingest_date,
            http_status=outcome.http_status,
            entry_count=len(outcome.entries),
            error_message=None,
            etag=outcome.etag,
            last_modified=outcome.last_modified,
            fetched_at=fetched_at,
        )
        connection.commit()

        result.total_entries_fetched += len(outcome.entries)
        result.rows_inserted += inserted_for_source
        result.sources_ok += 1

    return result


async def run_rss_ingest(
    connection: sa.Connection,
    sources: list[SourceConfig],
    *,
    user_agent: str,
    ingest_date: date,
    max_concurrent: int = 5,
    timeout: float = 30.0,
) -> RssIngestResult:
    """Wrapper cho CLI: tự tạo httpx.AsyncClient thật rồi gọi `fetch_all_sources`."""
    async with httpx.AsyncClient(
        headers={"User-Agent": user_agent}, follow_redirects=True
    ) as client:
        return await fetch_all_sources(
            connection,
            client,
            sources,
            ingest_date=ingest_date,
            max_concurrent=max_concurrent,
            timeout=timeout,
        )


async def run_validate_sources(
    sources: list[SourceConfig],
    *,
    user_agent: str,
    timeout: float = 30.0,
    max_concurrent: int = 5,
) -> list[SourceValidation]:
    """Wrapper cho CLI: tự tạo httpx.AsyncClient thật rồi gọi `validate_sources`."""
    async with httpx.AsyncClient(
        headers={"User-Agent": user_agent}, follow_redirects=True
    ) as client:
        return await validate_sources(
            client, sources, timeout=timeout, max_concurrent=max_concurrent
        )
