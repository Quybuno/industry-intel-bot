"""Đọc bronze.raw_articles theo ngày, chuẩn hoá, dedup cấp 1, ghi silver.articles.

Có I/O — nhận connection qua tham số, không tự tạo bên trong (để test được). Toàn bộ
logic chuẩn hoá/cold start nằm ở normalizer.py (hàm thuần); ở đây chỉ orchestrate.
KHÔNG đụng bronze ngoài SELECT (P2 — bronze bất biến).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from src.intel_bot.ingest.normalizer import (
    apply_cold_start_rules,
    canonicalize_url,
    compute_content_hash,
    extract_domain,
    make_article_id,
    parse_entry,
)


@dataclass(frozen=True)
class BronzeRow:
    """Một dòng bronze.raw_articles đọc để chuẩn hoá."""

    id: int
    source_id: str
    raw_url: str
    payload: dict[str, Any]
    fetched_at: dt.datetime


@dataclass
class NormalizeResult:
    """Thống kê một lần chạy normalize: số đọc, số ghi mới, số cập nhật, số loại theo lý do."""

    read: int = 0
    inserted: int = 0
    updated: int = 0
    excluded_by_reason: dict[str, int] = field(default_factory=dict)

    def add_exclusion(self, reason: str) -> None:
        self.excluded_by_reason[reason] = self.excluded_by_reason.get(reason, 0) + 1


def load_bronze_rows(
    connection: sa.Connection, *, ingest_date: dt.date
) -> list[BronzeRow]:
    """Đọc bronze.raw_articles của một ngày. CHỈ ĐỌC — không bao giờ UPDATE/DELETE bronze (P2)."""
    rows = connection.execute(
        sa.text(
            """
            SELECT id, source_id, raw_url, payload, fetched_at
            FROM bronze.raw_articles
            WHERE ingest_date = :ingest_date
            ORDER BY id
            """
        ),
        {"ingest_date": ingest_date},
    ).all()
    return [
        BronzeRow(
            id=row.id,
            source_id=row.source_id,
            raw_url=row.raw_url,
            payload=row.payload,
            fetched_at=row.fetched_at,
        )
        for row in rows
    ]


def upsert_silver_article(
    connection: sa.Connection,
    *,
    article_id: Any,
    canonical_url: str,
    raw_url: str,
    content_hash: str,
    source_id: str,
    title: str,
    snippet: str,
    published_at: dt.datetime | None,
    first_seen_at: dt.datetime,
    first_seen_date: dt.date,
    status: str,
    exclusion_reason: str | None,
    published_at_imputed: bool,
) -> bool:
    """Dedup cấp 1 theo canonical_url: INSERT ... ON CONFLICT (canonical_url) DO UPDATE.

    `first_seen_at` VÀ `first_seen_date` giữ giá trị SỚM NHẤT (LEAST trên chính cột của nó);
    các trường khác cập nhật theo bản mới (PRODUCTION_PLAN §8.4, task 0.5 mục 3). Trả về
    True nếu là dòng MỚI, False nếu UPDATE.

    **Bug thật đã sửa (task 1.8/1.9, phát hiện qua test backfill):** trước đây
    `first_seen_date` bị tính lại bằng `LEAST(first_seen_at, EXCLUDED.first_seen_at)::date`
    — tức suy ra từ THỜI ĐIỂM FETCH THẬT thay vì so trực tiếp cột `first_seen_date` (nhãn
    partition, độc lập với thời điểm chạy thật — xem `ingest_date` ở `normalize_partition`).
    Vô hại trong vận hành hằng ngày bình thường (ingest_date luôn trùng ngày thật), nhưng
    SAI khi backfill một partition CŨ: bài đã tồn tại từ trước, re-normalize dưới
    `ingest_date` cũ sẽ bị `first_seen_date` "nhảy" về ngày chạy THẬT thay vì giữ đúng nhãn
    partition — phá tính lũy đẳng (P1) đúng lúc backfill, không lộ ra ở vận hành thường
    ngày. Sửa: so trực tiếp `first_seen_date` cũ/mới, không suy ra từ `first_seen_at`.
    """
    result = connection.execute(
        sa.text(
            """
            INSERT INTO silver.articles (
                article_id, canonical_url, raw_url, content_hash, source_id, title, snippet,
                published_at, first_seen_at, first_seen_date, status, exclusion_reason,
                published_at_imputed
            ) VALUES (
                :article_id, :canonical_url, :raw_url, :content_hash, :source_id, :title, :snippet,
                :published_at, :first_seen_at, :first_seen_date, :status, :exclusion_reason,
                :published_at_imputed
            )
            ON CONFLICT (canonical_url) DO UPDATE SET
                raw_url = EXCLUDED.raw_url,
                content_hash = EXCLUDED.content_hash,
                source_id = EXCLUDED.source_id,
                title = EXCLUDED.title,
                snippet = EXCLUDED.snippet,
                published_at = EXCLUDED.published_at,
                first_seen_at = LEAST(silver.articles.first_seen_at, EXCLUDED.first_seen_at),
                first_seen_date = LEAST(silver.articles.first_seen_date, EXCLUDED.first_seen_date),
                status = EXCLUDED.status,
                exclusion_reason = EXCLUDED.exclusion_reason,
                published_at_imputed = EXCLUDED.published_at_imputed
            RETURNING (xmax = 0) AS was_insert
            """
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {
            "article_id": article_id,
            "canonical_url": canonical_url,
            "raw_url": raw_url,
            "content_hash": content_hash,
            "source_id": source_id,
            "title": title,
            "snippet": snippet,
            "published_at": published_at,
            "first_seen_at": first_seen_at,
            "first_seen_date": first_seen_date,
            "status": status,
            "exclusion_reason": exclusion_reason,
            "published_at_imputed": published_at_imputed,
        },
    )
    return bool(result.scalar_one())


def normalize_partition(
    connection: sa.Connection,
    *,
    ingest_date: dt.date,
    max_article_age_days: int,
    now: dt.datetime,
) -> NormalizeResult:
    """Chuẩn hoá toàn bộ bronze của một ngày → ghi silver.articles. Idempotent (P1)."""
    result = NormalizeResult()
    rows = load_bronze_rows(connection, ingest_date=ingest_date)
    result.read = len(rows)

    for row in rows:
        parsed = parse_entry(row.payload)
        if parsed is None:
            result.add_exclusion("missing_title_or_link")
            continue

        canonical_url = canonicalize_url(parsed.link)
        domain = extract_domain(canonical_url)
        content_hash = compute_content_hash(parsed.title, domain)
        article_id = make_article_id(canonical_url)
        cold_start = apply_cold_start_rules(
            parsed.published_at, now=now, max_article_age_days=max_article_age_days
        )

        was_insert = upsert_silver_article(
            connection,
            article_id=article_id,
            canonical_url=canonical_url,
            raw_url=row.raw_url,
            content_hash=content_hash,
            source_id=row.source_id,
            title=parsed.title,
            snippet=parsed.snippet,
            published_at=parsed.published_at,
            first_seen_at=row.fetched_at,
            first_seen_date=ingest_date,
            status=cold_start.status,
            exclusion_reason=cold_start.exclusion_reason,
            published_at_imputed=cold_start.published_at_imputed,
        )
        if was_insert:
            result.inserted += 1
        else:
            result.updated += 1

        if cold_start.exclusion_reason:
            result.add_exclusion(cold_start.exclusion_reason)

    connection.commit()
    return result
