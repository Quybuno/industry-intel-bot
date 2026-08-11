"""Truy cập bronze.raw_articles và silver.source_health — SQLAlchemy Core, không ORM.

Mọi hàm I/O nhận connection qua tham số, không tự tạo bên trong (để test được).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def insert_raw_article(
    connection: sa.Connection,
    *,
    ingest_date: dt.date,
    source_id: str,
    source_type: str,
    raw_url: str,
    payload: dict[str, Any],
    payload_hash: str,
    fetched_at: dt.datetime,
) -> bool:
    """Ghi một entry vào bronze.raw_articles, bỏ qua nếu đã tồn tại (idempotent theo P1).

    Trả về True nếu dòng mới được ghi, False nếu bị bỏ qua do trùng
    (ingest_date, payload_hash) — tức cùng một payload đã ghi trong cùng ngày.
    """
    result = connection.execute(
        sa.text(
            """
            INSERT INTO bronze.raw_articles
                (ingest_date, source_id, source_type, raw_url, payload, payload_hash, fetched_at)
            VALUES
                (:ingest_date, :source_id, :source_type, :raw_url, :payload, :payload_hash, :fetched_at)
            ON CONFLICT (ingest_date, payload_hash) DO NOTHING
            """
        ).bindparams(sa.bindparam("payload", type_=postgresql.JSONB)),
        {
            "ingest_date": ingest_date,
            "source_id": source_id,
            "source_type": source_type,
            "raw_url": raw_url,
            "payload": payload,
            "payload_hash": payload_hash,
            "fetched_at": fetched_at,
        },
    )
    return result.rowcount > 0


def get_last_conditional_headers(
    connection: sa.Connection, *, source_id: str
) -> tuple[str | None, str | None]:
    """Lấy (etag, last_modified) của lần fetch gần nhất cho một nguồn, để làm conditional GET.

    Trả về (None, None) nếu nguồn chưa từng được fetch trước đó.
    """
    row = connection.execute(
        sa.text(
            """
            SELECT etag, last_modified
            FROM silver.source_health
            WHERE source_id = :source_id
            ORDER BY fetched_at DESC
            LIMIT 1
            """
        ),
        {"source_id": source_id},
    ).first()
    if row is None:
        return None, None
    return row.etag, row.last_modified


def upsert_source_health(
    connection: sa.Connection,
    *,
    source_id: str,
    fetch_date: dt.date,
    http_status: int | None,
    entry_count: int | None,
    error_message: str | None,
    etag: str | None,
    last_modified: str | None,
    fetched_at: dt.datetime,
) -> None:
    """Ghi kết quả fetch một nguồn trong ngày vào silver.source_health.

    Ghi đè (upsert) theo (source_id, fetch_date) — chạy lại job của cùng ngày cho
    cùng nguồn cập nhật đúng dòng đó, không tạo dòng trùng (P1).
    """
    connection.execute(
        sa.text(
            """
            INSERT INTO silver.source_health
                (source_id, fetch_date, http_status, entry_count, error_message, etag, last_modified, fetched_at)
            VALUES
                (:source_id, :fetch_date, :http_status, :entry_count, :error_message, :etag, :last_modified, :fetched_at)
            ON CONFLICT (source_id, fetch_date) DO UPDATE SET
                http_status = EXCLUDED.http_status,
                entry_count = EXCLUDED.entry_count,
                error_message = EXCLUDED.error_message,
                etag = EXCLUDED.etag,
                last_modified = EXCLUDED.last_modified,
                fetched_at = EXCLUDED.fetched_at
            """
        ),
        {
            "source_id": source_id,
            "fetch_date": fetch_date,
            "http_status": http_status,
            "entry_count": entry_count,
            "error_message": error_message,
            "etag": etag,
            "last_modified": last_modified,
            "fetched_at": fetched_at,
        },
    )


def count_raw_articles(connection: sa.Connection, *, ingest_date: dt.date) -> int:
    """Đếm số dòng bronze.raw_articles của một ngày — tiện cho test idempotency."""
    count = connection.execute(
        sa.text("SELECT COUNT(*) FROM bronze.raw_articles WHERE ingest_date = :ingest_date"),
        {"ingest_date": ingest_date},
    ).scalar_one()
    return int(count)
