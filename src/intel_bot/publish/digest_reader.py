"""Đọc `gold.mart_daily_digest` và cập nhật `silver.articles.last_published_at`
(PRODUCTION_PLAN §12.1, §12.2, §4.3).

Có I/O — nhận connection qua tham số, không tự tạo bên trong (để test được, theo AGENTS.md
mục 3). Đây là module DUY NHẤT của tầng publish chạm tới Postgres; mọi lựa chọn bài, dedup,
xếp hạng, nhóm ngành đã xong ở `gold.mart_daily_digest` (§12.1) — module này chỉ SELECT *
từ đúng một bảng đó, cộng với UPDATE `last_published_at` (ngoại lệ DUY NHẤT được rào chắn
task 0.11 cho phép chạm bảng khác).

**Không dùng `last_published_at` để lọc bài ở lần chạy sau** (§4.3 "Sửa lỗi 1" — v1 dùng
trạng thái `published` làm bài đã lên trang bị loại khỏi cửa sổ 48h hôm sau; v2 bỏ hẳn khái
niệm "trạng thái đã publish" khỏi state machine, `last_published_at` chỉ là cột thông tin
để biết bài đã từng hiển thị chưa, không ảnh hưởng gì đến việc bài có được chọn lại lần
sau hay không — điều đó do `gold.mart_daily_digest` tự quyết định lại mỗi lần build).
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


@dataclass(frozen=True)
class DigestRow:
    """Một dòng `gold.mart_daily_digest` — khớp 1:1 với cột của mart, không thêm bớt.

    Đây là dữ liệu-nhập của tầng render (json_exporter/html_renderer); các hàm render là
    hàm thuần nhận `list[DigestRow]`, không tự truy vấn DB (AGENTS.md mục 3).
    """

    score_id: uuid.UUID
    article_id: uuid.UUID
    canonical_url: str
    title: str
    snippet: str | None
    industry_tags: list[str] | None
    source_id: str
    source_domain: str | None
    source_tier: int | None
    published_at: dt.datetime | None
    published_at_imputed: bool
    first_seen_at: dt.datetime
    credibility_blended: Decimal
    importance: int
    practicality: int
    depth: int
    recency_boost: Decimal
    composite_score: Decimal
    summary_vi: list[str]
    why_it_matters_vi: str | None
    industry_group: str
    digest_built_at: dt.datetime


def fetch_digest_rows(connection: sa.Connection) -> list[DigestRow]:
    """`SELECT * FROM gold.mart_daily_digest` — truy vấn DUY NHẤT của publish job (§12.1).

    KHÔNG thêm `ORDER BY`, `WHERE`, hay bất kỳ điều kiện nào: mart đã tự sắp xếp theo
    composite_score giảm dần và tự lọc cửa sổ 48h khi build (dbt, task 0.10/0.11). Thứ tự
    vật lý của một bảng Postgres vừa được `dbt run` tạo lại bằng CTAS (không UPDATE/DELETE
    xen giữa) ổn định giữa các lần SELECT liên tiếp trong thực tế — đây là giả định cần
    biết nếu sau này có ai thêm UPDATE/DELETE trực tiếp lên `gold.mart_daily_digest`.
    """
    rows = connection.execute(sa.text("SELECT * FROM gold.mart_daily_digest")).all()
    return [
        DigestRow(
            score_id=row.score_id,
            article_id=row.article_id,
            canonical_url=row.canonical_url,
            title=row.title,
            snippet=row.snippet,
            industry_tags=list(row.industry_tags) if row.industry_tags else None,
            source_id=row.source_id,
            source_domain=row.source_domain,
            source_tier=row.source_tier,
            published_at=row.published_at,
            published_at_imputed=row.published_at_imputed,
            first_seen_at=row.first_seen_at,
            credibility_blended=row.credibility_blended,
            importance=row.importance,
            practicality=row.practicality,
            depth=row.depth,
            recency_boost=row.recency_boost,
            composite_score=row.composite_score,
            summary_vi=list(row.summary_vi),
            why_it_matters_vi=row.why_it_matters_vi,
            industry_group=row.industry_group,
            digest_built_at=row.digest_built_at,
        )
        for row in rows
    ]


def mark_published(
    connection: sa.Connection,
    *,
    article_ids: list[uuid.UUID],
    published_at: dt.datetime,
) -> int:
    """Cập nhật `silver.articles.last_published_at` cho các bài vừa lên trang.

    Đây là cột THÔNG TIN (§4.3 "Sửa lỗi 1"), không phải trạng thái xử lý — không có bất kỳ
    hàm nào trong pipeline đọc cột này để quyết định bài có được chấm/xuất bản lại hay
    không. Trả về số dòng đã cập nhật.
    """
    if not article_ids:
        return 0
    result = connection.execute(
        sa.text(
            "UPDATE silver.articles SET last_published_at = :published_at"
            " WHERE article_id = ANY(:article_ids)"
        ).bindparams(
            sa.bindparam("article_ids", type_=postgresql.ARRAY(postgresql.UUID))
        ),
        {"published_at": published_at, "article_ids": article_ids},
    )
    return result.rowcount
