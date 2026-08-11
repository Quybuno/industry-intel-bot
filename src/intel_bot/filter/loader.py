"""Đọc silver.articles theo ngày, áp filter tối thiểu, ghi status/filter_score/exclusion_reason.

Có I/O — nhận connection qua tham số, không tự tạo bên trong (để test được). Logic quyết
định nằm ở keyword_filter.py (hàm thuần); ở đây chỉ orchestrate. KHÔNG xoá bản ghi bị
loại — chỉ đổi status. KHÔNG gọi LLM, KHÔNG cài đặt embedding filter (task 0.6 rào chắn).
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from src.intel_bot.filter.keyword_filter import (
    ArticleRow,
    FilterRules,
    FilterVerdict,
    apply_daily_cap,
    evaluate,
)


@dataclass
class FilterJobResult:
    """Thống kê một lần chạy filter: số đọc, số eligible, số excluded, phân bố theo lý do."""

    read: int = 0
    eligible: int = 0
    excluded: int = 0
    excluded_by_reason: dict[str, int] = field(default_factory=dict)

    def add_exclusion(self, reason: str) -> None:
        self.excluded_by_reason[reason] = self.excluded_by_reason.get(reason, 0) + 1


def load_articles_for_date(
    connection: sa.Connection, *, filter_date: dt.date
) -> list[ArticleRow]:
    """Đọc silver.articles theo first_seen_date, thứ tự ổn định (ORDER BY article_id).

    Thứ tự ổn định là điều kiện để `apply_daily_cap()` cho kết quả idempotent khi có
    nhiều bài trùng published_at (Python `sorted` ổn định, giữ nguyên thứ tự đầu vào khi hoà).
    """
    rows = connection.execute(
        sa.text(
            """
            SELECT article_id, title, snippet, published_at
            FROM silver.articles
            WHERE first_seen_date = :filter_date
            ORDER BY article_id
            """
        ),
        {"filter_date": filter_date},
    ).all()
    return [
        ArticleRow(
            article_id=row.article_id,
            title=row.title or "",
            snippet=row.snippet or "",
            published_at=row.published_at,
        )
        for row in rows
    ]


def write_verdict(
    connection: sa.Connection, *, article_id: uuid.UUID, verdict: FilterVerdict
) -> None:
    """Ghi status/filter_score/exclusion_reason cho một bài. KHÔNG xoá bản ghi."""
    status = "eligible" if verdict.passed else "excluded"
    connection.execute(
        sa.text(
            """
            UPDATE silver.articles
            SET status = :status,
                filter_score = :filter_score,
                exclusion_reason = :exclusion_reason
            WHERE article_id = :article_id
            """
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {
            "article_id": article_id,
            "status": status,
            "filter_score": verdict.filter_score,
            "exclusion_reason": verdict.exclusion_reason,
        },
    )


def run_filter_partition(
    connection: sa.Connection, *, filter_date: dt.date, rules: FilterRules
) -> FilterJobResult:
    """Chạy filter tối thiểu cho toàn bộ silver.articles của một ngày (first_seen_date).

    Áp evaluate() cho từng bài (3 quy tắc per-article), rồi apply_daily_cap() SAU CÙNG
    trên toàn bộ kết quả trong ngày (task 0.6 mục 3). Idempotent: chạy lại với cùng dữ
    liệu + cùng config cho ra cùng status/filter_score/exclusion_reason (P1).
    """
    result = FilterJobResult()
    articles = load_articles_for_date(connection, filter_date=filter_date)
    result.read = len(articles)

    evaluated = [(article, evaluate(article, rules)) for article in articles]
    final_verdicts = apply_daily_cap(
        evaluated, max_articles_per_day=rules.max_articles_per_day
    )

    for (article, _), verdict in zip(evaluated, final_verdicts, strict=True):
        write_verdict(connection, article_id=article.article_id, verdict=verdict)
        if verdict.passed:
            result.eligible += 1
        else:
            result.excluded += 1
            if verdict.exclusion_reason:
                result.add_exclusion(verdict.exclusion_reason)

    connection.commit()
    return result
