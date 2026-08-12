"""Xuất `gold.mart_daily_digest` ra JSON (PRODUCTION_PLAN §12.1, §12.2).

`build_digest_payload` là hàm THUẦN — nhận `list[DigestRow]` đã đọc sẵn (digest_reader.py),
không tự truy vấn DB, không sắp xếp/lọc/dedup gì thêm (mart đã làm hết, §12.1). Chỉ có
`write_digest_json` chạm filesystem (I/O), tách riêng theo AGENTS.md mục 3.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from src.intel_bot.publish.digest_reader import DigestRow

#: `Any` ở đây là giá trị JSON-serializable (str/int/float/bool/list/dict/None) sau khi ép
#: kiểu từ DigestRow — không có TypedDict tương ứng vì payload lồng (articles là list các
#: dict cùng cấu trúc); dùng Any thay vì viết TypedDict lồng nhau chỉ để phục vụ đúng một
#: chỗ export JSON (P8 — không thêm phức tạp khi chưa cần).
_JsonValue = Any


def _row_to_dict(row: DigestRow) -> dict[str, _JsonValue]:
    """Ép kiểu 1:1 từng cột của DigestRow sang giá trị JSON-serializable — không đổi tên,
    không tính toán lại gì (không phải business logic, chỉ ép kiểu để `json.dumps` chạy
    được: UUID/Decimal/datetime không tự serialize được)."""
    return {
        "score_id": str(row.score_id),
        "article_id": str(row.article_id),
        "canonical_url": row.canonical_url,
        "title": row.title,
        "snippet": row.snippet,
        "industry_tags": row.industry_tags,
        "source_id": row.source_id,
        "source_domain": row.source_domain,
        "source_tier": row.source_tier,
        "published_at": row.published_at.isoformat() if row.published_at else None,
        "published_at_imputed": row.published_at_imputed,
        "first_seen_at": row.first_seen_at.isoformat(),
        "credibility_blended": float(row.credibility_blended),
        "importance": row.importance,
        "practicality": row.practicality,
        "depth": row.depth,
        "recency_boost": float(row.recency_boost),
        "composite_score": float(row.composite_score),
        "summary_vi": row.summary_vi,
        "why_it_matters_vi": row.why_it_matters_vi,
        "industry_group": row.industry_group,
    }


def build_digest_payload(
    rows: list[DigestRow], *, generated_for_date: dt.date
) -> dict[str, _JsonValue]:
    """Dựng payload JSON từ các dòng `gold.mart_daily_digest` đã đọc sẵn — hàm thuần.

    `digest_built_at` lấy từ chính cột cùng tên trong mart (giống hệt mọi dòng, xem
    comment ở `mart_daily_digest.sql`) — không đọc bảng nào khác để có "thời điểm chạy
    pipeline" (rào chắn task 0.11: publish chỉ được đọc `gold.mart_daily_digest`).
    """
    digest_built_at = rows[0].digest_built_at.isoformat() if rows else None
    return {
        "generated_for_date": generated_for_date.isoformat(),
        "digest_built_at": digest_built_at,
        "article_count": len(rows),
        "articles": [_row_to_dict(row) for row in rows],
    }


def write_digest_json(payload: dict[str, _JsonValue], path: Path) -> None:
    """Ghi payload ra file JSON (UTF-8, không escape tiếng Việt thành \\uXXXX cho dễ đọc).

    Tạo thư mục cha nếu chưa có. `sort_keys=False` — key giữ đúng thứ tự dựng trong
    `build_digest_payload`/`_row_to_dict`, ổn định giữa các lần chạy nên hash file không đổi
    khi dữ liệu nguồn không đổi (DONE WHEN: chạy 2 lần → file giống hệt).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=False)
        fh.write("\n")
