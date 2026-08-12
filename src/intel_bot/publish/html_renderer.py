"""Render `gold.mart_daily_digest` thành trang HTML tĩnh (PRODUCTION_PLAN §12.1, §12.4).

`render_digest_html` là hàm THUẦN theo nghĩa: với cùng `rows` + `generated_for_date` +
nội dung template trên đĩa không đổi, luôn ra cùng một chuỗi HTML — không tự truy vấn DB,
không sắp xếp/lọc/dedup/nhóm lại theo tiêu chí MỚI (mart đã quyết hết, §12.1). Việc nhóm
theo `industry_group` dưới đây chỉ là BUCKET theo cột đã có sẵn để dựng section HTML, giữ
nguyên thứ tự các bài trong mỗi nhóm đúng như mart đã sắp (không gọi `sort`/`ORDER BY` mới).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import jinja2

from src.intel_bot.publish.digest_reader import DigestRow

#: Thứ tự + nhãn hiển thị 5 section ngành cố định theo PRODUCTION_PLAN §12.4. Đây là cấu
#: trúc TRANG (UI), không phải tiêu chí xếp hạng bài — khác với composite_score (mart quyết
#: định) hay industry_group (mart gán). 'uncategorized' là fallback đã có sẵn ở mart
#: (coalesce industry_tags[1], xem mart_daily_digest.sql).
INDUSTRY_SECTION_ORDER: tuple[str, ...] = (
    "ai",
    "construction",
    "hvac",
    "manufacturing",
    "iot",
    "uncategorized",
)
INDUSTRY_LABELS: dict[str, str] = {
    "ai": "AI",
    "construction": "Construction",
    "hvac": "HVAC",
    "manufacturing": "Manufacturing",
    "iot": "IoT",
    "uncategorized": "Khác",
}

#: Composite score tối đa lý thuyết (§5.7): importance*0.4 + practicality*0.3 +
#: credibility_blended*0.3, mỗi thành phần tối đa 10, cộng recency_boost tối đa 1.0
#: (0.4+0.3+0.3)*10 + 1.0 = 11.0. Dùng để vẽ thanh điểm 0-100%, KHÔNG dùng để xếp hạng lại.
_COMPOSITE_SCORE_MAX = Decimal("11.0")

_VN_MONTHS_FMT = "%d/%m/%Y"
_VN_DATETIME_FMT = "%d/%m/%Y %H:%M"


@dataclass(frozen=True)
class ArticleCardView:
    """Dữ liệu ĐÃ ĐỊNH DẠNG cho một article card — chỉ format hiển thị (ngày, %, nhãn tier),
    không phải quyết định nghiệp vụ nào mới."""

    article_id: str
    score_id: str
    title: str
    url: str
    snippet: str
    source_domain: str
    source_tier_label: str
    published_display: str
    composite_score_display: str
    score_bar_percent: float
    summary_vi: list[str]
    why_it_matters_vi: str


@dataclass(frozen=True)
class IndustrySection:
    """Một section ngành trên trang — nhãn hiển thị + danh sách card theo ĐÚNG thứ tự đã
    có trong `rows` (không sort lại)."""

    label: str
    articles: list[ArticleCardView]


def _format_score_bar_percent(composite_score: Decimal) -> float:
    ratio = composite_score / _COMPOSITE_SCORE_MAX
    clamped = max(Decimal(0), min(Decimal(1), ratio))
    return float(clamped * 100)


def _to_card_view(row: DigestRow) -> ArticleCardView:
    if row.published_at is not None and not row.published_at_imputed:
        published_display = row.published_at.strftime(_VN_DATETIME_FMT)
    else:
        # published_at NULL (hoặc bị suy luận, §8.2/§5.7) — hiển thị first_seen_at kèm chú
        # thích, KHÔNG giả vờ đó là mốc xuất bản gốc.
        published_display = (
            f"{row.first_seen_at.strftime(_VN_DATETIME_FMT)} (thời điểm thu thập)"
        )

    return ArticleCardView(
        article_id=str(row.article_id),
        score_id=str(row.score_id),
        title=row.title,
        url=row.canonical_url,
        snippet=row.snippet or "",
        source_domain=row.source_domain or "Không rõ nguồn",
        source_tier_label=(
            f"Tier {row.source_tier}"
            if row.source_tier is not None
            else "Chưa xếp tier"
        ),
        published_display=published_display,
        composite_score_display=f"{row.composite_score:.1f}",
        score_bar_percent=_format_score_bar_percent(row.composite_score),
        summary_vi=row.summary_vi,
        why_it_matters_vi=row.why_it_matters_vi or "",
    )


def _group_by_industry(rows: list[DigestRow]) -> list[IndustrySection]:
    buckets: dict[str, list[ArticleCardView]] = {}
    for row in rows:
        buckets.setdefault(row.industry_group, []).append(_to_card_view(row))

    sections: list[IndustrySection] = []
    for key in INDUSTRY_SECTION_ORDER:
        if key in buckets:
            sections.append(
                IndustrySection(label=INDUSTRY_LABELS[key], articles=buckets.pop(key))
            )
    # Nhóm ngành lạ (ngoài 5 nhóm + 'uncategorized') không nên xảy ra vì INDUSTRY_TAGS là
    # tập đóng (contracts/llm_score.py) — nhưng nếu có, hiển thị luôn thay vì âm thầm bỏ
    # bài (P4 tinh thần: rõ ràng hơn là che giấu).
    for key, articles in buckets.items():
        sections.append(IndustrySection(label=key, articles=articles))
    return sections


def build_jinja_environment(templates_dir: Path) -> jinja2.Environment:
    """Tạo Jinja2 Environment đọc template từ đĩa (I/O — nhận thư mục qua tham số, không
    hardcode, để test trỏ được vào thư mục fixture riêng)."""
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(templates_dir)),
        autoescape=True,  # bắt buộc — tiêu đề/snippet là dữ liệu bên ngoài (§19.2, rào chắn task 0.11)
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render_digest_html(
    env: jinja2.Environment,
    rows: list[DigestRow],
    *,
    generated_for_date: dt.date,
    repo_url: str,
) -> str:
    """Render `index.html.j2` — hàm thuần với `env` + `rows` + `generated_for_date` cố định.

    `digest_built_at` hiển thị ở header lấy từ cột cùng tên trong mart (giống mọi dòng) —
    không đọc bảng nào khác (rào chắn task 0.11).
    """
    template = env.get_template("index.html.j2")
    digest_built_at_display = (
        rows[0].digest_built_at.strftime(_VN_DATETIME_FMT) if rows else "—"
    )
    return template.render(
        generated_for_date_display=generated_for_date.strftime(_VN_MONTHS_FMT),
        digest_built_at_display=digest_built_at_display,
        article_count=len(rows),
        sections=_group_by_industry(rows),
        repo_url=repo_url,
    )
