"""Test render HTML (PRODUCTION_PLAN §12.4) — hàm THUẦN, không cần Postgres: nhận
`list[DigestRow]` fixture cố định, dựng bằng template thật ở `templates/index.html.j2`.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from pathlib import Path

from src.intel_bot.publish.digest_reader import DigestRow
from src.intel_bot.publish.html_renderer import (
    build_jinja_environment,
    render_digest_html,
)

TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"
NOW = dt.datetime(2026, 8, 11, 9, 0, 0, tzinfo=dt.UTC)


def _make_row(
    *,
    slug: str,
    title: str = "Tiêu đề test",
    snippet: str = "Snippet test",
    industry_group: str = "ai",
    why_it_matters_vi: str
    | None = "Lý do quan trọng test, đủ dài để hợp lệ theo contract.",
) -> DigestRow:
    return DigestRow(
        score_id=uuid.uuid5(uuid.NAMESPACE_URL, f"score/{slug}"),
        article_id=uuid.uuid5(uuid.NAMESPACE_URL, f"article/{slug}"),
        canonical_url=f"https://fixture.test/{slug}",
        title=title,
        snippet=snippet,
        industry_tags=[industry_group],
        source_id=f"source_{slug}",
        source_domain="fixture.test",
        source_tier=1,
        published_at=NOW,
        published_at_imputed=False,
        first_seen_at=NOW,
        credibility_blended=Decimal("8.4"),
        importance=8,
        practicality=6,
        depth=5,
        recency_boost=Decimal("0.5"),
        composite_score=Decimal("7.42"),
        summary_vi=[f"Gạch đầu dòng {i} cho {slug}" for i in range(5)],
        why_it_matters_vi=why_it_matters_vi,
        industry_group=industry_group,
        digest_built_at=NOW,
    )


def _render(rows: list[DigestRow]) -> str:
    env = build_jinja_environment(TEMPLATES_DIR)
    return render_digest_html(
        env,
        rows,
        generated_for_date=dt.date(2026, 8, 11),
        repo_url="https://github.com/quybuno/industry-intel-bot",
    )


def test_render_has_correct_article_card_count() -> None:
    rows = [_make_row(slug=f"article-{i}", industry_group="ai") for i in range(3)] + [
        _make_row(slug="construction-1", industry_group="construction")
    ]
    html = _render(rows)
    assert html.count('<article class="card"') == 4
    assert "Số bài: 4" in html


def test_render_groups_articles_by_industry_section() -> None:
    rows = [
        _make_row(slug="ai-1", industry_group="ai"),
        _make_row(slug="hvac-1", industry_group="hvac"),
    ]
    html = _render(rows)
    ai_pos = html.index(">AI<")
    hvac_pos = html.index(">HVAC<")
    # Thứ tự section cố định theo §12.4: AI trước HVAC.
    assert ai_pos < hvac_pos


def test_render_contains_five_summary_bullets_and_why_it_matters() -> None:
    row = _make_row(slug="bullets-1")
    html = _render([row])
    for bullet in row.summary_vi:
        assert bullet in html
    assert row.why_it_matters_vi is not None
    assert row.why_it_matters_vi in html
    assert "Tại sao quan trọng" in html


def test_render_escapes_html_and_preserves_vietnamese_text() -> None:
    dangerous_title = 'Tiêu đề <script>alert("xss")</script> & "trích dẫn" Đà Nẵng'
    row = _make_row(slug="escape-1", title=dangerous_title)
    html = _render([row])

    # Không được lọt nguyên văn thẻ script hay ký tự & chưa escape vào HTML.
    assert "<script>alert(" not in html
    assert "&lt;script&gt;" in html
    assert "&amp;" in html

    # Tiếng Việt có dấu phải giữ nguyên, không bị escape thành HTML entity hay mangled.
    assert "Đà Nẵng" in html


def test_render_checkbox_and_copy_button_present_per_article() -> None:
    row = _make_row(slug="widgets-1")
    html = _render([row])
    article_id = str(row.article_id)
    assert f'data-article-id="{article_id}"' in html
    assert 'class="read-checkbox"' in html
    assert 'class="copy-id-btn"' in html
    # JS đọc/ghi localStorage cho checkbox "đã đọc" — không backend.
    assert "localStorage" in html


def test_render_footer_has_ai_disclaimer_and_repo_link() -> None:
    html = _render([_make_row(slug="footer-1")])
    assert "AI sinh tự động" in html
    assert "https://github.com/quybuno/industry-intel-bot" in html


def test_render_empty_digest_still_produces_valid_shell() -> None:
    html = _render([])
    assert "Số bài: 0" in html
    assert html.count('<article class="card"') == 0
