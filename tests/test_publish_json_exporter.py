"""Test xuất JSON (PRODUCTION_PLAN §12.1, §12.2) — hàm THUẦN cho payload, chỉ ghi file
qua `write_digest_json` (kiểm bằng `tmp_path`, không đụng docs-site/ thật)."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal
from pathlib import Path

from src.intel_bot.publish.digest_reader import DigestRow
from src.intel_bot.publish.json_exporter import build_digest_payload, write_digest_json

NOW = dt.datetime(2026, 8, 11, 9, 0, 0, tzinfo=dt.UTC)


def _make_row(slug: str = "row-1") -> DigestRow:
    return DigestRow(
        score_id=uuid.uuid5(uuid.NAMESPACE_URL, f"score/{slug}"),
        article_id=uuid.uuid5(uuid.NAMESPACE_URL, f"article/{slug}"),
        canonical_url=f"https://fixture.test/{slug}",
        title="Tiêu đề tiếng Việt có dấu",
        snippet="Snippet",
        industry_tags=["ai", "tech"],
        source_id="fixture_source",
        source_domain="fixture.test",
        source_tier=1,
        published_at=NOW,
        published_at_imputed=False,
        first_seen_at=NOW,
        credibility_blended=Decimal("8.40"),
        importance=8,
        practicality=6,
        depth=5,
        recency_boost=Decimal("0.5"),
        composite_score=Decimal("7.42"),
        summary_vi=[f"Bullet {i}" for i in range(5)],
        why_it_matters_vi="Lý do quan trọng.",
        industry_group="ai",
        digest_built_at=NOW,
    )


def test_build_digest_payload_shape_and_types() -> None:
    row = _make_row()
    payload = build_digest_payload([row], generated_for_date=dt.date(2026, 8, 11))

    assert payload["generated_for_date"] == "2026-08-11"
    assert payload["article_count"] == 1
    assert payload["digest_built_at"] == NOW.isoformat()

    articles = payload["articles"]
    assert isinstance(articles, list)
    article = articles[0]
    assert article["article_id"] == str(row.article_id)
    assert article["score_id"] == str(row.score_id)
    assert article["composite_score"] == 7.42
    assert isinstance(article["composite_score"], float)
    assert article["summary_vi"] == row.summary_vi
    assert article["published_at"] == NOW.isoformat()


def test_build_digest_payload_empty_rows() -> None:
    payload = build_digest_payload([], generated_for_date=dt.date(2026, 8, 11))
    assert payload["article_count"] == 0
    assert payload["articles"] == []
    assert payload["digest_built_at"] is None


def test_write_digest_json_produces_valid_parseable_json(tmp_path: Path) -> None:
    payload = build_digest_payload(
        [_make_row()], generated_for_date=dt.date(2026, 8, 11)
    )
    out_path = tmp_path / "nested" / "articles.json"
    write_digest_json(payload, out_path)

    assert out_path.exists()
    with out_path.open("r", encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded == payload


def test_write_digest_json_keeps_vietnamese_readable_not_escaped(
    tmp_path: Path,
) -> None:
    payload = build_digest_payload(
        [_make_row()], generated_for_date=dt.date(2026, 8, 11)
    )
    out_path = tmp_path / "articles.json"
    write_digest_json(payload, out_path)

    raw = out_path.read_text(encoding="utf-8")
    assert "Tiêu đề tiếng Việt có dấu" in raw
    assert "\\u" not in raw
