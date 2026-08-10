"""Unit test cho compute_payload_hash — không gọi DB, không gọi mạng."""

from __future__ import annotations

from src.intel_bot.ingest.rss_fetcher import compute_payload_hash


def test_payload_hash_same_content_different_key_order() -> None:
    """Cùng nội dung, khác thứ tự key trong dict Python → cùng hash."""
    payload_a = {
        "title": "Hello",
        "link": "https://x.test/a",
        "published": "2026-01-01T00:00:00Z",
    }
    payload_b = {
        "published": "2026-01-01T00:00:00Z",
        "link": "https://x.test/a",
        "title": "Hello",
    }
    assert compute_payload_hash(payload_a) == compute_payload_hash(payload_b)


def test_payload_hash_nested_structures_order_independent() -> None:
    """Thứ tự key trong dict lồng nhau cũng không ảnh hưởng hash."""
    payload_a = {"tags": [{"term": "ai", "label": "AI"}], "title": "X"}
    payload_b = {"title": "X", "tags": [{"label": "AI", "term": "ai"}]}
    assert compute_payload_hash(payload_a) == compute_payload_hash(payload_b)


def test_payload_hash_different_content_gives_different_hash() -> None:
    """Nội dung khác nhau phải cho hash khác nhau (sanity — tránh hash hằng số)."""
    payload_a = {"title": "Hello"}
    payload_b = {"title": "World"}
    assert compute_payload_hash(payload_a) != compute_payload_hash(payload_b)
