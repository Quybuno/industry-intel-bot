"""Orchestration cho publish job (PRODUCTION_PLAN §12.1): truy vấn → render → ghi file →
cập nhật `last_published_at`. Không có business logic — mọi lựa chọn/dedup/xếp hạng/nhóm
ngành đã xong ở `gold.mart_daily_digest` (dbt, task 0.10/0.11).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import sqlalchemy as sa

from src.intel_bot.publish.digest_reader import fetch_digest_rows, mark_published
from src.intel_bot.publish.html_renderer import (
    build_jinja_environment,
    render_digest_html,
)
from src.intel_bot.publish.json_exporter import build_digest_payload, write_digest_json


@dataclass
class PublishResult:
    """Thống kê một lần chạy publish."""

    article_count: int
    index_html_path: Path
    articles_json_path: Path
    archive_json_path: Path
    articles_marked_published: int


def run_publish(
    connection: sa.Connection,
    *,
    generated_for_date: dt.date,
    docs_site_dir: Path,
    templates_dir: Path,
    repo_url: str,
    now: dt.datetime,
) -> PublishResult:
    """Chạy publish cho một ngày: đọc `gold.mart_daily_digest`, ghi `articles.json` +
    `archive/YYYY-MM-DD.json` + `index.html`, rồi cập nhật `last_published_at`.

    `generated_for_date` chỉ dùng để đặt tên file archive và hiển thị ở header — KHÔNG lọc
    lại `gold.mart_daily_digest` (mart tự quyết định cửa sổ 48h khi build, task 0.10/0.11,
    rào chắn: publish không thêm điều kiện lọc/sắp xếp nào trong Python). `now` dùng RIÊNG
    cho `last_published_at` (mốc thời điểm thật của lần publish này) — tách khỏi
    `generated_for_date` để chạy lại cùng một ngày nhiều lần vẫn ra file giống hệt (DONE
    WHEN), trong khi `last_published_at` vẫn phản ánh đúng lần cập nhật gần nhất.
    """
    rows = fetch_digest_rows(connection)

    payload = build_digest_payload(rows, generated_for_date=generated_for_date)
    articles_json_path = docs_site_dir / "articles.json"
    archive_json_path = (
        docs_site_dir / "archive" / f"{generated_for_date.isoformat()}.json"
    )
    write_digest_json(payload, articles_json_path)
    write_digest_json(payload, archive_json_path)

    env = build_jinja_environment(templates_dir)
    html = render_digest_html(
        env, rows, generated_for_date=generated_for_date, repo_url=repo_url
    )
    index_html_path = docs_site_dir / "index.html"
    index_html_path.parent.mkdir(parents=True, exist_ok=True)
    with index_html_path.open("w", encoding="utf-8") as fh:
        fh.write(html)

    published_count = mark_published(
        connection,
        article_ids=[row.article_id for row in rows],
        published_at=now,
    )
    connection.commit()

    return PublishResult(
        article_count=len(rows),
        index_html_path=index_html_path,
        articles_json_path=articles_json_path,
        archive_json_path=archive_json_path,
        articles_marked_published=published_count,
    )
