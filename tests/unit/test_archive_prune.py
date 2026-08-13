"""Test cho `src/intel_bot/publish/archive.py` (task 1.10, §12.2, D7) — thuần filesystem,
không cần Postgres (đúng nghĩa "unit": `prune_archive()` không nhận `connection` nào)."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.intel_bot.publish.archive import prune_archive

TODAY = dt.date(2026, 8, 13)


def _make_archive(docs_site_dir: Path, name: str) -> Path:
    archive_dir = docs_site_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    path = archive_dir / name
    path.write_text("{}", encoding="utf-8")
    return path


def test_prune_removes_files_older_than_archive_days(tmp_path: Path) -> None:
    old = _make_archive(tmp_path, "2026-07-01.json")  # 43 ngày trước TODAY
    within = _make_archive(tmp_path, "2026-08-10.json")  # 3 ngày trước TODAY

    removed = prune_archive(tmp_path, archive_days=7, today=TODAY)

    assert removed == [old]
    assert not old.exists()
    assert within.exists()


def test_prune_keeps_file_exactly_at_cutoff_boundary(tmp_path: Path) -> None:
    # cutoff = TODAY - 7 ngày = 2026-08-06. File ĐÚNG bằng cutoff không bị xoá (điều kiện
    # `< cutoff`, không phải `<=`) — file của "hôm cutoff" vẫn còn trong hạn 7 ngày.
    boundary = _make_archive(tmp_path, "2026-08-06.json")
    just_before = _make_archive(tmp_path, "2026-08-05.json")

    removed = prune_archive(tmp_path, archive_days=7, today=TODAY)

    assert removed == [just_before]
    assert boundary.exists()
    assert not just_before.exists()


def test_prune_no_archive_dir_returns_empty(tmp_path: Path) -> None:
    assert prune_archive(tmp_path, archive_days=7, today=TODAY) == []


def test_prune_nothing_old_removes_nothing(tmp_path: Path) -> None:
    recent = _make_archive(tmp_path, "2026-08-12.json")

    removed = prune_archive(tmp_path, archive_days=7, today=TODAY)

    assert removed == []
    assert recent.exists()


def test_prune_ignores_files_with_non_date_names(tmp_path: Path) -> None:
    weird = _make_archive(tmp_path, "not-a-date.json")

    removed = prune_archive(tmp_path, archive_days=7, today=TODAY)

    assert removed == []
    assert weird.exists()
