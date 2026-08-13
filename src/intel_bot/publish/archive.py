"""Dọn `docs-site/archive/*.json` cũ hơn `archive_days` (task 1.10, §12.2, D7).

Chỉ xoá FILE EXPORT tĩnh — dữ liệu gốc vẫn nguyên trong Postgres (`gold.mart_daily_digest`
build lại mỗi ngày từ `fct_article_score`, không phụ thuộc các file JSON này). `archive_days`
trước task này chỉ nằm trong `config/app.yaml` để tài liệu hoá (§12.2), không có code nào đọc
— hàm này là chỗ DUY NHẤT áp dụng nó vào hành vi thật.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path


def prune_archive(
    docs_site_dir: Path, *, archive_days: int, today: dt.date
) -> list[Path]:
    """Xoá file `archive/YYYY-MM-DD.json` có ngày (suy từ TÊN FILE, không phải mtime — tên
    file archive luôn là ngày digest đã build, đáng tin hơn thời điểm ghi đĩa) cũ hơn
    `archive_days` so với `today`. Trả về danh sách file đã xoá (rỗng nếu không có gì cũ).

    File tên không đúng định dạng `YYYY-MM-DD.json` bị BỎ QUA (không đoán/không xoá nhầm) —
    không nên xảy ra trong vận hành bình thường (chỉ `run_publish()` ghi vào thư mục này),
    nhưng an toàn hơn là raise nếu ai đó vô tình để lẫn file khác vào.
    """
    archive_dir = docs_site_dir / "archive"
    if not archive_dir.exists():
        return []

    cutoff = today - dt.timedelta(days=archive_days)
    removed: list[Path] = []
    for file_path in sorted(archive_dir.glob("*.json")):
        try:
            file_date = dt.date.fromisoformat(file_path.stem)
        except ValueError:
            continue
        if file_date < cutoff:
            file_path.unlink()
            removed.append(file_path)
    return removed
