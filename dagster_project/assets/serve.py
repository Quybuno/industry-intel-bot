"""Asset `published_site` (serve, không partition) — bọc lại `run_publish()` (task 0.11),
rồi prune archive cũ + commit/push `docs-site/` lên nhánh GitHub Pages (task 1.10, §12.1,
§12.2, D6/D7), rồi ping heartbeat ra ngoài SAU KHI publish thành công (PRODUCTION_PLAN §7.5,
task 0.13 mục 7). Đây là bước DUY NHẤT gọi `NotifierResource` ở Phase 0 — không có sensor nào
khác (rào chắn task 0.12: sensor để lại Phase 1).

**Push docs-site/ lỗi (PAT hết hạn, mất mạng) chỉ log error + gửi alert, KHÔNG fail asset**
(rào chắn task 1.10 mục 1) — file cục bộ đã ghi thành công, dữ liệu không mất, chỉ trang công
khai chưa cập nhật; đây là tình huống có sẵn ở `docs/RUNBOOK.md` ("Git push bị từ chối").

**Không có `from __future__ import annotations`** — xem giải thích ở `assets/bronze.py`.
"""

import datetime as dt
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dagster import (
    AssetExecutionContext,
    Failure,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_project.resources.notifier import NotifierResource
from dagster_project.resources.postgres import PostgresResource
from src.intel_bot.config import load_config_dir, settings
from src.intel_bot.publish.archive import prune_archive
from src.intel_bot.publish.git_publish import commit_and_push_docs_site
from src.intel_bot.publish.runner import run_publish

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


@asset(
    key="published_site",
    group_name="serve",
    deps=["mart_daily_digest"],
    description=(
        "Publish JSON + HTML tĩnh từ gold.mart_daily_digest (task 0.11), sau đó ping "
        "heartbeat (§7.5)."
    ),
)
def published_site(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    notifier: NotifierResource,
) -> MaterializeResult[Any]:
    """Không partition — khớp bảng asset gốc §7.2 (`published_site` | partition `—`):
    `mart_daily_digest` là cửa sổ 48h tự quyết bởi dbt, không có khái niệm "publish lại cho
    một ngày quá khứ cụ thể" (đã ghi rõ ở `run_publish()` — §12.2, task 0.11). Vì vậy
    `generated_for_date` dùng ngày hôm nay THẬT (`datetime.now`), không lấy từ partition
    key — asset này không có partition key để lấy."""
    now = dt.datetime.now(tz=VN_TZ)
    generated_for_date = now.date()

    publish_cfg = load_config_dir().get("app", {}).get("publish", {})
    repo_url = publish_cfg.get("repo_url")
    if not repo_url:
        raise Failure(
            "Thiếu config app.yaml: publish.repo_url — không tự bịa link repo."
        )
    docs_site_dir = Path(publish_cfg.get("docs_site_dir", "docs-site"))
    templates_dir = Path(publish_cfg.get("templates_dir", "templates"))
    archive_days = int(publish_cfg.get("archive_days", 7))
    gh_pages_branch = publish_cfg.get("gh_pages_branch", "gh-pages")
    worktree_dir = REPO_ROOT / publish_cfg.get(
        "gh_pages_worktree_dir", ".gh-pages-worktree"
    )

    started_at = time.monotonic()
    with postgres.get_connection() as connection:
        result = run_publish(
            connection,
            generated_for_date=generated_for_date,
            docs_site_dir=docs_site_dir,
            templates_dir=templates_dir,
            repo_url=repo_url,
            now=now,
        )
    duration_seconds = time.monotonic() - started_at

    # D7 (§12.2) — dọn archive cũ TRƯỚC khi commit/push, để lần push này phản ánh đúng
    # trạng thái docs-site/ sau khi đã xoá (không push rồi mới xoá ở lần chạy sau).
    removed_archives = prune_archive(
        docs_site_dir, archive_days=archive_days, today=generated_for_date
    )
    if removed_archives:
        context.log.info(
            f"Đã xoá {len(removed_archives)} file archive cũ hơn {archive_days} ngày."
        )

    # D6 (§12.1) — commit + push docs-site/ lên nhánh GitHub Pages. GIT_PUBLISH_TOKEN thiếu
    # → coi như lỗi cấu hình rõ ràng (log + alert), KHÔNG bịa/bỏ qua âm thầm, nhưng vẫn không
    # fail asset (file cục bộ đã ghi xong, đúng rào chắn task 1.10 mục 1).
    git_push_error: str | None = None
    if not settings.GIT_PUBLISH_TOKEN:
        git_push_error = (
            "Thiếu biến môi trường GIT_PUBLISH_TOKEN — không commit/push docs-site/."
        )
        context.log.warning(git_push_error)
    else:
        # `commit_and_push_docs_site()` có thể RAISE (vd. `ensure_worktree()` khi nhánh
        # gh-pages chưa bootstrap — lỗi cấu hình, không phải lỗi tạm thời), khác với lỗi PUSH
        # (PAT hết hạn/mất mạng) mà bản thân hàm đã tự trả về qua `.error`. Bắt CẢ HAI ở đây
        # vì rào chắn task 1.10 mục 1 không phân biệt "loại" lỗi git-publish — bất kỳ lỗi nào
        # ở bước này cũng KHÔNG được làm fail asset (heartbeat vẫn phải ping bên dưới dù bước
        # này lỗi kiểu gì).
        try:
            git_result = commit_and_push_docs_site(
                repo_root=REPO_ROOT,
                docs_site_dir=docs_site_dir,
                worktree_dir=worktree_dir,
                branch=gh_pages_branch,
                pat=settings.GIT_PUBLISH_TOKEN,
                commit_message=f"publish: digest {generated_for_date.isoformat()}",
            )
        except Exception as exc:  # noqa: BLE001 — cố ý bắt rộng, xem giải thích ở trên
            git_push_error = str(exc)
            context.log.warning(f"Push docs-site/ thất bại: {git_push_error}")
        else:
            if git_result.error:
                git_push_error = git_result.error
                context.log.warning(f"Push docs-site/ thất bại: {git_push_error}")
            elif git_result.skipped_no_changes:
                context.log.info("docs-site/ không đổi — bỏ qua commit (P1).")
            else:
                context.log.info(f"Đã push docs-site/ ({git_result.commit_sha}).")

    if git_push_error:
        notifier.send_alert(
            f"⚠️ Push docs-site/ lên GitHub Pages thất bại: {git_push_error}",
            logger=context.log,
        )

    # Heartbeat SAU KHI publish thành công — lỗi ping chỉ log warning, không fail asset
    # (rào chắn task 0.13 mục 7; xem docstring NotifierResource.ping_heartbeat).
    notifier.ping_heartbeat(logger=context.log)

    return MaterializeResult(
        metadata={
            "article_count": MetadataValue.int(result.article_count),
            "articles_marked_published": MetadataValue.int(
                result.articles_marked_published
            ),
            "index_html_path": MetadataValue.path(str(result.index_html_path)),
            "archives_pruned": MetadataValue.int(len(removed_archives)),
            "git_push_error": MetadataValue.text(git_push_error or ""),
            "duration_seconds": MetadataValue.float(round(duration_seconds, 3)),
        }
    )
