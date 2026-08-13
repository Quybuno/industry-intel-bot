"""Commit + push `docs-site/` lên nhánh GitHub Pages sau khi publish (task 1.10, §12.1, D6).

**Vì sao một nhánh riêng (`gh-pages`), không phải push thẳng `docs-site/` trên `main`:**
GitHub Pages CHỈ serve được từ root (`/`) hoặc thư mục `/docs` của một nhánh — không hỗ trợ
tên thư mục tuỳ ý như `docs-site/` (verify thật bằng cách gọi `POST .../pages` với
`path=/docs-site` → GitHub từ chối, không phải suy đoán). Đổi tên `docs-site/` → `docs/`
trên `main` sẽ đơn giản hơn nhưng phải sửa lại quy ước đang dùng khắp repo (config,
docstring, README, CLI help) chỉ để phục vụ một quyết định hạ tầng — rủi ro/diff không
tương xứng. Thay vào đó: `docs-site/` trên `main` VẪN là nơi Dagster/CLI ghi file cục bộ
(không đổi convention), publish job đồng bộ NỘI DUNG của nó sang ROOT của nhánh `gh-pages`
riêng (qua `git worktree`) — nhánh này chỉ có 1 mục đích DUY NHẤT: GitHub Pages serve từ đó.

**`git worktree` thay vì `git subtree`:** cả hai đều giải quyết được bài toán "một thư mục
con → root của nhánh khác", nhưng `git subtree` thao tác trên LỊCH SỬ COMMIT (cần commit
docs-site/ trên `main` trước rồi mới subtree-push, tạo commit kép không cần thiết cho một
thư mục toàn file sinh ra tự động — không ai cần xem lại lịch sử "site build" trên `main`).
`git worktree` chỉ là một checkout thứ hai của CÙNG repo trỏ nhánh khác, dùng chung
`.git/objects` — rẻ hơn, không tạo commit thừa trên `main`, và mã nguồn/deploy-artifact tách
bạch rõ ràng: `main` có source, `gh-pages` có build output.

**Bootstrap 1 lần, KHÔNG tự động:** nhánh `gh-pages` phải đã tồn tại trên remote (tạo tay
1 lần lúc deploy, xem `docs/RUNBOOK.md`) trước khi hàm này chạy lần đầu — cố tình không tự
tạo orphan branch ở runtime để tránh một nhánh logic bootstrap chỉ chạy đúng 1 lần rồi không
bao giờ dùng lại, làm phức tạp code production cho một sự kiện hiếm (P8).

**PAT KHÔNG BAO GIỜ ghi vào git config/log/commit message** (rào chắn task 1.10): token chỉ
xuất hiện trong URL truyền TRỰC TIẾP cho lệnh `git push <url> <refspec>` (không phải
`git remote set-url` — không đụng `.git/config`), và MỌI output (`stdout`/`stderr`) của lệnh
push đều được redact token trước khi log/trả về, đề phòng git tự in lại URL lỗi vào thông
báo (gặp thật trên các loại lỗi "repository not found"). Token vẫn có thể thoáng qua trong
argv của tiến trình con (`ps`/`/proc` trên máy nhiều người dùng) trong vài trăm ms — chấp
nhận được cho quy mô 1 người dùng/máy riêng của dự án này (P8); không dùng `GIT_ASKPASS`
phức tạp hơn cho một rủi ro thực tế rất nhỏ ở quy mô này.
"""

from __future__ import annotations

import dataclasses
import shutil
import subprocess
from pathlib import Path


@dataclasses.dataclass(frozen=True)
class GitPublishResult:
    """Kết quả một lần commit+push docs-site/. `pushed=False` với `error=None` nghĩa là
    SKIP hợp lệ (không có gì đổi, P1) — KHÔNG phải lỗi."""

    pushed: bool
    commit_sha: str | None
    skipped_no_changes: bool
    error: str | None


def _run_git(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )


def _redact(text: str, secret: str) -> str:
    """Xoá token khỏi output trước khi log/trả về — git có thể tự in lại URL lỗi (vd.
    "repository 'https://x-access-token:<token>@...' not found") vào stderr."""
    if not secret:
        return text
    return text.replace(secret, "***REDACTED***")


def ensure_worktree(repo_root: Path, worktree_dir: Path, branch: str) -> None:
    """Đảm bảo worktree cục bộ trỏ nhánh GitHub Pages tồn tại. KHÔNG tự tạo nhánh trên remote
    nếu chưa có — xem giải thích "Bootstrap 1 lần" ở docstring module.

    `git fetch` trước khi kiểm tra `origin/<branch>` — cần thiết cho lần chạy ĐẦU TIÊN sau
    một `git clone` mới (repo mới clone không tự có remote-tracking ref của nhánh vừa tạo
    trên remote cho tới khi fetch), không chỉ máy dev đã fetch sẵn."""
    if worktree_dir.exists():
        return
    fetch_result = _run_git(["fetch", "origin", branch], cwd=repo_root)
    if fetch_result.returncode != 0:
        raise RuntimeError(
            f"git fetch origin '{branch}' thất bại — kiểm tra mạng/quyền remote. "
            f"stderr: {fetch_result.stderr.strip()}"
        )
    result = _run_git(
        [
            "worktree",
            "add",
            "--track",
            "-B",
            branch,
            str(worktree_dir),
            f"origin/{branch}",
        ],
        cwd=repo_root,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git worktree add thất bại cho nhánh '{branch}' — nhánh này phải được tạo THẬT "
            f"trên remote trước (xem docs/RUNBOOK.md, bước bootstrap gh-pages). "
            f"stderr: {result.stderr.strip()}"
        )


def _sync_docs_site_to_worktree(docs_site_dir: Path, worktree_dir: Path) -> None:
    """Đồng bộ NỘI DUNG `docs_site_dir` (không phải chính thư mục đó) vào ROOT của worktree —
    xoá sạch trước (trừ `.git`) rồi copy lại, để lần chạy nào cũng phản ánh ĐÚNG trạng thái
    hiện tại của docs-site/ (kể cả khi archive pruning đã xoá bớt file, D7)."""
    for item in worktree_dir.iterdir():
        if item.name == ".git":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    for item in docs_site_dir.iterdir():
        dest = worktree_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)


def commit_and_push_docs_site(
    *,
    repo_root: Path,
    docs_site_dir: Path,
    worktree_dir: Path,
    branch: str,
    pat: str,
    commit_message: str,
) -> GitPublishResult:
    """Đồng bộ `docs_site_dir` vào worktree nhánh `branch`, commit + push NẾU có thay đổi.

    Không có thay đổi (nội dung giống hệt lần publish trước, P1) → `skipped_no_changes=True`,
    KHÔNG tạo commit rỗng. Push lỗi (PAT hết hạn, mất mạng) → `error` khác `None`, hàm này
    KHÔNG raise — bên gọi (asset/CLI) tự quyết định log + gửi alert, không fail chính nó vì
    file cục bộ đã ghi thành công (rào chắn task 1.10 mục 1).
    """
    ensure_worktree(repo_root, worktree_dir, branch)
    _sync_docs_site_to_worktree(docs_site_dir, worktree_dir)

    status = _run_git(["status", "--porcelain"], cwd=worktree_dir)
    if not status.stdout.strip():
        return GitPublishResult(
            pushed=False, commit_sha=None, skipped_no_changes=True, error=None
        )

    add_result = _run_git(["add", "-A"], cwd=worktree_dir)
    if add_result.returncode != 0:
        return GitPublishResult(
            pushed=False,
            commit_sha=None,
            skipped_no_changes=False,
            error=f"git add thất bại: {add_result.stderr.strip()}",
        )

    commit_result = _run_git(["commit", "-m", commit_message], cwd=worktree_dir)
    if commit_result.returncode != 0:
        return GitPublishResult(
            pushed=False,
            commit_sha=None,
            skipped_no_changes=False,
            error=f"git commit thất bại: {commit_result.stderr.strip()}",
        )

    remote_result = _run_git(["remote", "get-url", "origin"], cwd=worktree_dir)
    if remote_result.returncode != 0:
        return GitPublishResult(
            pushed=False,
            commit_sha=None,
            skipped_no_changes=False,
            error=f"Không đọc được remote 'origin': {remote_result.stderr.strip()}",
        )
    remote_url = remote_result.stdout.strip()
    # Chỉ nhúng PAT nếu remote thật sự là HTTPS — remote SSH (`git@...`)/local (`file://`,
    # dùng trong test tích hợp của chính module này) không cần và không hiểu cú pháp
    # `x-access-token:<pat>@`, tự xác thực bằng cơ chế riêng (SSH key/quyền file cục bộ).
    push_url = (
        remote_url.replace("https://", f"https://x-access-token:{pat}@", 1)
        if remote_url.startswith("https://")
        else remote_url
    )

    push_result = _run_git(["push", push_url, f"HEAD:{branch}"], cwd=worktree_dir)
    if push_result.returncode != 0:
        return GitPublishResult(
            pushed=False,
            commit_sha=None,
            skipped_no_changes=False,
            error=_redact(f"git push thất bại: {push_result.stderr.strip()}", pat),
        )

    sha_result = _run_git(["rev-parse", "HEAD"], cwd=worktree_dir)
    commit_sha = sha_result.stdout.strip() if sha_result.returncode == 0 else None

    return GitPublishResult(
        pushed=True, commit_sha=commit_sha, skipped_no_changes=False, error=None
    )
