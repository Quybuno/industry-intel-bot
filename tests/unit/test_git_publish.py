"""Test cho `src/intel_bot/publish/git_publish.py` (task 1.10, §12.1, D6).

Dùng git THẬT (bare repo cục bộ làm "origin" giả — `git clone`/`git worktree`/`git push`
đều là lệnh git thật, không mock) nhưng KHÔNG chạm GitHub/mạng thật — remote là một đường
dẫn filesystem, không phải HTTPS, nên không cần PAT thật (nhánh "local remote không cần
PAT" của `commit_and_push_docs_site()`, xem docstring hàm đó). Đúng nghĩa "unit": không có
`db_connection`/`DATABASE_URL` nào — chỉ subprocess `git` + filesystem tạm (`tmp_path`)."""

from __future__ import annotations

import subprocess
from pathlib import Path

from src.intel_bot.publish.git_publish import commit_and_push_docs_site


def _git_run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    )


def _init_repo_with_gh_pages(tmp_path: Path, initial_html: str) -> tuple[Path, Path]:
    """Tạo bare repo "origin" giả + 1 clone đã có sẵn nhánh gh-pages với nội dung
    `initial_html` — mô phỏng đúng trạng thái "đã bootstrap 1 lần" mà module giả định."""
    origin = tmp_path / "origin.git"
    _git_run(["init", "--bare", "-q", str(origin)], tmp_path)

    repo = tmp_path / "repo"
    _git_run(["clone", "-q", str(origin), str(repo)], tmp_path)
    _git_run(["config", "user.email", "test@example.com"], repo)
    _git_run(["config", "user.name", "Test"], repo)

    (repo / "README.md").write_text("main branch placeholder", encoding="utf-8")
    _git_run(["add", "-A"], repo)
    _git_run(["commit", "-q", "-m", "init main"], repo)
    _git_run(["push", "-q", "origin", "HEAD:main"], repo)

    _git_run(["checkout", "-q", "-b", "gh-pages"], repo)
    (repo / "README.md").unlink()
    (repo / "index.html").write_text(initial_html, encoding="utf-8")
    _git_run(["add", "-A"], repo)
    _git_run(["commit", "-q", "-m", "gh-pages init"], repo)
    _git_run(["push", "-q", "origin", "HEAD:gh-pages"], repo)
    _git_run(["checkout", "-q", "main"], repo)

    return origin, repo


def test_commit_and_push_creates_commit_when_content_changed(tmp_path: Path) -> None:
    origin, repo = _init_repo_with_gh_pages(tmp_path, "<html>old</html>")
    docs_site_dir = tmp_path / "docs-site"
    docs_site_dir.mkdir()
    (docs_site_dir / "index.html").write_text("<html>NEW</html>", encoding="utf-8")

    result = commit_and_push_docs_site(
        repo_root=repo,
        docs_site_dir=docs_site_dir,
        worktree_dir=tmp_path / "gh-pages-worktree",
        branch="gh-pages",
        pat="",
        commit_message="publish: test",
    )

    assert result.pushed is True
    assert result.skipped_no_changes is False
    assert result.error is None
    assert result.commit_sha is not None

    # Xác nhận THẬT trên "remote" (bare repo) — không chỉ tin giá trị trả về.
    show = subprocess.run(
        ["git", "show", "gh-pages:index.html"],
        cwd=origin,
        capture_output=True,
        text=True,
        check=True,
    )
    assert show.stdout == "<html>NEW</html>"


def test_commit_and_push_skips_when_content_unchanged(tmp_path: Path) -> None:
    origin, repo = _init_repo_with_gh_pages(tmp_path, "<html>same</html>")
    docs_site_dir = tmp_path / "docs-site"
    docs_site_dir.mkdir()
    (docs_site_dir / "index.html").write_text("<html>same</html>", encoding="utf-8")

    before = subprocess.run(
        ["git", "rev-parse", "gh-pages"],
        cwd=origin,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    result = commit_and_push_docs_site(
        repo_root=repo,
        docs_site_dir=docs_site_dir,
        worktree_dir=tmp_path / "gh-pages-worktree",
        branch="gh-pages",
        pat="",
        commit_message="publish: test (không nên tạo commit này)",
    )

    assert result.pushed is False
    assert result.skipped_no_changes is True
    assert result.error is None
    assert result.commit_sha is None

    after = subprocess.run(
        ["git", "rev-parse", "gh-pages"],
        cwd=origin,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert before == after  # P1: không tạo commit rỗng khi không có gì đổi


def test_commit_and_push_reflects_deleted_files(tmp_path: Path) -> None:
    """Mô phỏng đúng thứ tự thật (D7 chạy trước D6, xem serve.py): archive/2026-07-01.json
    từng được push lên gh-pages, giờ đã bị prune_archive() xoá khỏi docs_site_dir cục bộ —
    lần push kế tiếp phải phản ánh đúng việc XOÁ đó trên nhánh gh-pages."""
    origin, repo = _init_repo_with_gh_pages(tmp_path, "<html>v1</html>")
    docs_site_dir = tmp_path / "docs-site"
    (docs_site_dir / "archive").mkdir(parents=True)
    (docs_site_dir / "index.html").write_text("<html>v1</html>", encoding="utf-8")
    (docs_site_dir / "archive" / "2026-07-01.json").write_text("{}", encoding="utf-8")

    first = commit_and_push_docs_site(
        repo_root=repo,
        docs_site_dir=docs_site_dir,
        worktree_dir=tmp_path / "gh-pages-worktree",
        branch="gh-pages",
        pat="",
        commit_message="publish: v1 (kèm archive)",
    )
    assert first.pushed is True

    # Xoá file archive cục bộ (mô phỏng prune_archive()) rồi publish lại — index.html KHÔNG
    # đổi, chỉ archive/ mất 1 file, vẫn phải tính là "có thay đổi" và push được.
    (docs_site_dir / "archive" / "2026-07-01.json").unlink()

    second = commit_and_push_docs_site(
        repo_root=repo,
        docs_site_dir=docs_site_dir,
        worktree_dir=tmp_path / "gh-pages-worktree",
        branch="gh-pages",
        pat="",
        commit_message="publish: v1 (archive đã prune)",
    )
    assert second.pushed is True

    show = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", "gh-pages"],
        cwd=origin,
        capture_output=True,
        text=True,
        check=True,
    )
    files = show.stdout.splitlines()
    assert "archive/2026-07-01.json" not in files
    assert "index.html" in files
