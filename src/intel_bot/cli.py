import asyncio
import datetime
import io
import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import sqlalchemy as sa
import typer
from dotenv import load_dotenv

# console Windows mặc định cp1252, không in được tiếng Việt — ép UTF-8 nếu stdout/stderr
# là TextIOWrapper thật (không phải khi bị pytest/CI thay bằng thứ khác không có reconfigure).
if isinstance(sys.stdout, io.TextIOWrapper) and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
if isinstance(sys.stderr, io.TextIOWrapper) and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

from src.intel_bot.config import load_config_dir, settings
from src.intel_bot.db.health import (
    check_connection,
    get_database_url,
    list_tables_by_schema,
)
from src.intel_bot.filter.keyword_filter import FilterRules
from src.intel_bot.filter.loader import run_filter_partition
from src.intel_bot.ingest.loader import normalize_partition
from src.intel_bot.ingest.rss_fetcher import (
    load_source_configs,
    run_rss_ingest,
    run_validate_sources,
)
from src.intel_bot.publish.archive import prune_archive
from src.intel_bot.publish.git_publish import commit_and_push_docs_site
from src.intel_bot.publish.runner import PublishResult, run_publish
from src.intel_bot.score.cost import ModelPricing
from src.intel_bot.score.providers.base import LLMProvider
from src.intel_bot.score.providers.deepseek import DeepSeekProvider
from src.intel_bot.score.providers.deepseek import max_per_run as deepseek_max_per_run
from src.intel_bot.score.providers.mock import ZERO_PRICING, MockProvider
from src.intel_bot.score.runner import (
    RunnerResult,
    run_score_partition,
    run_summarize_top_k_partition,
)

#: Thư mục dbt project — dùng làm cả --project-dir lẫn --profiles-dir (profiles.yml nằm
#: trong chính dbt_project/, xem README mục Dagster / AGENTS.md mục 8).
DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"

#: Gốc repo — dùng để đặt worktree nhánh GitHub Pages (task 1.10, §12.1).
REPO_ROOT = Path(__file__).resolve().parents[2]

load_dotenv()

app = typer.Typer()

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def _load_ingest_settings() -> tuple[str, float, int]:
    """Đọc user_agent, timeout_seconds, max_concurrent_requests từ config/app.yaml."""
    cfg = load_config_dir().get("app", {}).get("ingest", {})
    user_agent = cfg.get("user_agent")
    if not user_agent:
        typer.echo(
            "Thiếu config ingest.user_agent trong config/app.yaml — không tự bịa User-Agent.",
            err=True,
        )
        raise typer.Exit(code=1)
    timeout = float(cfg.get("timeout_seconds", 30))
    max_concurrent = int(cfg.get("max_concurrent_requests", 5))
    return user_agent, timeout, max_concurrent


@app.command()
def ingest(
    date: str | None = typer.Option(
        None,
        "--date",
        help="Ngày ingest (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh",
    ),
) -> None:
    """Fetch RSS từ config/sources.yaml, ghi vào bronze.raw_articles + silver.source_health."""
    if date:
        ingest_date = datetime.date.fromisoformat(date)
    else:
        ingest_date = datetime.datetime.now(tz=VN_TZ).date()

    user_agent, timeout, max_concurrent = _load_ingest_settings()
    sources = load_source_configs(only_enabled=True)
    if not sources:
        typer.echo("Không có nguồn nào enabled trong config/sources.yaml.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Ingest ngày {ingest_date} — {len(sources)} nguồn RSS đang bật")

    database_url = get_database_url()
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            result = asyncio.run(
                run_rss_ingest(
                    connection,
                    sources,
                    user_agent=user_agent,
                    ingest_date=ingest_date,
                    max_concurrent=max_concurrent,
                    timeout=timeout,
                )
            )
    finally:
        engine.dispose()

    typer.echo(
        f"Done: entries_fetched={result.total_entries_fetched} rows_inserted={result.rows_inserted} "
        f"sources_ok={result.sources_ok} sources_failed={len(result.failed_sources)}"
    )
    if result.failed_sources:
        typer.echo("Nguồn lỗi:")
        for source_id in result.failed_sources:
            typer.echo(f"  - {source_id}")


@app.command("validate-sources")
def validate_sources_cmd() -> None:
    """Kiểm tra từng nguồn trong config/sources.yaml: HTTP 200, parse được, ≥1 entry, có ngày.

    Task 1.1 (PRODUCTION_PLAN §8.5): lệnh này chạy trong CI — exit code khác 0 nếu BẤT KỲ
    nguồn nào fail, để CI đỏ thật khi một nguồn ngừng hoạt động, không chỉ in bảng cho có.
    """
    user_agent, timeout, max_concurrent = _load_ingest_settings()
    sources = load_source_configs(only_enabled=False)
    if not sources:
        typer.echo("config/sources.yaml không có nguồn nào.", err=True)
        raise typer.Exit(code=1)

    results = asyncio.run(
        run_validate_sources(
            sources,
            user_agent=user_agent,
            timeout=timeout,
            max_concurrent=max_concurrent,
        )
    )

    headers = (
        "source_id",
        "domain",
        "http",
        "entries",
        "ngày mới nhất",
        "kết quả",
    )
    rows = [
        (
            r.source_id,
            r.domain,
            str(r.http_status) if r.http_status is not None else "-",
            str(r.entry_count),
            r.latest_entry_date or "-",
            "OK" if r.ok else f"FAIL ({r.error})" if r.error else "FAIL",
        )
        for r in results
    ]
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def fmt(cells: tuple[str, ...]) -> str:
        return " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cells))

    typer.echo(fmt(headers))
    typer.echo("-+-".join("-" * w for w in col_widths))
    for row in rows:
        typer.echo(fmt(row))

    ok_count = sum(1 for r in results if r.ok)
    typer.echo(f"\n{ok_count}/{len(results)} nguồn OK")

    failed = [r for r in results if not r.ok]
    if failed:
        typer.echo(
            f"\n{len(failed)} nguồn FAIL: " + ", ".join(r.source_id for r in failed),
            err=True,
        )
        raise typer.Exit(code=1)


@app.command()
def normalize(
    date: str | None = typer.Option(
        None,
        "--date",
        help="Ngày cần chuẩn hoá (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh",
    ),
) -> None:
    """Đọc bronze.raw_articles theo ngày, chuẩn hoá + dedup cấp 1, ghi silver.articles."""
    if date:
        ingest_date = datetime.date.fromisoformat(date)
    else:
        ingest_date = datetime.datetime.now(tz=VN_TZ).date()

    max_article_age_days = int(
        load_config_dir()
        .get("app", {})
        .get("normalize", {})
        .get("max_article_age_days", 7)
    )
    now = datetime.datetime.now(tz=VN_TZ)

    database_url = get_database_url()
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            result = normalize_partition(
                connection,
                ingest_date=ingest_date,
                max_article_age_days=max_article_age_days,
                now=now,
            )
    finally:
        engine.dispose()

    typer.echo(
        f"Normalize ngày {ingest_date}: read={result.read} inserted={result.inserted} "
        f"updated={result.updated}"
    )
    if result.excluded_by_reason:
        typer.echo("Loại theo lý do:")
        for reason, count in sorted(
            result.excluded_by_reason.items(), key=lambda x: -x[1]
        ):
            typer.echo(f"  {reason}: {count}")


@app.command()
def filter(
    date: str | None = typer.Option(
        None,
        "--date",
        help="Ngày cần filter (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh",
    ),
) -> None:
    """Filter tối thiểu (PRODUCTION_PLAN §9.2): đọc silver.articles theo ngày, ghi
    status ('eligible'/'excluded') + filter_score + exclusion_reason. Không gọi LLM,
    không embedding (Phase 2)."""
    if date:
        filter_date = datetime.date.fromisoformat(date)
    else:
        filter_date = datetime.datetime.now(tz=VN_TZ).date()

    cfg = load_config_dir()
    filter_cfg = cfg.get("app", {}).get("filter", {})
    normalize_cfg = cfg.get("app", {}).get("normalize", {})
    blocklist = cfg.get("keywords", {}).get("blocklist", [])
    if not blocklist:
        typer.echo(
            "Thiếu config keywords.yaml: blocklist — không tự bịa danh sách.", err=True
        )
        raise typer.Exit(code=1)

    rules = FilterRules(
        max_article_age_days=int(normalize_cfg.get("max_article_age_days", 7)),
        min_snippet_chars=int(filter_cfg.get("min_snippet_chars", 80)),
        blocklist_keywords=tuple(blocklist),
        max_articles_per_day=int(filter_cfg.get("max_articles_per_day", 200)),
        now=datetime.datetime.now(tz=VN_TZ),
    )

    database_url = get_database_url()
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            result = run_filter_partition(
                connection, filter_date=filter_date, rules=rules
            )
    finally:
        engine.dispose()

    typer.echo(
        f"Filter ngày {filter_date}: read={result.read} eligible={result.eligible} "
        f"excluded={result.excluded}"
    )
    if result.excluded_by_reason:
        typer.echo("Loại theo lý do:")
        for reason, count in sorted(
            result.excluded_by_reason.items(), key=lambda x: -x[1]
        ):
            typer.echo(f"  {reason}: {count}")


def _run_dbt_build(
    select: list[str], run_date: datetime.date, *, error_label: str
) -> None:
    """`dbt build --select <select> --vars '{"run_date": ...}'` — chỉ gọi CLI dbt qua
    subprocess, KHÔNG tính lại business logic trong Python (P5), y hệt cách
    `dagster_project/assets/dbt_assets.py` gọi `dbt.cli(["build", ...])`. Raise `typer.Exit`
    rõ ràng nếu dbt build lỗi — lỗi hạ tầng, không phải lỗi một bản ghi (khác bảng §10.5),
    KHÔNG được nuốt lỗi rồi coi như đã xong.

    Ép UTF-8 tường minh cho subprocess (`PYTHONUTF8`/`PYTHONIOENCODING`) — dbt tự đọc file
    `.sql` có comment tiếng Việt lúc build manifest, vỡ `UnicodeDecodeError` trên Console
    Windows cp1252 nếu KHÔNG ép (AGENTS.md mục 8) và tiến trình cha (vd. CI) lỡ quên set
    biến này trước khi gọi `intel-bot pipeline`/`score` — không dựa vào bên gọi nhớ set."""
    vars_json = json.dumps({"run_date": run_date.isoformat()})
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    try:
        subprocess.run(
            [
                "dbt",
                "build",
                "--select",
                *select,
                "--vars",
                vars_json,
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
            ],
            check=True,
            env=env,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        typer.echo(f"dbt build {error_label} thất bại: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _run_dbt_build_for_fct_article_score(run_date: datetime.date) -> None:
    """`+fct_article_score` (D1, PROGRESS.md mục 9) kéo theo TOÀN BỘ node thượng nguồn thật
    sự cần (staging, snapshot dim_source, intermediate) — không liệt kê tay từng model."""
    _run_dbt_build(["+fct_article_score"], run_date, error_label="fct_article_score")


def _run_dbt_build_for_marts(run_date: datetime.date) -> None:
    """`mart_daily_digest` + `mart_pipeline_health` — bước dbt build TƯỜNG MINH riêng biệt
    với bước ẩn bên trong `score`/`_score_and_summarize` (chỉ build `+fct_article_score`).
    Lệnh `pipeline` cần thêm bước này để `publish` đọc được digest mới nhất (task 1.8/1.9,
    nhiệm vụ 4: "... → dbt build → publish" là 2 mắt xích tách biệt trong chuỗi)."""
    _run_dbt_build(
        ["mart_daily_digest", "mart_pipeline_health"], run_date, error_label="marts"
    )


def _build_provider(
    provider_name: str, *, default_batch_size: int
) -> tuple[LLMProvider, ModelPricing, int]:
    """Chọn provider theo `--provider` (mock|deepseek) — dùng chung cho lệnh `score` và
    `pipeline`, tránh viết lặp logic chọn provider ở 2 nơi."""
    if provider_name == "mock":
        return MockProvider(), ZERO_PRICING, default_batch_size
    if provider_name == "deepseek":
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            typer.echo("Thiếu DEEPSEEK_API_KEY trong .env — không tự bịa.", err=True)
            raise typer.Exit(code=1)
        try:
            deepseek_provider = DeepSeekProvider.from_config(
                api_key=api_key, tier="fast"
            )
        except ValueError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
        return (
            deepseek_provider,
            deepseek_provider.pricing,
            deepseek_max_per_run() or default_batch_size,
        )
    typer.echo(
        f"Provider không hỗ trợ: '{provider_name}' (chỉ 'mock' hoặc 'deepseek').",
        err=True,
    )
    raise typer.Exit(code=1)


def _score_and_summarize(
    connection: sa.Connection,
    *,
    partition_date: datetime.date,
    provider_name: str,
    now: datetime.datetime,
) -> RunnerResult:
    """Chấm điểm + (nếu có bài mới, chưa vượt ngân sách) build `fct_article_score` rồi tóm
    tắt top-K — logic dùng chung cho lệnh `score` và `pipeline` (trước đây chỉ nằm trong
    `score`, tách ra ở task 1.8/1.9 để `pipeline` không phải chép lại)."""
    score_cfg = load_config_dir().get("app", {}).get("score", {})
    daily_budget_usd = Decimal(str(score_cfg.get("daily_budget_usd", "1.00")))
    top_k_summaries = int(score_cfg.get("top_k_summaries", 15))
    default_batch_size = int(score_cfg.get("default_batch_size", 10))

    provider, pricing, batch_size = _build_provider(
        provider_name, default_batch_size=default_batch_size
    )

    result = run_score_partition(
        connection,
        partition_date=partition_date,
        provider=provider,
        pricing=pricing,
        daily_budget_usd=daily_budget_usd,
        batch_size=batch_size,
        now=now,
    )

    if result.scored > 0 and not result.budget_stopped:
        _run_dbt_build_for_fct_article_score(partition_date)
        run_summarize_top_k_partition(
            connection,
            partition_date=partition_date,
            provider=provider,
            pricing=pricing,
            daily_budget_usd=daily_budget_usd,
            top_k_summaries=top_k_summaries,
            now=now,
            result=result,
        )
    return result


@app.command()
def score(
    date: str | None = typer.Option(
        None,
        "--date",
        help="Ngày cần chấm (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh",
    ),
    provider_name: str = typer.Option("mock", "--provider", help="mock hoặc deepseek"),
) -> None:
    """Chấm điểm + tóm tắt top-K cho một partition (PRODUCTION_PLAN §10.1-10.7, §21).

    Không clamp giá trị ngoài miền; lỗi từng bản ghi vào silver.score_quarantine theo
    đúng bảng §10.5. Không raise vì lỗi một bản ghi — chỉ exit khác 0 khi cả provider
    không dùng được giữa chừng.

    **D1:** sau khi chấm điểm xong, lệnh này tự chạy `dbt build fct_article_score` (composite
    score CHÍNH THỨC §5.7) rồi mới tóm tắt top-K — xem `_run_dbt_build_for_fct_article_score`
    và `docs/PROGRESS.md` mục 9 (D1) để biết lý do. Bước dbt CHỈ chạy nếu có bài mới được
    chấm thành công và chưa vượt ngân sách — không tốn thời gian dbt build vô ích khi không
    có gì mới (vd. chạy lại `score` trên partition đã chấm xong).
    """
    if date:
        partition_date = datetime.date.fromisoformat(date)
    else:
        partition_date = datetime.datetime.now(tz=VN_TZ).date()
    now = datetime.datetime.now(tz=VN_TZ)

    database_url = get_database_url()
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            result = _score_and_summarize(
                connection,
                partition_date=partition_date,
                provider_name=provider_name,
                now=now,
            )
    finally:
        engine.dispose()

    typer.echo(
        f"Score ngày {partition_date} (provider={provider_name}): scored={result.scored} "
        f"quarantined={result.quarantined} summarized={result.summarized} "
        f"summary_quarantined={result.summary_quarantined}"
    )
    typer.echo(f"Chi phí: {result.total_cost_usd} USD")
    if result.latencies_ms:
        p50 = result.latency_p50_ms or 0.0
        p95 = result.latency_p95_ms or 0.0
        typer.echo(f"Latency p50={p50:.0f}ms p95={p95:.0f}ms")
    if result.quarantine_by_reason:
        typer.echo("Quarantine theo lý do:")
        for reason, count in sorted(
            result.quarantine_by_reason.items(), key=lambda x: -x[1]
        ):
            typer.echo(f"  {reason}: {count}")
    if result.budget_stopped:
        typer.echo("Đã DỪNG do vượt ngân sách ngày — các bài còn lại vẫn ở 'eligible'.")
    if result.provider_unavailable:
        typer.echo(
            "Provider không dùng được giữa chừng — các bài chưa xử lý vẫn giữ nguyên status.",
            err=True,
        )
        raise typer.Exit(code=1)


def _publish_and_sync_docs_site(
    connection: sa.Connection,
    *,
    generated_for_date: datetime.date,
    now: datetime.datetime,
) -> tuple[PublishResult, list[Path], str | None]:
    """`run_publish()` + prune archive cũ (D7, §12.2) + commit/push docs-site/ lên nhánh
    GitHub Pages (D6, §12.1) — logic dùng chung cho lệnh `publish` và `pipeline`, cùng một
    hành vi với asset `published_site` (`dagster_project/assets/serve.py`) để đường CLI dự
    phòng (task 1.8/1.9) cũng cập nhật được trang công khai, không chỉ ghi file cục bộ.

    Trả về `(PublishResult, danh_sách_file_archive_đã_xoá, git_push_error_hoặc_None)` — lỗi
    push KHÔNG raise (rào chắn task 1.10 mục 1), bên gọi tự quyết định báo lỗi thế nào.
    """
    publish_cfg = load_config_dir().get("app", {}).get("publish", {})
    repo_url = publish_cfg.get("repo_url")
    if not repo_url:
        typer.echo(
            "Thiếu config app.yaml: publish.repo_url — không tự bịa link repo.",
            err=True,
        )
        raise typer.Exit(code=1)
    docs_site_dir = Path(publish_cfg.get("docs_site_dir", "docs-site"))
    templates_dir = Path(publish_cfg.get("templates_dir", "templates"))
    archive_days = int(publish_cfg.get("archive_days", 7))
    gh_pages_branch = publish_cfg.get("gh_pages_branch", "gh-pages")
    worktree_dir = REPO_ROOT / publish_cfg.get(
        "gh_pages_worktree_dir", ".gh-pages-worktree"
    )

    result = run_publish(
        connection,
        generated_for_date=generated_for_date,
        docs_site_dir=docs_site_dir,
        templates_dir=templates_dir,
        repo_url=repo_url,
        now=now,
    )

    removed_archives = prune_archive(
        docs_site_dir, archive_days=archive_days, today=generated_for_date
    )

    git_push_error: str | None = None
    if not settings.GIT_PUBLISH_TOKEN:
        git_push_error = (
            "Thiếu biến môi trường GIT_PUBLISH_TOKEN — không commit/push docs-site/."
        )
    else:
        # commit_and_push_docs_site() có thể raise (vd. nhánh gh-pages chưa bootstrap) —
        # bắt ở đây giống hệt lý do ở dagster_project/assets/serve.py: rào chắn task 1.10
        # mục 1 không phân biệt "loại" lỗi git-publish, lệnh vẫn phải thành công vì file cục
        # bộ đã ghi xong.
        try:
            git_result = commit_and_push_docs_site(
                repo_root=REPO_ROOT,
                docs_site_dir=docs_site_dir,
                worktree_dir=worktree_dir,
                branch=gh_pages_branch,
                pat=settings.GIT_PUBLISH_TOKEN,
                commit_message=f"publish: digest {generated_for_date.isoformat()}",
            )
        except Exception as exc:  # noqa: BLE001 — xem giải thích ở trên
            git_push_error = str(exc)
        else:
            if git_result.error:
                git_push_error = git_result.error

    return result, removed_archives, git_push_error


@app.command()
def publish(
    date: str | None = typer.Option(
        None,
        "--date",
        help=(
            "Ngày publish (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh — chỉ "
            "dùng để đặt tên file archive và hiển thị header, KHÔNG lọc lại "
            "gold.mart_daily_digest (mart tự quyết cửa sổ 48h khi build, §12.1)."
        ),
    ),
) -> None:
    """Xuất `gold.mart_daily_digest` ra JSON + HTML tĩnh (PRODUCTION_PLAN §12.1-12.4).

    Truy vấn DUY NHẤT: SELECT * FROM gold.mart_daily_digest — không sắp xếp/lọc/dedup gì
    thêm ở đây, mart đã làm hết (§12.1). Sau khi ghi file, cập nhật
    silver.articles.last_published_at (ngoại lệ DUY NHẤT được chạm bảng khác), dọn archive
    cũ hơn `archive_days` (D7, §12.2), rồi commit + push `docs-site/` lên nhánh GitHub Pages
    (D6, §12.1) — lỗi push chỉ cảnh báo, KHÔNG làm lệnh thất bại (file cục bộ đã ghi xong).
    """
    if date:
        generated_for_date = datetime.date.fromisoformat(date)
    else:
        generated_for_date = datetime.datetime.now(tz=VN_TZ).date()
    now = datetime.datetime.now(tz=VN_TZ)

    database_url = get_database_url()
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            result, removed_archives, git_push_error = _publish_and_sync_docs_site(
                connection, generated_for_date=generated_for_date, now=now
            )
    finally:
        engine.dispose()

    typer.echo(
        f"Publish ngày {generated_for_date}: {result.article_count} bài -> "
        f"{result.index_html_path}"
    )
    typer.echo(f"  JSON: {result.articles_json_path}")
    typer.echo(f"  Archive: {result.archive_json_path}")
    typer.echo(
        f"  Đã cập nhật last_published_at cho {result.articles_marked_published} bài"
    )
    if removed_archives:
        typer.echo(f"  Đã xoá {len(removed_archives)} file archive cũ")
    if git_push_error:
        typer.echo(f"  Cảnh báo: push docs-site/ thất bại: {git_push_error}", err=True)
    if result.article_count == 0:
        typer.echo(
            "Cảnh báo: gold.mart_daily_digest rỗng — kiểm tra dbt run gần nhất.",
            err=True,
        )


@app.command()
def pipeline(
    date: str | None = typer.Option(
        None,
        "--date",
        help="Ngày chạy (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh",
    ),
    provider_name: str = typer.Option("mock", "--provider", help="mock hoặc deepseek"),
) -> None:
    """Chạy TUẦN TỰ ingest → normalize → filter → score → dbt build → publish cho một
    `--date` (task 1.8/1.9). **Đường chạy DỰ PHÒNG khi Dagster không dùng được — KHÔNG thay
    thế Dagster** (lịch chính vẫn là Dagster schedule + 4 sensor, task 0.12/1.6): lệnh này
    gọi lại ĐÚNG các hàm mà `ingest`/`normalize`/`filter`/`score`/`publish` đã dùng, không
    viết lại logic nghiệp vụ nào ở đây (P5) — chỉ khác chỗ chạy hết một lượt, một tiến trình,
    một `--date` duy nhất thay vì gõ 5 lệnh tay.

    Dừng NGAY ở bước đầu tiên lỗi (không nuốt lỗi rồi chạy tiếp bước sau trên dữ liệu chưa
    sẵn sàng) — mỗi bước in rõ kết quả trước khi sang bước kế, giống hệt log của từng lệnh
    CLI gốc.
    """
    if date:
        run_date = datetime.date.fromisoformat(date)
    else:
        run_date = datetime.datetime.now(tz=VN_TZ).date()
    now = datetime.datetime.now(tz=VN_TZ)

    typer.echo(f"=== pipeline ngày {run_date} (provider={provider_name}) ===")

    # 1. ingest
    user_agent, timeout, max_concurrent = _load_ingest_settings()
    sources = load_source_configs(only_enabled=True)
    if not sources:
        typer.echo("Không có nguồn nào enabled trong config/sources.yaml.", err=True)
        raise typer.Exit(code=1)

    database_url = get_database_url()
    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            typer.echo("[1/6] ingest...")
            ingest_result = asyncio.run(
                run_rss_ingest(
                    connection,
                    sources,
                    user_agent=user_agent,
                    ingest_date=run_date,
                    max_concurrent=max_concurrent,
                    timeout=timeout,
                )
            )
            typer.echo(
                f"      entries_fetched={ingest_result.total_entries_fetched} "
                f"rows_inserted={ingest_result.rows_inserted} "
                f"sources_ok={ingest_result.sources_ok} "
                f"sources_failed={len(ingest_result.failed_sources)}"
            )

            # 2. normalize
            max_article_age_days = int(
                load_config_dir()
                .get("app", {})
                .get("normalize", {})
                .get("max_article_age_days", 7)
            )
            typer.echo("[2/6] normalize...")
            normalize_result = normalize_partition(
                connection,
                ingest_date=run_date,
                max_article_age_days=max_article_age_days,
                now=now,
            )
            typer.echo(
                f"      read={normalize_result.read} inserted={normalize_result.inserted} "
                f"updated={normalize_result.updated}"
            )

            # 3. filter
            cfg = load_config_dir()
            filter_cfg = cfg.get("app", {}).get("filter", {})
            blocklist = cfg.get("keywords", {}).get("blocklist", [])
            if not blocklist:
                typer.echo(
                    "Thiếu config keywords.yaml: blocklist — không tự bịa danh sách.",
                    err=True,
                )
                raise typer.Exit(code=1)
            rules = FilterRules(
                max_article_age_days=max_article_age_days,
                min_snippet_chars=int(filter_cfg.get("min_snippet_chars", 80)),
                blocklist_keywords=tuple(blocklist),
                max_articles_per_day=int(filter_cfg.get("max_articles_per_day", 200)),
                now=now,
            )
            typer.echo("[3/6] filter...")
            filter_result = run_filter_partition(
                connection, filter_date=run_date, rules=rules
            )
            typer.echo(
                f"      read={filter_result.read} eligible={filter_result.eligible} "
                f"excluded={filter_result.excluded}"
            )

            # 4. score (+ dbt build fct_article_score + summarize top-K bên trong)
            typer.echo("[4/6] score...")
            score_result = _score_and_summarize(
                connection,
                partition_date=run_date,
                provider_name=provider_name,
                now=now,
            )
            typer.echo(
                f"      scored={score_result.scored} quarantined={score_result.quarantined} "
                f"summarized={score_result.summarized} chi_phí={score_result.total_cost_usd} USD"
            )
            if score_result.provider_unavailable:
                typer.echo(
                    "Provider không dùng được giữa chừng — dừng pipeline.", err=True
                )
                raise typer.Exit(code=1)

            # 5. dbt build (marts còn lại publish cần — fct_article_score đã build ở bước 4)
            typer.echo("[5/6] dbt build (marts)...")
            _run_dbt_build_for_marts(run_date)

            # 6. publish (+ prune archive D7 + commit/push docs-site/ D6, §12.1/§12.2)
            typer.echo("[6/6] publish...")
            publish_result, removed_archives, git_push_error = (
                _publish_and_sync_docs_site(
                    connection, generated_for_date=run_date, now=now
                )
            )
            typer.echo(
                f"      {publish_result.article_count} bài -> {publish_result.index_html_path}"
            )
            if removed_archives:
                typer.echo(f"      Đã xoá {len(removed_archives)} file archive cũ")
            if git_push_error:
                typer.echo(
                    f"      Cảnh báo: push docs-site/ thất bại: {git_push_error}",
                    err=True,
                )
    finally:
        engine.dispose()

    typer.echo(f"=== pipeline ngày {run_date} XONG ===")


@app.command()
def doctor() -> None:
    """Kiểm tra thật kết nối Postgres và liệt kê các bảng đã tồn tại theo từng schema."""
    try:
        database_url = get_database_url()
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)

    engine = sa.create_engine(database_url, future=True)
    try:
        with engine.connect() as connection:
            if not check_connection(connection):
                typer.echo("DB: SELECT 1 không trả về giá trị mong đợi.", err=True)
                raise typer.Exit(code=1)
            typer.echo(
                f"DB: kết nối OK ({engine.url.host}:{engine.url.port}/{engine.url.database})"
            )

            tables_by_schema = list_tables_by_schema(connection)
            for schema_name, table_names in tables_by_schema.items():
                if table_names:
                    typer.echo(f"  {schema_name}: {len(table_names)} bảng")
                    for table_name in table_names:
                        typer.echo(f"    - {table_name}")
                else:
                    typer.echo(f"  {schema_name}: (chưa có bảng)")
    except sa.exc.SQLAlchemyError as exc:
        typer.echo(f"DB: kết nối thất bại — {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        engine.dispose()


@app.command()
def eval() -> None:
    """Run evaluation on labeled dataset."""
    typer.echo("Eval job (placeholder)")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
