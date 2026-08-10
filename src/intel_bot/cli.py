import asyncio
import datetime
import sys
import typer
from typing import Optional
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from dotenv import load_dotenv

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(
        encoding="utf-8"
    )  # console Windows mặc định cp1252, không in được tiếng Việt
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

from src.intel_bot.config import load_config_dir
from src.intel_bot.db.health import (
    check_connection,
    get_database_url,
    list_tables_by_schema,
)
from src.intel_bot.ingest.rss_fetcher import (
    load_source_configs,
    run_rss_ingest,
    run_validate_sources,
)
from src.intel_bot.jobs.filter_job import run_filter_job

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
    date: Optional[str] = typer.Option(
        None,
        "--date",
        help="Ngày ingest (YYYY-MM-DD), mặc định hôm nay theo giờ Asia/Ho_Chi_Minh",
    ),
):
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
def validate_sources_cmd():
    """Kiểm tra từng nguồn trong config/sources.yaml: HTTP 200, parse được, ≥1 entry, có ngày."""
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

    headers = ("source_id", "domain", "http", "entries", "có ngày", "kết quả")
    rows = [
        (
            r.source_id,
            r.domain,
            str(r.http_status) if r.http_status is not None else "-",
            str(r.entry_count),
            "yes" if r.has_date_field else "no",
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


@app.command()
def filter(
    limit: Optional[int] = typer.Option(None, help="Max raw articles to process"),
):
    """Run the filter job (keyword + embedding)."""
    typer.echo("Running filter (keyword + embedding)")
    result = run_filter_job(limit=limit)
    typer.echo(
        f"Done: processed={result.processed} filtered={result.filtered} "
        f"rejected={result.rejected} errors={result.errors}"
    )
    if result.rejection_reasons:
        typer.echo("Rejection breakdown:")
        for reason, count in sorted(
            result.rejection_reasons.items(), key=lambda x: -x[1]
        ):
            typer.echo(f"  {reason}: {count}")


@app.command()
def score():
    """Run the score job (LLM cascade)."""
    typer.echo("Running score (placeholder)")


@app.command()
def publish():
    """Run the publish job (export JSON + HTML + git push)."""
    typer.echo("Running publish (placeholder)")


@app.command()
def pipeline():
    """Run full pipeline: ingest -> filter -> score -> publish."""
    typer.echo("Running full pipeline (placeholder)")


@app.command()
def doctor():
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
def eval():
    """Run evaluation on labeled dataset."""
    typer.echo("Eval job (placeholder)")


def main():
    app()


if __name__ == "__main__":
    main()
