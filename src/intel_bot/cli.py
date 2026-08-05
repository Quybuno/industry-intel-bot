import typer
from typing import Optional

from src.intel_bot.config import load_config_dir
from src.intel_bot.jobs.ingest_job import run_ingest_job
from src.intel_bot.jobs.filter_job import run_filter_job

app = typer.Typer()


@app.command()
def ingest(limit: Optional[int] = typer.Option(None, help='Limit per source for testing')):
    """Run the ingest job (fetch RSS + GitHub)."""
    typer.echo('Running ingest (RSS + GitHub)')
    cfg = load_config_dir()
    typer.echo(f'Loaded config sections: {list(cfg.keys())}')
    result = run_ingest_job(limit=limit)
    typer.echo(
        f'Done: inserted={result.inserted} skipped={result.skipped} '
        f'sources_ok={result.sources_ok} sources_failed={result.sources_failed}'
    )
    if result.errors:
        typer.echo('Errors:')
        for err in result.errors:
            typer.echo(f'  - {err}')


@app.command()
def filter(limit: Optional[int] = typer.Option(None, help='Max raw articles to process')):
    """Run the filter job (keyword + embedding)."""
    typer.echo('Running filter (keyword + embedding)')
    result = run_filter_job(limit=limit)
    typer.echo(
        f'Done: processed={result.processed} filtered={result.filtered} '
        f'rejected={result.rejected} errors={result.errors}'
    )
    if result.rejection_reasons:
        typer.echo('Rejection breakdown:')
        for reason, count in sorted(result.rejection_reasons.items(), key=lambda x: -x[1]):
            typer.echo(f'  {reason}: {count}')


@app.command()
def score():
    """Run the score job (LLM cascade)."""
    typer.echo('Running score (placeholder)')


@app.command()
def publish():
    """Run the publish job (export JSON + HTML + git push)."""
    typer.echo('Running publish (placeholder)')


@app.command()
def pipeline():
    """Run full pipeline: ingest -> filter -> score -> publish."""
    typer.echo('Running full pipeline (placeholder)')


@app.command()
def doctor():
    """Run health checks (DB, Ollama)."""
    typer.echo('Doctor: DB and Ollama health checks (placeholder)')


@app.command()
def eval():
    """Run evaluation on labeled dataset."""
    typer.echo('Eval job (placeholder)')


def main():
    app()


if __name__ == '__main__':
    main()
