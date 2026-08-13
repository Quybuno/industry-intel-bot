"""Test CLI `intel-bot validate-sources` (task 1.1, PRODUCTION_PLAN §8.5).

Đây là lệnh chạy trong CI — DONE WHEN chính là exit code khác 0 khi có nguồn fail. Dùng
CliRunner của typer, không gọi mạng thật (patch `run_validate_sources`), giống mẫu
`tests/test_cli_score.py`.
"""

from __future__ import annotations

from unittest.mock import patch

from typer.testing import CliRunner

from src.intel_bot.cli import app
from src.intel_bot.ingest.rss_fetcher import SourceValidation, latest_entry_date

runner = CliRunner()


def _validation(
    source_id: str, *, ok: bool, error: str | None = None
) -> SourceValidation:
    return SourceValidation(
        source_id=source_id,
        domain=f"{source_id}.test",
        http_status=200 if ok else 404,
        entry_count=10 if ok else 0,
        has_date_field=ok,
        latest_entry_date="2026-08-12T00:00:00+00:00" if ok else None,
        ok=ok,
        error=error,
    )


def test_validate_sources_all_ok_exits_zero() -> None:
    fake_results = [_validation("a", ok=True), _validation("b", ok=True)]
    with patch(
        "src.intel_bot.cli.run_validate_sources",
        return_value=fake_results,
    ):
        result = runner.invoke(app, ["validate-sources"])

    assert result.exit_code == 0
    assert "2/2 nguồn OK" in result.stdout


def test_validate_sources_one_fail_exits_nonzero() -> None:
    """DONE WHEN: exit code khác 0 khi có nguồn fail — đây là lệnh chạy trong CI."""
    fake_results = [
        _validation("a", ok=True),
        _validation("b", ok=False, error="HTTP 404"),
    ]
    with patch(
        "src.intel_bot.cli.run_validate_sources",
        return_value=fake_results,
    ):
        result = runner.invoke(app, ["validate-sources"])

    assert result.exit_code != 0
    assert "1 nguồn FAIL" in result.output
    assert "b" in result.output


def test_latest_entry_date_picks_the_most_recent() -> None:
    entries = [
        {"published_parsed": "2026-08-01T00:00:00+00:00"},
        {"published_parsed": "2026-08-11T00:00:00+00:00"},
        {"published_parsed": "2026-08-05T00:00:00+00:00"},
    ]
    assert latest_entry_date(entries) == "2026-08-11T00:00:00+00:00"


def test_latest_entry_date_none_when_no_entries_have_date() -> None:
    assert latest_entry_date([{"title": "no date here"}]) is None


def test_latest_entry_date_empty_list() -> None:
    assert latest_entry_date([]) is None
