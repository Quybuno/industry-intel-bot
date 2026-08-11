"""Test CLI `intel-bot score` — xác nhận exit code đúng như DONE WHEN task 0.8/0.9.

Lỗi từng bản ghi (quarantine) → exit code 0. Provider không dùng được → exit code khác 0.
Dùng CliRunner của typer, không gọi mạng thật (patch run_score_partition).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from typer.testing import CliRunner

from src.intel_bot.cli import app
from src.intel_bot.score.runner import RunnerResult

runner = CliRunner()


def test_score_json_hong_injected_quarantine_but_exit_code_zero() -> None:
    """DONE WHEN: inject JSON hỏng → vào quarantine, exit code vẫn là 0."""
    fake_result = RunnerResult(
        scored=9,
        quarantined=1,
        quarantine_by_reason={"json_parse_error": 1},
        total_cost_usd=Decimal(0),
        latencies_ms=[50] * 9,
    )
    with patch("src.intel_bot.cli.run_score_partition", return_value=fake_result):
        result = runner.invoke(
            app, ["score", "--date", "2000-01-01", "--provider", "mock"]
        )

    assert result.exit_code == 0
    assert "quarantined=1" in result.stdout


def test_score_provider_unavailable_gives_nonzero_exit_code() -> None:
    """DONE WHEN implicit: provider không dùng được (§10.5) không được xem như thành công."""
    fake_result = RunnerResult(provider_unavailable=True)
    with patch("src.intel_bot.cli.run_score_partition", return_value=fake_result):
        result = runner.invoke(
            app, ["score", "--date", "2000-01-01", "--provider", "mock"]
        )

    assert result.exit_code != 0


def test_score_unsupported_provider_name_gives_nonzero_exit_code() -> None:
    result = runner.invoke(
        app, ["score", "--date", "2000-01-01", "--provider", "nonexistent"]
    )
    assert result.exit_code != 0


def test_score_deepseek_without_api_key_gives_clear_error_not_a_guess(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """Không tự bịa API key — thiếu key phải dừng rõ ràng, không đoán."""
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    result = runner.invoke(
        app, ["score", "--date", "2000-01-01", "--provider", "deepseek"]
    )
    assert result.exit_code != 0
    assert "DEEPSEEK_API_KEY" in result.output
