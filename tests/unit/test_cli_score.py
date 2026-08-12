"""Test CLI `intel-bot score` — xác nhận exit code đúng như DONE WHEN task 0.8/0.9.

Lỗi từng bản ghi (quarantine) → exit code 0. Provider không dùng được → exit code khác 0.
Dùng CliRunner của typer, không gọi mạng thật (patch run_score_partition) — D1: từ khi
`score` tự chạy `dbt build` + `run_summarize_top_k_partition()` khi có bài mới được chấm,
test nào có `scored > 0` phải patch thêm hai bước đó để không thực sự shell ra dbt/đọc
gold.fct_article_score.
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
    with (
        patch("src.intel_bot.cli.run_score_partition", return_value=fake_result),
        patch("src.intel_bot.cli._run_dbt_build_for_fct_article_score"),
        patch("src.intel_bot.cli.run_summarize_top_k_partition"),
    ):
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


def test_score_new_articles_triggers_dbt_build_then_summarize() -> None:
    """D1: `scored > 0` và không vượt ngân sách → phải chạy dbt build RỒI MỚI tóm tắt top-K
    (composite score chính thức §5.7 cần dbt build fct_article_score trước)."""
    fake_result = RunnerResult(scored=3, total_cost_usd=Decimal(0))
    with (
        patch(
            "src.intel_bot.cli.run_score_partition", return_value=fake_result
        ) as mock_score,
        patch("src.intel_bot.cli._run_dbt_build_for_fct_article_score") as mock_dbt,
        patch("src.intel_bot.cli.run_summarize_top_k_partition") as mock_summarize,
    ):
        result = runner.invoke(
            app, ["score", "--date", "2000-01-01", "--provider", "mock"]
        )

    assert result.exit_code == 0
    mock_score.assert_called_once()
    mock_dbt.assert_called_once()
    mock_summarize.assert_called_once()


def test_score_nothing_new_skips_dbt_build_and_summarize() -> None:
    """D1: `scored == 0` (vd. chạy lại partition đã chấm xong) → KHÔNG tốn thời gian
    `dbt build` hay gọi provider tóm tắt lần nữa."""
    fake_result = RunnerResult(scored=0, total_cost_usd=Decimal(0))
    with (
        patch("src.intel_bot.cli.run_score_partition", return_value=fake_result),
        patch("src.intel_bot.cli._run_dbt_build_for_fct_article_score") as mock_dbt,
        patch("src.intel_bot.cli.run_summarize_top_k_partition") as mock_summarize,
    ):
        result = runner.invoke(
            app, ["score", "--date", "2000-01-01", "--provider", "mock"]
        )

    assert result.exit_code == 0
    mock_dbt.assert_not_called()
    mock_summarize.assert_not_called()


def test_score_budget_stopped_skips_dbt_build_and_summarize() -> None:
    """D1: dù có bài đã chấm trước khi hết ngân sách, `budget_stopped=True` vẫn không nên
    kéo dbt build/tóm tắt chạy thêm ở lần này (đợi ngân sách reset)."""
    fake_result = RunnerResult(scored=2, budget_stopped=True, total_cost_usd=Decimal(0))
    with (
        patch("src.intel_bot.cli.run_score_partition", return_value=fake_result),
        patch("src.intel_bot.cli._run_dbt_build_for_fct_article_score") as mock_dbt,
        patch("src.intel_bot.cli.run_summarize_top_k_partition") as mock_summarize,
    ):
        result = runner.invoke(
            app, ["score", "--date", "2000-01-01", "--provider", "mock"]
        )

    assert result.exit_code == 0
    mock_dbt.assert_not_called()
    mock_summarize.assert_not_called()
