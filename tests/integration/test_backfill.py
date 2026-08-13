"""Test backfill tự động (task 1.8/1.9, PRODUCTION_PLAN §1.8, §23.3) — CHƯA từng tồn tại
trước task này (đề bài nói rõ: "chỉ được verify BẰNG TAY ở task 0.12"). Postgres THẬT
(§20.2), provider MOCK, mạng RSS giả bằng `httpx.MockTransport` — KHÔNG gọi LLM/mạng thật.

Chạy TOÀN BỘ đường ống thật (ingest → normalize → filter → score → dbt build) cho 2
partition tách biệt hoàn toàn dữ liệu thật (2019-06-14/15), đếm số dòng THẬT ở mọi bảng
bronze/silver/gold liên quan tới từng partition — không đếm suông rồi tin, đếm lại bằng SQL
sau mỗi bước.

**`now` CỐ ĐỊNH xuyên suốt (không phải `datetime.now()` thật), dùng LẠI Y HỆT giữa lần chạy
1 và lần chạy 2 của CÙNG một partition.** Đây là điểm mấu chốt để tách bạch đúng "đổi vì mất
tính lũy đẳng" khỏi "đổi vì thiết kế" (nhiệm vụ 3 yêu cầu): §8.2 (cold-start) so tuổi bài với
`now` THẬT tại thời điểm chạy — nếu dùng `datetime.now()` thật cho cả 2 lần chạy cách nhau
vài giây, on paper vẫn có thể lệch (dù cực hiếm) nếu published_at của bài nằm sát biên
`max_article_age_days`. Cố định `now` loại bỏ hoàn toàn biến số thời gian khỏi test này —
test chỉ còn đo đúng MỘT thứ: logic ghi (`ON CONFLICT`/`MERGE`) có lũy đẳng hay không, đúng
tinh thần P1. Hành vi "mart_pipeline_health của một NGÀY CŨ đổi nhẹ khi ai đó chạy lại
THẬT SỰ cách nhau nhiều ngày" (cạm bẫy đã gặp thật ở task 0.12, §8.2) là đúng thiết kế và
KHÔNG bị test này chạm tới — nó chỉ xảy ra khi `now` thay đổi giữa 2 lần chạy, mà test này
cố tình không cho `now` thay đổi.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import subprocess
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa

from src.intel_bot.filter.keyword_filter import FilterRules
from src.intel_bot.filter.loader import run_filter_partition
from src.intel_bot.ingest.loader import normalize_partition
from src.intel_bot.ingest.rss_fetcher import SourceConfig, fetch_all_sources
from src.intel_bot.score.providers.mock import ZERO_PRICING, MockProvider
from src.intel_bot.score.runner import (
    RunnerResult,
    run_score_partition,
    run_summarize_top_k_partition,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "rss"
DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_project"

#: Dải ngày RIÊNG cho test backfill — tách biệt hoàn toàn dữ liệu thật (2026-08-xx) và các
#: dải ngày test khác trong repo (2000-01-xx: rss/github ingest; 2025-01-xx: alerting).
DAY_D = dt.date(2019, 6, 15)
DAY_D_MINUS_1 = dt.date(2019, 6, 14)

#: 2 fixture KHÁC NHAU (URL/canonical_url không trùng) cho D và D-1 — dùng CHUNG một
#: fixture sẽ khiến D-1 (sớm hơn) "thắng" `LEAST(first_seen_date)` trên chính bài của D
#: (đúng hành vi dedup cấp 1 cho MỘT bài xuất hiện ở 2 ngày, KHÔNG phải kịch bản backfill
#: "2 ngày có 2 bài khác nhau" mà DONE WHEN #3 cần verify) — đã tự phát hiện khi viết test
#: này (lần chạy đầu dùng chung 1 fixture, assertion #3 báo đỏ đúng vì lý do này, không phải
#: bug).
FIXTURE_DAY_D = FIXTURES_DIR / "sample_valid.xml"
FIXTURE_DAY_D_MINUS_1 = FIXTURES_DIR / "sample_valid_2.xml"

#: `now` CỐ ĐỊNH — xem docstring module. Sau pubDate của CẢ HAI fixture (09/10 Aug 2026) và
#: trong vòng `max_article_age_days` của chúng, để không phụ thuộc vào việc so sánh cold-start
#: có rơi vào biên hay không — test không đo hành vi cold-start, chỉ cần published_at không
#: bị loại "too_old" một cách tình cờ.
FIXED_NOW = dt.datetime(2026, 8, 15, 9, 0, 0, tzinfo=dt.UTC)

MAX_ARTICLE_AGE_DAYS = 7
MIN_SNIPPET_CHARS = (
    0  # fixture RSS có snippet ngắn — tắt ngưỡng này để không bị filter loại
)
BLOCKLIST: tuple[str, ...] = ("webinar", "sponsored")
MAX_ARTICLES_PER_DAY = 200
DAILY_BUDGET_USD = Decimal("100.00")
TOP_K_SUMMARIES = 15


def _mock_transport(fixture_path: Path) -> httpx.MockTransport:
    fixture_bytes = fixture_path.read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=fixture_bytes)

    return httpx.MockTransport(handler)


def _fixture_path_for_day(day: dt.date) -> Path:
    """Chọn fixture RIÊNG theo partition — xem chú thích ở FIXTURE_DAY_D/_MINUS_1."""
    if day == DAY_D:
        return FIXTURE_DAY_D
    if day == DAY_D_MINUS_1:
        return FIXTURE_DAY_D_MINUS_1
    raise ValueError(
        f"Không có fixture cho ngày {day} (chỉ hỗ trợ DAY_D/DAY_D_MINUS_1)"
    )


def _fixture_source(day: dt.date) -> list[SourceConfig]:
    # source_id RIÊNG theo ngày để 2 partition không đụng payload_hash lẫn nhau qua
    # silver.source_health (khoá unique (source_id, fetch_date), không phải vấn đề ở đây
    # nhưng giữ tách bạch cho rõ ràng khi debug).
    return [
        SourceConfig(
            source_id=f"test_backfill_source_{day.isoformat()}",
            url="https://fixture.test/backfill.xml",
            domain="fixture.test",
            tier=1,
            industries=["ai"],
            is_enabled=True,
        )
    ]


def _dbt_build(select: list[str], run_date: dt.date) -> None:
    """`Console Windows mặc định cp1252` (AGENTS.md mục 8) — dbt tự đọc file `.sql` có
    comment tiếng Việt lúc build manifest, vỡ với `UnicodeDecodeError` nếu subprocess không
    được ép UTF-8 tường minh (gặp thật khi viết test này, không phải suy đoán)."""
    vars_json = json.dumps({"run_date": run_date.isoformat()})
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(
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
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt build {select} thất bại (exit {result.returncode}):\n"
            f"{result.stdout[-4000:]}\n{result.stderr[-2000:]}"
        )


def _run_full_pipeline_for_partition(connection: sa.Connection, day: dt.date) -> None:
    """ingest (mock) -> normalize -> filter -> score (mock) -> dbt build -> summarize (mock)
    -> dbt build marts, cho đúng một partition, dùng `FIXED_NOW` xuyên suốt."""
    sources = _fixture_source(day)

    async def _ingest() -> None:
        transport = _mock_transport(_fixture_path_for_day(day))
        async with httpx.AsyncClient(transport=transport) as client:
            await fetch_all_sources(connection, client, sources, ingest_date=day)

    asyncio.run(_ingest())

    normalize_partition(
        connection,
        ingest_date=day,
        max_article_age_days=MAX_ARTICLE_AGE_DAYS,
        now=FIXED_NOW,
    )

    rules = FilterRules(
        max_article_age_days=MAX_ARTICLE_AGE_DAYS,
        min_snippet_chars=MIN_SNIPPET_CHARS,
        blocklist_keywords=BLOCKLIST,
        max_articles_per_day=MAX_ARTICLES_PER_DAY,
        now=FIXED_NOW,
    )
    run_filter_partition(connection, filter_date=day, rules=rules)

    provider = MockProvider()
    score_result = run_score_partition(
        connection,
        partition_date=day,
        provider=provider,
        pricing=ZERO_PRICING,
        daily_budget_usd=DAILY_BUDGET_USD,
        batch_size=50,
        now=FIXED_NOW,
    )

    if score_result.scored > 0 and not score_result.budget_stopped:
        _dbt_build(["+fct_article_score"], day)
        result = RunnerResult()
        run_summarize_top_k_partition(
            connection,
            partition_date=day,
            provider=provider,
            pricing=ZERO_PRICING,
            daily_budget_usd=DAILY_BUDGET_USD,
            top_k_summaries=TOP_K_SUMMARIES,
            now=FIXED_NOW,
            result=result,
        )

    _dbt_build(["mart_daily_digest", "mart_pipeline_health"], day)


def _snapshot_counts(connection: sa.Connection, day: dt.date) -> dict[str, int]:
    """Đếm THẬT bằng SQL — mọi bảng bronze/silver/gold liên quan tới đúng partition `day`."""
    queries: dict[str, sa.TextClause] = {
        "bronze.raw_articles": sa.text(
            "SELECT COUNT(*) FROM bronze.raw_articles WHERE ingest_date = :d"
        ),
        "silver.articles": sa.text(
            "SELECT COUNT(*) FROM silver.articles WHERE first_seen_date = :d"
        ),
        "silver.article_scores": sa.text(
            "SELECT COUNT(*) FROM silver.article_scores sc "
            "JOIN silver.articles a ON a.article_id = sc.article_id "
            "WHERE a.first_seen_date = :d"
        ),
        "silver.article_summaries": sa.text(
            "SELECT COUNT(*) FROM silver.article_summaries su "
            "JOIN silver.articles a ON a.article_id = su.article_id "
            "WHERE a.first_seen_date = :d"
        ),
        "silver.score_quarantine": sa.text(
            "SELECT COUNT(*) FROM silver.score_quarantine q "
            "JOIN silver.articles a ON a.article_id = q.article_id "
            "WHERE a.first_seen_date = :d"
        ),
        "gold.fct_article_score": sa.text(
            "SELECT COUNT(*) FROM gold.fct_article_score WHERE first_seen_date = :d"
        ),
        "gold.mart_pipeline_health": sa.text(
            "SELECT COUNT(*) FROM gold.mart_pipeline_health WHERE pipeline_date = :d"
        ),
    }
    return {
        name: connection.execute(query, {"d": day}).scalar_one()
        for name, query in queries.items()
    }


def _cleanup_partition(connection: sa.Connection, day: dt.date) -> None:
    connection.execute(
        sa.text(
            "DELETE FROM silver.score_quarantine WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": day},
    )
    connection.execute(
        sa.text(
            "DELETE FROM silver.article_summaries WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": day},
    )
    connection.execute(
        sa.text(
            "DELETE FROM silver.article_scores WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": day},
    )
    connection.execute(
        sa.text("DELETE FROM gold.fct_article_score WHERE first_seen_date = :d"),
        {"d": day},
    )
    connection.execute(
        sa.text("DELETE FROM gold.mart_pipeline_health WHERE pipeline_date = :d"),
        {"d": day},
    )
    connection.execute(
        sa.text("DELETE FROM silver.articles WHERE first_seen_date = :d"), {"d": day}
    )
    connection.execute(
        sa.text("DELETE FROM bronze.raw_articles WHERE ingest_date = :d"), {"d": day}
    )
    connection.execute(
        sa.text("DELETE FROM silver.source_health WHERE fetch_date = :d"), {"d": day}
    )
    connection.commit()


@pytest.fixture()
def clean_backfill_partitions(db_connection: sa.Connection) -> Iterator[sa.Connection]:
    _cleanup_partition(db_connection, DAY_D)
    _cleanup_partition(db_connection, DAY_D_MINUS_1)
    yield db_connection
    _cleanup_partition(db_connection, DAY_D)
    _cleanup_partition(db_connection, DAY_D_MINUS_1)


def test_backfill_idempotent_and_isolated(
    clean_backfill_partitions: sa.Connection,
) -> None:
    """3 khẳng định của DONE WHEN task 1.8, trong MỘT test (chia sẻ pipeline run tốn kém —
    3 lần build dbt thật):
    1. Materialize D -> ghi lại count mọi bảng.
    2. Materialize LẠI D -> count KHÔNG đổi ở MỌI bảng (P1).
    3. Materialize D-1 -> KHÔNG đụng count của D.
    """
    connection = clean_backfill_partitions

    # 1. Materialize D lần đầu.
    _run_full_pipeline_for_partition(connection, DAY_D)
    counts_after_first_run = _snapshot_counts(connection, DAY_D)

    # Sanity: pipeline phải thực sự ghi được gì đó — nếu không, test "không đổi" ở dưới sẽ
    # PASS giả vì 0 == 0 luôn đúng, không chứng minh được gì.
    assert counts_after_first_run["bronze.raw_articles"] > 0, (
        "Fixture RSS không ghi được dòng nào vào bronze — test không đo được gì, kiểm tra "
        "lại mock transport."
    )
    assert counts_after_first_run["silver.articles"] > 0
    assert counts_after_first_run["silver.article_scores"] > 0
    assert counts_after_first_run["silver.score_quarantine"] == 0, (
        "MockProvider mặc định luôn thành công — quarantine > 0 nghĩa là có lỗi thật, "
        "không phải hành vi mong đợi của test này."
    )

    # 2. Materialize LẠI ĐÚNG partition D — cùng now, cùng fixture, cùng mọi tham số.
    _run_full_pipeline_for_partition(connection, DAY_D)
    counts_after_second_run = _snapshot_counts(connection, DAY_D)

    assert counts_after_second_run == counts_after_first_run, (
        "Chạy lại CÙNG partition D phải cho count giống hệt ở MỌI bảng (P1 — tính lũy đẳng). "
        f"Lần 1: {counts_after_first_run}, lần 2: {counts_after_second_run}."
    )

    # 3. Materialize partition D-1 — KHÔNG được đụng dữ liệu của D.
    _run_full_pipeline_for_partition(connection, DAY_D_MINUS_1)
    counts_of_d_after_backfill = _snapshot_counts(connection, DAY_D)
    counts_of_d_minus_1 = _snapshot_counts(connection, DAY_D_MINUS_1)

    assert counts_of_d_after_backfill == counts_after_second_run, (
        "Materialize partition D-1 không được làm đổi count của partition D. "
        f"Trước: {counts_after_second_run}, sau khi chạy D-1: {counts_of_d_after_backfill}."
    )
    assert counts_of_d_minus_1["bronze.raw_articles"] > 0, (
        "Partition D-1 phải tự có dữ liệu riêng của nó, không phải 0 dòng."
    )
