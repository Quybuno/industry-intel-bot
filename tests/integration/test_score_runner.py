"""Integration test cho runner.py — Postgres thật (PRODUCTION_PLAN §20.2), provider MOCK
(§7 rào chắn task 0.8/0.9: test tuyệt đối không gọi API thật).

Mỗi dòng trong bảng lỗi §10.5 có một test tương ứng — xem tên hàm test theo từng dòng.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.dialects import postgresql

from src.intel_bot.contracts.llm_score import ScoreResult
from src.intel_bot.score.providers.base import (
    ScoreFailure,
    ScoreOutcome,
    ScoreRequest,
    ScoreSuccess,
    SummaryOutcome,
    classify_validation_error,
)
from src.intel_bot.score.providers.mock import (
    ZERO_PRICING,
    MockFailureRates,
    MockProvider,
)
from src.intel_bot.score.runner import (
    RunnerResult,
    load_eligible_articles,
    load_top_k_from_fct_article_score,
    run_score_partition,
    run_summarize_top_k_partition,
)

TEST_PARTITION_DATE = dt.date(
    2000, 1, 3
)  # ngày riêng cho test — không đụng dữ liệu thật
NOW = dt.datetime(2000, 1, 3, 12, 0, 0, tzinfo=dt.UTC)
LONG_SNIPPET = (
    "Đây là snippet đủ dài cho test task 0.8/0.9, lặp lại vài lần cho chắc. " * 2
)


def _insert_eligible_article(
    connection: sa.Connection,
    *,
    slug: str,
    title: str = "Tiêu đề test",
    published_at: dt.datetime = NOW,
) -> uuid.UUID:
    article_id = uuid.uuid5(uuid.NAMESPACE_URL, f"https://fixture.test/score/{slug}")
    connection.execute(
        sa.text(
            """
            INSERT INTO silver.articles (
                article_id, canonical_url, raw_url, content_hash, source_id, title, snippet,
                published_at, first_seen_at, first_seen_date, status, published_at_imputed
            ) VALUES (
                :article_id, :url, :url, :hash, 'test_score_source', :title, :snippet,
                :published_at, :first_seen_at, :first_seen_date, 'eligible', false
            )
            """
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {
            "article_id": article_id,
            "url": f"https://fixture.test/score/{slug}",
            "hash": f"hash-{slug}",
            "title": title,
            "snippet": LONG_SNIPPET,
            "published_at": published_at,
            "first_seen_at": NOW,
            "first_seen_date": TEST_PARTITION_DATE,
        },
    )
    return article_id


def _insert_fct_article_score_row(
    connection: sa.Connection,
    *,
    article_id: uuid.UUID,
    composite_score: float,
) -> None:
    """Chèn thẳng một dòng vào gold.fct_article_score, KHÔNG qua dbt — cùng mẫu đã dùng ở
    tests/test_publish_runner.py cho gold.mart_daily_digest. D1: `run_summarize_top_k_partition`
    đọc top-K từ bảng này (composite_score CHÍNH THỨC §5.7), nên test cho hàm đó phải tự
    dựng dữ liệu gold thay vì chạy dbt build thật (chậm, cần DBT_PROFILES_DIR)."""
    connection.execute(
        sa.text(
            """
            INSERT INTO gold.fct_article_score (
                score_id, article_id, source_id, model_name, prompt_version,
                first_seen_date, first_seen_at, published_at, published_at_imputed,
                llm_credibility, importance, depth, practicality, confidence,
                source_tier, source_tier_score, credibility_blended, recency_boost,
                composite_score, content_hash_group_size
            ) VALUES (
                :score_id, :article_id, 'test_score_source', 'mock', 'score_v2.0.0',
                :first_seen_date, :now, :now, false,
                5, 5, 5, 5, 'high',
                1, 10, 5.0, 0.0,
                :composite_score, 1
            )
            """
        ).bindparams(
            sa.bindparam("score_id", type_=postgresql.UUID),
            sa.bindparam("article_id", type_=postgresql.UUID),
        ),
        {
            "score_id": uuid.uuid4(),
            "article_id": article_id,
            "first_seen_date": TEST_PARTITION_DATE,
            "now": NOW,
            "composite_score": composite_score,
        },
    )


def _cleanup(connection: sa.Connection) -> None:
    connection.execute(
        sa.text(
            "DELETE FROM gold.fct_article_score WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": TEST_PARTITION_DATE},
    )
    connection.execute(
        sa.text(
            "DELETE FROM silver.score_quarantine WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": TEST_PARTITION_DATE},
    )
    connection.execute(
        sa.text(
            "DELETE FROM silver.article_summaries WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": TEST_PARTITION_DATE},
    )
    connection.execute(
        sa.text(
            "DELETE FROM silver.article_scores WHERE article_id IN "
            "(SELECT article_id FROM silver.articles WHERE first_seen_date = :d)"
        ),
        {"d": TEST_PARTITION_DATE},
    )
    connection.execute(
        sa.text("DELETE FROM silver.articles WHERE first_seen_date = :d"),
        {"d": TEST_PARTITION_DATE},
    )
    connection.commit()


@pytest.fixture()
def clean_partition(db_connection: sa.Connection) -> Iterator[sa.Connection]:
    _cleanup(db_connection)
    yield db_connection
    _cleanup(db_connection)


def _run(
    connection: sa.Connection,
    *,
    provider: object,
    daily_budget_usd: Decimal = Decimal(1000),
    batch_size: int = 10,
):
    return run_score_partition(
        connection,
        partition_date=TEST_PARTITION_DATE,
        provider=provider,  # type: ignore[arg-type]
        pricing=ZERO_PRICING,
        daily_budget_usd=daily_budget_usd,
        batch_size=batch_size,
        now=NOW,
    )


def _quarantine_rows(connection: sa.Connection, article_id: uuid.UUID) -> list[sa.Row]:
    return connection.execute(
        sa.text(
            "SELECT failure_reason, raw_response, attempt_no FROM silver.score_quarantine"
            " WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": article_id},
    ).all()


def _article_status(connection: sa.Connection, article_id: uuid.UUID) -> str:
    return connection.execute(
        sa.text(
            "SELECT status FROM silver.articles WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": article_id},
    ).scalar_one()


# ---------------------------------------------------------------------------
# §10.5 dòng 1: JSON không parse được → retry 1 lần → (mock: final) → quarantine
# ---------------------------------------------------------------------------


def test_error_table_json_parse_error_goes_to_quarantine(
    clean_partition: sa.Connection,
) -> None:
    article_id = _insert_eligible_article(clean_partition, slug="json-error")
    clean_partition.commit()

    provider = MockProvider(failure_rates=MockFailureRates(json_parse_error=1.0))
    result = _run(clean_partition, provider=provider)

    assert result.quarantined == 1
    assert result.quarantine_by_reason == {"json_parse_error": 1}
    rows = _quarantine_rows(clean_partition, article_id)
    assert len(rows) == 1
    assert rows[0].failure_reason == "json_parse_error"
    assert _article_status(clean_partition, article_id) == "quarantined"


# ---------------------------------------------------------------------------
# §10.5 dòng 2: Vi phạm schema → retry 1 lần; lần 2 → quarantine
# ---------------------------------------------------------------------------


def test_error_table_schema_violation_goes_to_quarantine(
    clean_partition: sa.Connection,
) -> None:
    article_id = _insert_eligible_article(clean_partition, slug="schema-violation")
    clean_partition.commit()

    provider = MockProvider(failure_rates=MockFailureRates(schema_violation=1.0))
    result = _run(clean_partition, provider=provider)

    assert result.quarantine_by_reason == {"schema_violation": 1}
    rows = _quarantine_rows(clean_partition, article_id)
    assert rows[0].failure_reason == "schema_violation"
    # JSON hợp lệ cú pháp, chỉ thiếu trường — parse được, không phải {"text": ...}.
    assert "confidence" not in rows[0].raw_response
    assert _article_status(clean_partition, article_id) == "quarantined"


# ---------------------------------------------------------------------------
# §10.5 dòng 3: Điểm ngoài 1-10 → quarantine NGAY, không retry, KHÔNG clamp
# ---------------------------------------------------------------------------


@dataclass
class _FixedRawResponseProvider:
    """Provider giả cố định trả đúng 1 raw_response cho mọi request — dùng để test chính
    xác giá trị điểm=15 mà MockProvider (chỉ sinh 11 hoặc 0) không tạo ra được."""

    raw_payload: dict[str, object]

    def score_batch(self, items: list[ScoreRequest]) -> list[ScoreOutcome]:
        text = json.dumps(self.raw_payload, ensure_ascii=False)
        outcomes: list[ScoreOutcome] = []
        for item in items:
            try:
                parsed = ScoreResult.model_validate(json.loads(text))
            except ValidationError as exc:
                outcomes.append(
                    ScoreFailure(
                        article_id=item.article_id,
                        raw_response=text,
                        failure_reason=classify_validation_error(exc),
                        model_name="fixed-test-provider",
                        prompt_version=item.prompt_version,
                        attempt_no=1,
                    )
                )
                continue
            outcomes.append(
                ScoreSuccess(
                    article_id=item.article_id,
                    result=parsed,
                    input_tokens=10,
                    output_tokens=10,
                    latency_ms=1,
                    model_name="fixed-test-provider",
                    prompt_version=item.prompt_version,
                )
            )
        return outcomes

    def summarize_batch(self, items: list[ScoreRequest]) -> list[SummaryOutcome]:
        return []

    def estimate_cost(self, items: list[ScoreRequest]) -> Decimal:
        return Decimal(0)


def test_error_table_score_15_quarantined_not_clamped(
    clean_partition: sa.Connection,
) -> None:
    """Test bắt buộc: điểm = 15 → quarantine failure_reason='out_of_range', KHÔNG bị clamp về 10."""
    article_id = _insert_eligible_article(clean_partition, slug="score-15")
    clean_partition.commit()

    provider = _FixedRawResponseProvider(
        raw_payload={
            "credibility": 5,
            "importance": 15,
            "depth": 5,
            "practicality": 5,
            "industry_tags": ["ai"],
            "confidence": "high",
        }
    )
    result = _run(clean_partition, provider=provider)

    assert result.quarantine_by_reason == {"out_of_range": 1}
    rows = _quarantine_rows(clean_partition, article_id)
    assert rows[0].failure_reason == "out_of_range"
    # Nguyên văn 15 phải còn trong raw_response — không bị sửa/clamp trước khi lưu.
    assert rows[0].raw_response["importance"] == 15

    # KHÔNG được có dòng nào trong article_scores cho bài này (không clamp về 10 rồi ghi).
    scored_count = clean_partition.execute(
        sa.text(
            "SELECT count(*) FROM silver.article_scores WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": article_id},
    ).scalar_one()
    assert scored_count == 0
    assert _article_status(clean_partition, article_id) == "quarantined"


def test_error_table_out_of_range_via_mock_boundary_values(
    clean_partition: sa.Connection,
) -> None:
    """MockProvider tự sinh 11 hoặc 0 (hai biên) khi ép out_of_range — cả hai đều bị quarantine."""
    article_id = _insert_eligible_article(clean_partition, slug="out-of-range-mock")
    clean_partition.commit()

    provider = MockProvider(failure_rates=MockFailureRates(out_of_range=1.0))
    result = _run(clean_partition, provider=provider)

    assert result.quarantine_by_reason == {"out_of_range": 1}
    rows = _quarantine_rows(clean_partition, article_id)
    assert rows[0].raw_response["importance"] in (0, 11)


# ---------------------------------------------------------------------------
# §10.5 dòng 4: Provider timeout → retry 2 lần → (mock: final) → quarantine
# ---------------------------------------------------------------------------


def test_error_table_timeout_goes_to_quarantine(clean_partition: sa.Connection) -> None:
    article_id = _insert_eligible_article(clean_partition, slug="timeout")
    clean_partition.commit()

    provider = MockProvider(failure_rates=MockFailureRates(timeout=1.0))
    result = _run(clean_partition, provider=provider)

    assert result.quarantine_by_reason == {"timeout": 1}
    rows = _quarantine_rows(clean_partition, article_id)
    assert rows[0].failure_reason == "timeout"
    # raw_response rỗng (không có gì để parse) -> bọc {"text": ""} vì JSONB NOT NULL.
    assert rows[0].raw_response == {"text": ""}


# ---------------------------------------------------------------------------
# §10.5 dòng 5: Provider không dùng được → Asset fail, KHÔNG mark hàng loạt quarantined
# ---------------------------------------------------------------------------


def test_error_table_provider_unavailable_does_not_mass_quarantine(
    clean_partition: sa.Connection,
) -> None:
    ids = [
        _insert_eligible_article(clean_partition, slug=f"unavail-{i}") for i in range(3)
    ]
    clean_partition.commit()

    provider = MockProvider(raise_unavailable=True)
    result = _run(clean_partition, provider=provider)

    assert result.provider_unavailable is True
    assert result.quarantined == 0
    assert result.scored == 0
    for article_id in ids:
        assert _article_status(clean_partition, article_id) == "eligible"
        assert _quarantine_rows(clean_partition, article_id) == []


# ---------------------------------------------------------------------------
# §10.5 dòng 6: Vượt ngân sách ngày → dừng chấm, giữ bài ở 'eligible'
# ---------------------------------------------------------------------------


def test_error_table_budget_exceeded_stops_and_keeps_eligible(
    clean_partition: sa.Connection,
) -> None:
    ids = [
        _insert_eligible_article(clean_partition, slug=f"budget-{i}") for i in range(3)
    ]
    clean_partition.commit()

    # Ngân sách âm -> luôn vượt ngay từ batch đầu, bất kể chi phí thật hôm nay là bao nhiêu.
    provider = MockProvider()
    result = _run(clean_partition, provider=provider, daily_budget_usd=Decimal(-1))

    assert result.budget_stopped is True
    assert result.scored == 0
    for article_id in ids:
        assert _article_status(clean_partition, article_id) == "eligible"


# ---------------------------------------------------------------------------
# §10.5 dòng 7: Tag ngoài tập đóng → bỏ tag, ghi warning, GIỮ bản ghi
# ---------------------------------------------------------------------------


def test_error_table_unknown_tag_dropped_but_article_scored_and_tags_written(
    clean_partition: sa.Connection,
) -> None:
    article_id = _insert_eligible_article(clean_partition, slug="unknown-tag")
    clean_partition.commit()

    provider = _FixedRawResponseProvider(
        raw_payload={
            "credibility": 5,
            "importance": 5,
            "depth": 5,
            "practicality": 5,
            "industry_tags": ["ai", "totally_made_up_tag"],
            "confidence": "high",
        }
    )
    result = _run(clean_partition, provider=provider)

    assert result.scored == 1
    assert result.quarantined == 0
    assert _article_status(clean_partition, article_id) == "scored"

    tags = clean_partition.execute(
        sa.text(
            "SELECT industry_tags FROM silver.articles WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": article_id},
    ).scalar_one()
    assert tags == ["ai"]  # tag lạ đã bị loại, tag hợp lệ vẫn còn


# ---------------------------------------------------------------------------
# Idempotency: chạy score 2 lần cùng partition → article_scores không sinh dòng trùng
# ---------------------------------------------------------------------------


def test_run_twice_same_partition_no_duplicate_scores(
    clean_partition: sa.Connection,
) -> None:
    _insert_eligible_article(clean_partition, slug="idempotent-1")
    _insert_eligible_article(clean_partition, slug="idempotent-2")
    clean_partition.commit()

    provider = MockProvider()
    first = _run(clean_partition, provider=provider)
    count_after_first = clean_partition.execute(
        sa.text(
            "SELECT count(*) FROM silver.article_scores s JOIN silver.articles a"
            " ON a.article_id = s.article_id WHERE a.first_seen_date = :d"
        ),
        {"d": TEST_PARTITION_DATE},
    ).scalar_one()

    second = _run(clean_partition, provider=provider)
    count_after_second = clean_partition.execute(
        sa.text(
            "SELECT count(*) FROM silver.article_scores s JOIN silver.articles a"
            " ON a.article_id = s.article_id WHERE a.first_seen_date = :d"
        ),
        {"d": TEST_PARTITION_DATE},
    ).scalar_one()

    assert first.scored == 2
    assert second.scored == 0  # không còn bài 'eligible' nào ở lần 2
    assert count_after_first == 2
    assert count_after_second == count_after_first


# ---------------------------------------------------------------------------
# D1: run_score_partition() và run_summarize_top_k_partition() giờ TÁCH RỜI — top-K đọc
# composite score CHÍNH THỨC từ gold.fct_article_score (dbt), không tính lại trong Python
# (composite.py đã xoá). Test chèn thẳng fixture vào gold.fct_article_score (không chạy dbt
# thật, giống mẫu tests/test_publish_runner.py) để test hàm summarize độc lập với dbt build.
# ---------------------------------------------------------------------------


def test_load_top_k_from_fct_article_score_orders_by_composite_desc(
    clean_partition: sa.Connection,
) -> None:
    ids = [
        _insert_eligible_article(clean_partition, slug=f"fct-order-{i}")
        for i in range(3)
    ]
    clean_partition.commit()
    # Composite cố tình KHÔNG theo thứ tự chèn, để chứng minh ORDER BY thật sự sắp lại.
    for article_id, composite in zip(ids, [3.0, 9.0, 5.0], strict=True):
        _insert_fct_article_score_row(
            clean_partition, article_id=article_id, composite_score=composite
        )
    clean_partition.commit()

    top_2 = load_top_k_from_fct_article_score(
        clean_partition, first_seen_date=TEST_PARTITION_DATE, k=2
    )

    assert [a.article_id for a in top_2] == [ids[1], ids[2]]  # composite 9.0 rồi 5.0


def test_run_summarize_top_k_partition_only_summarizes_top_k(
    clean_partition: sa.Connection,
) -> None:
    """D1: chấm điểm (run_score_partition) KHÔNG còn tự tóm tắt — phải gọi
    run_summarize_top_k_partition() riêng, đọc top-K từ gold.fct_article_score (fixture chèn
    thẳng ở đây, KHÔNG chạy dbt thật)."""
    ids = [
        _insert_eligible_article(clean_partition, slug=f"summarize-{i}")
        for i in range(5)
    ]
    clean_partition.commit()

    provider = MockProvider()
    score_result = _run(clean_partition, provider=provider)
    assert score_result.scored == 5
    assert (
        score_result.summarized == 0
    )  # D1: không còn tự tóm tắt trong run_score_partition

    for i, article_id in enumerate(ids):
        _insert_fct_article_score_row(
            clean_partition, article_id=article_id, composite_score=float(i)
        )
    clean_partition.commit()

    run_summarize_top_k_partition(
        clean_partition,
        partition_date=TEST_PARTITION_DATE,
        provider=provider,
        pricing=ZERO_PRICING,
        daily_budget_usd=Decimal(1000),
        top_k_summaries=2,
        now=NOW,
        result=score_result,
    )

    assert score_result.summarized == 2

    summarized_ids = {
        row.article_id
        for row in clean_partition.execute(
            sa.text(
                "SELECT article_id FROM silver.article_summaries WHERE article_id = ANY(:ids)"
            ).bindparams(sa.bindparam("ids", type_=postgresql.ARRAY(postgresql.UUID))),
            {"ids": ids},
        ).all()
    }
    # composite cao nhất = index 4 và 3 (composite_score = float(i)).
    assert summarized_ids == {ids[4], ids[3]}


def test_run_summarize_top_k_partition_no_fct_rows_summarizes_nothing(
    clean_partition: sa.Connection,
) -> None:
    """gold.fct_article_score rỗng cho partition (vd. dbt build chưa chạy/chưa có bài nào
    qua được is_production_model()) — không có gì để tóm tắt, không lỗi."""
    result = RunnerResult()
    run_summarize_top_k_partition(
        clean_partition,
        partition_date=TEST_PARTITION_DATE,
        provider=MockProvider(),
        pricing=ZERO_PRICING,
        daily_budget_usd=Decimal(1000),
        top_k_summaries=5,
        now=NOW,
        result=result,
    )
    assert result.summarized == 0


def test_summary_table_has_no_score_columns(clean_partition: sa.Connection) -> None:
    """§4.4/§5.4: điểm và tóm tắt vòng đời khác nhau — article_summaries không có cột điểm."""
    columns = {
        row.column_name
        for row in clean_partition.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = 'silver' AND table_name = 'article_summaries'"
            )
        ).all()
    }
    assert columns.isdisjoint(
        {"credibility", "importance", "depth", "practicality", "confidence"}
    )


# ---------------------------------------------------------------------------
# load_eligible_articles — sanity đọc đúng partition
# ---------------------------------------------------------------------------


def test_load_eligible_articles_only_returns_this_partition(
    clean_partition: sa.Connection,
) -> None:
    _insert_eligible_article(clean_partition, slug="loader-check")
    clean_partition.commit()
    articles = load_eligible_articles(
        clean_partition, first_seen_date=TEST_PARTITION_DATE
    )
    assert len(articles) == 1
    assert articles[0].title == "Tiêu đề test"
