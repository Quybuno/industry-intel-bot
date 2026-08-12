"""Chấm điểm + tóm tắt: đọc bài eligible, gọi provider, ghi kết quả (PRODUCTION_PLAN
§10.1-10.7, §4.4, §5.4, §21.1-21.2).

Có I/O — nhận connection/provider qua tham số, không tự tạo bên trong (để test được).
Logic quyết định (contract validation ở task 0.7) nằm ở module khác; ở đây chỉ orchestrate
đúng theo bảng lỗi §10.5.

**D1 (dọn nợ kỹ thuật, thay `composite.py` đã xoá):** chấm điểm (`run_score_partition`) và
tóm tắt top-K (`run_summarize_top_k_partition`) giờ là HAI bước TÁCH RỜI, không còn gọi lẫn
nhau trong cùng một hàm. Composite score dùng để xếp hạng top-K đọc THẲNG từ
`gold.fct_article_score` (dbt, công thức CHÍNH THỨC §5.7: credibility = 80% source tier +
20% LLM) — không tính lại trong Python (P5). Giữa hai bước, bên gọi (CLI `score`/asset
Dagster `article_summaries`) PHẢI đảm bảo dbt đã build `fct_article_score` cho đúng
partition_date; hàm ở đây không tự chạy dbt. Xem PROGRESS.md mục 9 (D1) để biết lý do đầy
đủ và các phương án đã cân nhắc.
"""

from __future__ import annotations

import datetime as dt
import json
import statistics
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from src.intel_bot.score.cost import ModelPricing, compute_cost_usd
from src.intel_bot.score.prompt_builder import build_score_prompt, build_summary_prompt
from src.intel_bot.score.providers.base import (
    FailureReason,
    LLMProvider,
    ProviderUnavailableError,
    ScoreRequest,
    ScoreSuccess,
    SummarySuccess,
)


@dataclass(frozen=True)
class EligibleArticle:
    """Một bài status='eligible' đọc để chấm."""

    article_id: uuid.UUID
    title: str
    snippet: str
    published_at: dt.datetime | None
    first_seen_at: dt.datetime
    published_at_imputed: bool


@dataclass
class RunnerResult:
    """Thống kê một lần chạy score (DONE WHEN: số thành công, quarantine theo lý do, chi
    phí, latency p50/p95)."""

    scored: int = 0
    quarantined: int = 0
    quarantine_by_reason: dict[str, int] = field(default_factory=dict)
    summarized: int = 0
    summary_quarantined: int = 0
    total_cost_usd: Decimal = field(default_factory=lambda: Decimal(0))
    latencies_ms: list[int] = field(default_factory=list)
    budget_stopped: bool = False
    provider_unavailable: bool = False

    def add_quarantine(self, reason: str) -> None:
        self.quarantine_by_reason[reason] = self.quarantine_by_reason.get(reason, 0) + 1

    @property
    def latency_p50_ms(self) -> float | None:
        return (
            float(statistics.median(self.latencies_ms)) if self.latencies_ms else None
        )

    @property
    def latency_p95_ms(self) -> float | None:
        if not self.latencies_ms:
            return None
        ordered = sorted(self.latencies_ms)
        index = min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))
        return float(ordered[index])


# ---------------------------------------------------------------------------
# I/O — đọc dữ liệu
# ---------------------------------------------------------------------------


def load_eligible_articles(
    connection: sa.Connection, *, first_seen_date: dt.date
) -> list[EligibleArticle]:
    """Đọc silver.articles status='eligible' của một ngày, thứ tự ổn định."""
    rows = connection.execute(
        sa.text(
            """
            SELECT article_id, title, snippet, published_at, first_seen_at, published_at_imputed
            FROM silver.articles
            WHERE first_seen_date = :first_seen_date AND status = 'eligible'
            ORDER BY article_id
            """
        ),
        {"first_seen_date": first_seen_date},
    ).all()
    return [
        EligibleArticle(
            article_id=row.article_id,
            title=row.title,
            snippet=row.snippet or "",
            published_at=row.published_at,
            first_seen_at=row.first_seen_at,
            published_at_imputed=row.published_at_imputed,
        )
        for row in rows
    ]


def load_top_k_from_fct_article_score(
    connection: sa.Connection, *, first_seen_date: dt.date, k: int
) -> list[EligibleArticle]:
    """Đọc K bài xếp hạng cao nhất theo `composite_score` CHÍNH THỨC từ
    `gold.fct_article_score` (dbt, §5.7: credibility = 80% source tier + 20% LLM, đã dedup
    cấp 2 ở `int_articles_deduped`) — KHÔNG tính lại composite trong Python (P5, D1). Xếp
    hạng (`ORDER BY ... LIMIT`) là một câu SQL, không phải logic xếp hạng viết bằng Python.

    YÊU CẦU: dbt đã build `fct_article_score` cho đúng `first_seen_date` này TRƯỚC khi gọi
    hàm này — xem `run_summarize_top_k_partition` và nơi gọi nó (cli.py/dagster asset
    `article_summaries`). Chỉ join `silver.articles` để lấy title/snippet cho prompt tóm
    tắt; không đọc lại cột điểm nào (đã dùng xong ở dbt)."""
    rows = connection.execute(
        sa.text(
            """
            SELECT a.article_id, a.title, a.snippet, a.published_at, a.first_seen_at,
                   a.published_at_imputed
            FROM gold.fct_article_score AS fct
            JOIN silver.articles AS a ON a.article_id = fct.article_id
            WHERE fct.first_seen_date = :first_seen_date
            ORDER BY fct.composite_score DESC, fct.article_id ASC
            LIMIT :k
            """
        ),
        {"first_seen_date": first_seen_date, "k": k},
    ).all()
    return [
        EligibleArticle(
            article_id=row.article_id,
            title=row.title,
            snippet=row.snippet or "",
            published_at=row.published_at,
            first_seen_at=row.first_seen_at,
            published_at_imputed=row.published_at_imputed,
        )
        for row in rows
    ]


def get_daily_cost_so_far(connection: sa.Connection, *, day: dt.date) -> Decimal:
    """Tổng cost_usd đã ghi vào silver.article_scores trong ngày (giờ hệ thống) — dùng để
    kiểm tra ngân sách trước mỗi batch (§10.5, §21.2)."""
    total = connection.execute(
        sa.text(
            "SELECT COALESCE(SUM(cost_usd), 0) FROM silver.article_scores WHERE scored_at::date = :day"
        ),
        {"day": day},
    ).scalar_one()
    return Decimal(total)


# ---------------------------------------------------------------------------
# I/O — ghi dữ liệu
# ---------------------------------------------------------------------------


def upsert_article_score(
    connection: sa.Connection,
    *,
    article_id: uuid.UUID,
    model_name: str,
    prompt_version: str,
    credibility: int,
    importance: int,
    depth: int,
    practicality: int,
    confidence: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: Decimal,
    latency_ms: int,
    scored_at: dt.datetime,
) -> bool:
    """INSERT ... ON CONFLICT (article_id, prompt_version, model_name) DO NOTHING.

    Chạy lại không sinh dòng trùng (task 0.8 mục 4, P1). Trả về True nếu là dòng MỚI.
    """
    result = connection.execute(
        sa.text(
            """
            INSERT INTO silver.article_scores (
                score_id, article_id, model_name, prompt_version, credibility, importance,
                depth, practicality, confidence, input_tokens, output_tokens, cost_usd,
                latency_ms, scored_at
            ) VALUES (
                :score_id, :article_id, :model_name, :prompt_version, :credibility, :importance,
                :depth, :practicality, :confidence, :input_tokens, :output_tokens, :cost_usd,
                :latency_ms, :scored_at
            )
            ON CONFLICT (article_id, prompt_version, model_name) DO NOTHING
            """
        ).bindparams(
            sa.bindparam("score_id", type_=postgresql.UUID),
            sa.bindparam("article_id", type_=postgresql.UUID),
        ),
        {
            "score_id": uuid.uuid4(),
            "article_id": article_id,
            "model_name": model_name,
            "prompt_version": prompt_version,
            "credibility": credibility,
            "importance": importance,
            "depth": depth,
            "practicality": practicality,
            "confidence": confidence,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_usd": cost_usd,
            "latency_ms": latency_ms,
            "scored_at": scored_at,
        },
    )
    return result.rowcount > 0


def upsert_article_summary(
    connection: sa.Connection,
    *,
    article_id: uuid.UUID,
    model_name: str,
    prompt_version: str,
    summary_vi: list[str],
    why_it_matters_vi: str,
    created_at: dt.datetime,
) -> bool:
    """INSERT ... ON CONFLICT (article_id, prompt_version, model_name) DO NOTHING.

    KHÔNG ghi cột điểm nào — điểm và tóm tắt có vòng đời khác nhau (§4.4, §5.4). Ràng buộc
    unique bổ sung ở migration 0004 (bảng gốc chưa có, xem ghi chú ở đó) để idempotent
    giống hệt article_scores (P1).
    """
    result = connection.execute(
        sa.text(
            """
            INSERT INTO silver.article_summaries (
                summary_id, article_id, model_name, prompt_version, summary_vi,
                why_it_matters_vi, created_at
            ) VALUES (
                :summary_id, :article_id, :model_name, :prompt_version, :summary_vi,
                :why_it_matters_vi, :created_at
            )
            ON CONFLICT (article_id, prompt_version, model_name) DO NOTHING
            """
        ).bindparams(
            sa.bindparam("summary_id", type_=postgresql.UUID),
            sa.bindparam("article_id", type_=postgresql.UUID),
            sa.bindparam("summary_vi", type_=postgresql.JSONB),
        ),
        {
            "summary_id": uuid.uuid4(),
            "article_id": article_id,
            "model_name": model_name,
            "prompt_version": prompt_version,
            "summary_vi": summary_vi,
            "why_it_matters_vi": why_it_matters_vi,
            "created_at": created_at,
        },
    )
    return result.rowcount > 0


def insert_quarantine(
    connection: sa.Connection,
    *,
    article_id: uuid.UUID,
    prompt_version: str,
    raw_response_text: str,
    failure_reason: FailureReason,
    attempt_no: int,
    created_at: dt.datetime,
) -> None:
    """Ghi silver.score_quarantine kèm NGUYÊN VĂN response.

    Nếu response không parse được JSON thì lưu dạng {"text": "..."} (task 0.8 mục 4).
    Dùng chung cho cả lỗi chấm điểm lẫn lỗi tóm tắt — phân biệt bằng prompt_version.
    """
    connection.execute(
        sa.text(
            """
            INSERT INTO silver.score_quarantine (
                article_id, prompt_version, raw_response, failure_reason, attempt_no, created_at
            ) VALUES (
                :article_id, :prompt_version, :raw_response, :failure_reason, :attempt_no, :created_at
            )
            """
        ).bindparams(
            sa.bindparam("article_id", type_=postgresql.UUID),
            sa.bindparam("raw_response", type_=postgresql.JSONB),
        ),
        {
            "article_id": article_id,
            "prompt_version": prompt_version,
            "raw_response": _wrap_raw_response(raw_response_text),
            "failure_reason": failure_reason,
            "attempt_no": attempt_no,
            "created_at": created_at,
        },
    )


def _wrap_raw_response(text: str) -> object:
    """Parse được thì lưu nguyên JSON; không parse được thì bọc {"text": "..."} (JSONB NOT NULL)."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


def mark_article_status(
    connection: sa.Connection, *, article_id: uuid.UUID, status: str
) -> None:
    """Cập nhật status bài — KHÔNG xoá bản ghi (P2 tinh thần, task 0.6 đã áp dụng tương tự)."""
    connection.execute(
        sa.text(
            "UPDATE silver.articles SET status = :status WHERE article_id = :article_id"
        ).bindparams(sa.bindparam("article_id", type_=postgresql.UUID)),
        {"article_id": article_id, "status": status},
    )


def mark_article_scored(
    connection: sa.Connection, *, article_id: uuid.UUID, industry_tags: list[str]
) -> None:
    """status='scored' + ghi industry_tags (đã lọc tag lạ ở contract, §10.4/§10.5) vào
    silver.articles.industry_tags — cột này thuộc articles (§5.3), không phải article_scores."""
    connection.execute(
        sa.text(
            "UPDATE silver.articles SET status = 'scored', industry_tags = :industry_tags"
            " WHERE article_id = :article_id"
        ).bindparams(
            sa.bindparam("article_id", type_=postgresql.UUID),
            sa.bindparam("industry_tags", type_=postgresql.ARRAY(sa.Text())),
        ),
        {"article_id": article_id, "industry_tags": industry_tags},
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _chunked(items: list[EligibleArticle], size: int) -> list[list[EligibleArticle]]:
    return [items[i : i + size] for i in range(0, len(items), max(1, size))]


def run_score_partition(
    connection: sa.Connection,
    *,
    partition_date: dt.date,
    provider: LLMProvider,
    pricing: ModelPricing,
    daily_budget_usd: Decimal,
    batch_size: int,
    now: dt.datetime,
) -> RunnerResult:
    """Chấm điểm cho một partition. Xử lý lỗi ĐÚNG bảng §10.5.

    **D1:** hàm này giờ CHỈ chấm điểm — KHÔNG còn tự tóm tắt top-K (khác 0.8/0.9). Muốn tóm
    tắt top-K, gọi `run_summarize_top_k_partition()` riêng SAU KHI dbt đã build
    `gold.fct_article_score` cho `partition_date` này (composite score chính thức §5.7 cần
    dbt tính — xem docstring module và PROGRESS.md mục 9 D1). Bên gọi (cli.py `score`, asset
    Dagster `article_scores`/`article_summaries`) chịu trách nhiệm xen bước dbt build vào
    giữa hai lời gọi.

    Idempotent: bài đã 'scored'/'quarantined' không còn 'eligible' nên lần chạy sau không
    xử lý lại; article_scores còn có ON CONFLICT DO NOTHING làm lớp bảo vệ thứ hai (vd. khi
    job bị ngắt giữa chừng sau INSERT nhưng trước UPDATE status).
    """
    result = RunnerResult()
    articles = load_eligible_articles(connection, first_seen_date=partition_date)

    for chunk in _chunked(articles, batch_size):
        built_prompts = [
            build_score_prompt(title=a.title, snippet=a.snippet) for a in chunk
        ]
        requests = [
            ScoreRequest(
                article_id=a.article_id,
                title=a.title,
                snippet=a.snippet,
                prompt=bp.text,
                prompt_version=bp.prompt_version,
            )
            for a, bp in zip(chunk, built_prompts, strict=True)
        ]

        cost_so_far = get_daily_cost_so_far(connection, day=now.date())
        estimated_cost = provider.estimate_cost(requests)
        if cost_so_far + estimated_cost > daily_budget_usd:
            # Vượt ngân sách ngày: dừng chấm, giữ nguyên các bài còn lại ở 'eligible' (§10.5).
            result.budget_stopped = True
            break

        try:
            outcomes = provider.score_batch(requests)
        except ProviderUnavailableError:
            # Provider không dùng được: KHÔNG mark hàng loạt quarantined (§10.5).
            result.provider_unavailable = True
            break

        for article, outcome in zip(chunk, outcomes, strict=True):
            if isinstance(outcome, ScoreSuccess):
                cost_usd = compute_cost_usd(
                    pricing=pricing,
                    input_cache_hit_tokens=outcome.input_cache_hit_tokens,
                    input_cache_miss_tokens=outcome.input_tokens
                    - outcome.input_cache_hit_tokens,
                    output_tokens=outcome.output_tokens,
                )
                upsert_article_score(
                    connection,
                    article_id=article.article_id,
                    model_name=outcome.model_name,
                    prompt_version=outcome.prompt_version,
                    credibility=outcome.result.credibility,
                    importance=outcome.result.importance,
                    depth=outcome.result.depth,
                    practicality=outcome.result.practicality,
                    confidence=outcome.result.confidence,
                    input_tokens=outcome.input_tokens,
                    output_tokens=outcome.output_tokens,
                    cost_usd=cost_usd,
                    latency_ms=outcome.latency_ms,
                    scored_at=now,
                )
                mark_article_scored(
                    connection,
                    article_id=article.article_id,
                    industry_tags=outcome.result.industry_tags,
                )
                result.scored += 1
                result.total_cost_usd += cost_usd
                result.latencies_ms.append(outcome.latency_ms)
            else:
                insert_quarantine(
                    connection,
                    article_id=article.article_id,
                    prompt_version=outcome.prompt_version,
                    raw_response_text=outcome.raw_response,
                    failure_reason=outcome.failure_reason,
                    attempt_no=outcome.attempt_no,
                    created_at=now,
                )
                mark_article_status(
                    connection, article_id=article.article_id, status="quarantined"
                )
                result.quarantined += 1
                result.add_quarantine(outcome.failure_reason)

        connection.commit()

    return result


def run_summarize_top_k_partition(
    connection: sa.Connection,
    *,
    partition_date: dt.date,
    provider: LLMProvider,
    pricing: ModelPricing,
    daily_budget_usd: Decimal,
    top_k_summaries: int,
    now: dt.datetime,
    result: RunnerResult,
) -> None:
    """Sinh tóm tắt cho top-K bài theo composite score CHÍNH THỨC — chỉ top-K, không phải
    toàn bộ (§21.2).

    **D1:** thay cho `_summarize_top_k` cũ (đã xoá cùng `composite.py`) — đọc top-K từ
    `gold.fct_article_score` (`load_top_k_from_fct_article_score`) thay vì tính composite
    tạm trong Python. YÊU CẦU bên gọi đã chạy `dbt build` cho `fct_article_score` của
    `partition_date` này trước khi gọi hàm này (xem cli.py `score` / asset Dagster
    `article_summaries`) — hàm không tự kiểm tra hay tự chạy dbt.

    `result` truyền vào và bị SỬA TRỰC TIẾP (cùng object với kết quả `run_score_partition`
    trả về) để bên gọi gộp thống kê cả hai bước vào một `RunnerResult` duy nhất, giữ đúng
    hình dạng báo cáo cũ (`summarized`, `summary_quarantined`, `total_cost_usd` cộng dồn).
    """
    top_k = load_top_k_from_fct_article_score(
        connection, first_seen_date=partition_date, k=top_k_summaries
    )
    if not top_k:
        return

    built_prompts = [
        build_summary_prompt(title=a.title, snippet=a.snippet) for a in top_k
    ]
    requests = [
        ScoreRequest(
            article_id=a.article_id,
            title=a.title,
            snippet=a.snippet,
            prompt=bp.text,
            prompt_version=bp.prompt_version,
        )
        for a, bp in zip(top_k, built_prompts, strict=True)
    ]

    cost_so_far = get_daily_cost_so_far(connection, day=now.date())
    estimated_cost = provider.estimate_cost(requests)
    if cost_so_far + estimated_cost > daily_budget_usd:
        result.budget_stopped = True
        return

    try:
        outcomes = provider.summarize_batch(requests)
    except ProviderUnavailableError:
        result.provider_unavailable = True
        return

    for outcome in outcomes:
        if isinstance(outcome, SummarySuccess):
            cost_usd = compute_cost_usd(
                pricing=pricing,
                input_cache_hit_tokens=outcome.input_cache_hit_tokens,
                input_cache_miss_tokens=outcome.input_tokens
                - outcome.input_cache_hit_tokens,
                output_tokens=outcome.output_tokens,
            )
            upsert_article_summary(
                connection,
                article_id=outcome.article_id,
                model_name=outcome.model_name,
                prompt_version=outcome.prompt_version,
                summary_vi=outcome.result.summary_vi,
                why_it_matters_vi=outcome.result.why_it_matters_vi,
                created_at=now,
            )
            result.summarized += 1
            result.total_cost_usd += cost_usd
        else:
            insert_quarantine(
                connection,
                article_id=outcome.article_id,
                prompt_version=outcome.prompt_version,
                raw_response_text=outcome.raw_response,
                failure_reason=outcome.failure_reason,
                attempt_no=outcome.attempt_no,
                created_at=now,
            )
            result.summary_quarantined += 1

    connection.commit()
