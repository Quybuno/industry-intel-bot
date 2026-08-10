"""Tạo bảng bronze + silver theo docs/PRODUCTION_PLAN.md mục 5.2-5.5.

Không tạo bảng nào thuộc schema gold (dbt sinh ở task 0.10).

Quyết định thiết kế đáng chú ý (không có trong plan, cần biết khi đọc lại migration này):
- `silver.articles.source_id` KHÔNG có FK tới `gold.dim_source`: dim_source do dbt snapshot
  sinh ra ở task 0.10, chưa tồn tại lúc migration này chạy, và vòng đời của nó (snapshot,
  có thể rebuild) không phù hợp làm mục tiêu FK ổn định từ silver.
- Mọi UUID PK (`article_id`, `score_id`, `summary_id`) KHÔNG có server default — ứng dụng
  tự sinh (UUIDv5 cho article_id theo đúng plan 5.3), tránh phụ thuộc extension
  pgcrypto/uuid-ossp không cần thiết ở quy mô này (P8).
- `silver.source_health` không nằm trong plan mục 5, cột lấy nguyên theo mô tả ở task 0.2/0.3
  phần E, cộng thêm `fetched_at TIMESTAMPTZ` để có mốc thời gian ghi nhận (theo quy ước
  AGENTS.md — mọi thời điểm là TIMESTAMPTZ) và unique (source_id, fetch_date) để job ingest
  ghi đè idempotent theo ngày (P1), thay vì chèn trùng mỗi lần chạy lại.

Revision ID: 0002_bronze_silver_tables
Revises: 0001_create_schemas
Create Date: 2026-08-10

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0002_bronze_silver_tables"
down_revision: Union[str, None] = "0001_create_schemas"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ---------- bronze.raw_articles ----------
    op.create_table(
        "raw_articles",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("ingest_date", sa.Date(), nullable=False),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("raw_url", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("payload_hash", sa.Text(), nullable=False),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "ingest_date", "payload_hash", name="uq_raw_articles_ingest_date_payload_hash"
        ),
        schema="bronze",
    )
    op.create_index(
        "ix_raw_articles_ingest_date", "raw_articles", ["ingest_date"], schema="bronze"
    )
    op.create_index(
        "ix_raw_articles_source_id_ingest_date",
        "raw_articles",
        ["source_id", "ingest_date"],
        schema="bronze",
    )

    # ---------- silver.articles ----------
    op.create_table(
        "articles",
        sa.Column("article_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=True),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("snippet", sa.Text(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("first_seen_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("first_seen_date", sa.Date(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("filter_score", sa.Numeric(5, 4), nullable=True),
        sa.Column("exclusion_reason", sa.Text(), nullable=True),
        sa.Column("industry_tags", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("last_published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.UniqueConstraint("canonical_url", name="uq_articles_canonical_url"),
        sa.CheckConstraint(
            "status IN ('ingested', 'eligible', 'excluded', 'scored', 'quarantined')",
            name="ck_articles_status",
        ),
        schema="silver",
    )
    op.create_index("ix_articles_content_hash", "articles", ["content_hash"], schema="silver")
    op.create_index(
        "ix_articles_status_first_seen_at",
        "articles",
        ["status", "first_seen_at"],
        schema="silver",
    )
    op.create_index(
        "ix_articles_first_seen_date", "articles", ["first_seen_date"], schema="silver"
    )

    # ---------- silver.article_scores ----------
    op.create_table(
        "article_scores",
        sa.Column("score_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "article_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("silver.articles.article_id", name="fk_article_scores_article_id"),
            nullable=False,
        ),
        sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("credibility", sa.SmallInteger(), nullable=False),
        sa.Column("importance", sa.SmallInteger(), nullable=False),
        sa.Column("depth", sa.SmallInteger(), nullable=False),
        sa.Column("practicality", sa.SmallInteger(), nullable=False),
        sa.Column("confidence", sa.Text(), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=False),
        sa.Column("output_tokens", sa.Integer(), nullable=False),
        sa.Column("cost_usd", sa.Numeric(10, 6), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("scored_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "article_id",
            "prompt_version",
            "model_name",
            name="uq_article_scores_article_prompt_model",
        ),
        sa.CheckConstraint(
            "credibility BETWEEN 1 AND 10", name="ck_article_scores_credibility_range"
        ),
        sa.CheckConstraint(
            "importance BETWEEN 1 AND 10", name="ck_article_scores_importance_range"
        ),
        sa.CheckConstraint("depth BETWEEN 1 AND 10", name="ck_article_scores_depth_range"),
        sa.CheckConstraint(
            "practicality BETWEEN 1 AND 10", name="ck_article_scores_practicality_range"
        ),
        schema="silver",
    )

    # ---------- silver.article_summaries ----------
    op.create_table(
        "article_summaries",
        sa.Column("summary_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "article_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("silver.articles.article_id", name="fk_article_summaries_article_id"),
            nullable=False,
        ),
        sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("summary_vi", postgresql.JSONB(), nullable=False),
        sa.Column("why_it_matters_vi", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        schema="silver",
    )
    op.create_index(
        "ix_article_summaries_article_id", "article_summaries", ["article_id"], schema="silver"
    )

    # ---------- silver.score_quarantine ----------
    op.create_table(
        "score_quarantine",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "article_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("silver.articles.article_id", name="fk_score_quarantine_article_id"),
            nullable=False,
        ),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("raw_response", postgresql.JSONB(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint(
            "failure_reason IN ('json_parse_error', 'schema_violation', 'out_of_range', 'timeout')",
            name="ck_score_quarantine_failure_reason",
        ),
        schema="silver",
    )

    # ---------- silver.source_health ----------
    op.create_table(
        "source_health",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("fetch_date", sa.Date(), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("entry_count", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.Text(), nullable=True),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "source_id", "fetch_date", name="uq_source_health_source_id_fetch_date"
        ),
        schema="silver",
    )
    op.create_index(
        "ix_source_health_fetch_date", "source_health", ["fetch_date"], schema="silver"
    )


def downgrade() -> None:
    op.drop_table("source_health", schema="silver")
    op.drop_table("score_quarantine", schema="silver")
    op.drop_index("ix_article_summaries_article_id", table_name="article_summaries", schema="silver")
    op.drop_table("article_summaries", schema="silver")
    op.drop_table("article_scores", schema="silver")
    op.drop_index("ix_articles_first_seen_date", table_name="articles", schema="silver")
    op.drop_index("ix_articles_status_first_seen_at", table_name="articles", schema="silver")
    op.drop_index("ix_articles_content_hash", table_name="articles", schema="silver")
    op.drop_table("articles", schema="silver")
    op.drop_index(
        "ix_raw_articles_source_id_ingest_date", table_name="raw_articles", schema="bronze"
    )
    op.drop_index("ix_raw_articles_ingest_date", table_name="raw_articles", schema="bronze")
    op.drop_table("raw_articles", schema="bronze")
