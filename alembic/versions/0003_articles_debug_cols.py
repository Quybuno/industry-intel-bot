"""Bổ sung silver.articles: raw_url (debug dedup) + published_at_imputed (cold start, §8.2).

Không sửa migration 0002 đã có (theo AGENTS.md) — thêm cột bằng migration mới.

Revision ID: 0003_articles_debug_cols
Revises: 0002_bronze_silver_tables
Create Date: 2026-08-11

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0003_articles_debug_cols"
down_revision: str | None = "0002_bronze_silver_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # raw_url: URL gốc trước khi canonicalize — để debug khi dedup cấp 1 gộp sai (task 0.5 mục 3).
    # Nullable: ALTER TABLE ADD COLUMN NOT NULL không có default sẽ lỗi ngay khi bảng đã có
    # dữ liệu (không còn rỗng sau lần normalize đầu) — loader.py luôn cung cấp giá trị cho
    # dòng mới, nên cột chỉ NULL với các dòng ghi trước khi cột này tồn tại (không có).
    op.add_column(
        "articles",
        sa.Column("raw_url", sa.Text(), nullable=True),
        schema="silver",
    )
    # published_at_imputed: cờ đánh dấu published_at NULL nên first_seen_at được dùng làm mốc
    # thay thế ở tầng gold (PRODUCTION_PLAN §8.2, §5.7) — bản thân silver không ghi đè published_at.
    op.add_column(
        "articles",
        sa.Column(
            "published_at_imputed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        schema="silver",
    )


def downgrade() -> None:
    op.drop_column("articles", "published_at_imputed", schema="silver")
    op.drop_column("articles", "raw_url", schema="silver")
