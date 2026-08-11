"""Bổ sung UNIQUE (article_id, prompt_version, model_name) cho silver.article_summaries.

Bảng gốc (migration 0002) chưa có ràng buộc này — chỉ có PK summary_id + index thường
trên article_id. Task 0.8 cần ghi tóm tắt idempotent giống hệt article_scores (INSERT ...
ON CONFLICT DO NOTHING, P1: "chạy lại không sinh dòng trùng") nên bổ sung ở đây thay vì
sửa migration 0002 đã có (theo AGENTS.md).

Revision ID: 0004_summaries_unique
Revises: 0003_articles_debug_cols
Create Date: 2026-08-12

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0004_summaries_unique"
down_revision: str | None = "0003_articles_debug_cols"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_article_summaries_article_prompt_model",
        "article_summaries",
        ["article_id", "prompt_version", "model_name"],
        schema="silver",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_article_summaries_article_prompt_model",
        "article_summaries",
        schema="silver",
        type_="unique",
    )
