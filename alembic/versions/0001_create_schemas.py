"""Tạo 3 schema Medallion: bronze, silver, gold.

gold để trống ở bước này — các bảng trong gold do dbt sinh ra (task 0.10),
schema chỉ tạo trước để dbt có nơi ghi vào.

Revision ID: 0001_create_schemas
Revises:
Create Date: 2026-08-10

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0001_create_schemas"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    op.execute("CREATE SCHEMA IF NOT EXISTS silver")
    op.execute("CREATE SCHEMA IF NOT EXISTS gold")


def downgrade() -> None:
    op.execute("DROP SCHEMA IF EXISTS gold CASCADE")
    op.execute("DROP SCHEMA IF EXISTS silver CASCADE")
    op.execute("DROP SCHEMA IF EXISTS bronze CASCADE")
