"""Kiểm tra sức khoẻ kết nối Postgres — dùng cho lệnh CLI `intel-bot doctor`.

Chỉ dùng SQLAlchemy Core (không ORM), theo đúng quy ước truy cập DB trong AGENTS.md.
Các hàm I/O nhận connection qua tham số, không tự tạo bên trong, để test được.
"""

from __future__ import annotations

import os

import sqlalchemy as sa

#: Ba schema Medallion mà doctor phải kiểm tra. gold có thể rỗng nếu dbt chưa chạy (task 0.10).
MEDALLION_SCHEMAS: tuple[str, ...] = ("bronze", "silver", "gold")


def get_database_url() -> str:
    """Đọc `DATABASE_URL` bắt buộc từ biến môi trường.

    Dừng rõ ràng nếu thiếu thay vì đoán hoặc fallback về SQLite — DB thật của dự án
    là Postgres theo ADR-015.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Thiếu biến môi trường DATABASE_URL. Đặt trong .env, ví dụ:\n"
            "  DATABASE_URL=postgresql+psycopg://intel:intel@localhost:5432/intel_bot"
        )
    return url


def check_connection(connection: sa.Connection) -> bool:
    """Kiểm tra kết nối DB còn sống bằng `SELECT 1`."""
    return connection.execute(sa.text("SELECT 1")).scalar_one() == 1


def list_tables_by_schema(
    connection: sa.Connection, schemas: tuple[str, ...] = MEDALLION_SCHEMAS
) -> dict[str, list[str]]:
    """Liệt kê tên bảng đã tồn tại theo từng schema, đọc từ `information_schema.tables`."""
    tables_by_schema: dict[str, list[str]] = {schema: [] for schema in schemas}
    rows = connection.execute(
        sa.text(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(:schemas) AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        ),
        {"schemas": list(schemas)},
    ).all()
    for schema_name, table_name in rows:
        tables_by_schema[schema_name].append(table_name)
    return tables_by_schema
