"""Alembic env — không dùng ORM. Mọi migration viết bằng Alembic op / SQLAlchemy Core,
nên không có metadata tự động để autogenerate (target_metadata = None, luôn viết tay).
"""

import os
from logging.config import fileConfig

from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

from alembic import context

# Nạp .env ở thư mục gốc repo (nếu có) trước khi đọc DATABASE_URL.
load_dotenv()

config = context.config
fileConfig(config.config_file_name)

# Không dùng ORM: không autogenerate, mọi bảng viết tay trong từng migration.
target_metadata = None


def get_database_url() -> str:
    """Đọc DATABASE_URL bắt buộc từ môi trường — không fallback về SQLite hay giá trị đoán."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Thiếu biến môi trường DATABASE_URL. Đặt trong .env, ví dụ:\n"
            "  DATABASE_URL=postgresql+psycopg://intel:intel@localhost:5432/intel_bot"
        )
    return url


def run_migrations_offline() -> None:
    """Sinh SQL mà không cần kết nối DB thật (`alembic upgrade head --sql`)."""
    context.configure(
        url=get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Kết nối Postgres thật và chạy migration."""
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_database_url()
    connectable = engine_from_config(configuration, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
