"""Fixture dùng chung cho test — kết nối Postgres thật (không mock DB, theo PRODUCTION_PLAN §20.2)."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def db_engine() -> Iterator[sa.Engine]:
    """Engine Postgres thật, đọc DATABASE_URL từ môi trường. Skip nếu chưa có."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip(
            "Thiếu DATABASE_URL — cần Postgres thật (docker compose up -d postgres) để chạy test này."
        )
    engine = sa.create_engine(database_url, future=True)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_connection(db_engine: sa.Engine) -> Iterator[sa.Connection]:
    """Một connection riêng cho mỗi test."""
    with db_engine.connect() as connection:
        yield connection
