"""Resource Postgres dùng chung cho mọi asset (task 0.12 mục 4).

Asset KHÔNG tự tạo connection — nhận `PostgresResource` qua tham số, gọi
`with postgres.get_connection() as connection:` rồi truyền `connection` thẳng vào các hàm
đã có sẵn ở `src/intel_bot/...` (cùng chữ ký `connection: sa.Connection` mà CLI đã dùng).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

import sqlalchemy as sa
from dagster import ConfigurableResource

from src.intel_bot.db.health import get_database_url


class PostgresResource(ConfigurableResource[Any]):
    """Bọc một SQLAlchemy engine. `database_url` rỗng (mặc định) → đọc `DATABASE_URL` qua
    `get_database_url()` (đúng hàm CLI `doctor`/mọi lệnh khác đã dùng, lỗi rõ ràng nếu
    thiếu — P4); truyền `database_url` khác rỗng để trỏ DB test.

    `ConfigurableResource[Any]`: tham số generic của dagster là kiểu resource TRẢ VỀ khi
    dùng qua factory — với resource tự thân (như ở đây) không có kiểu cụ thể nào để khai
    ngoài `Any` (giới hạn của SDK, không có `Self` type hợp lệ ở vị trí này)."""

    database_url: str = ""

    @contextlib.contextmanager
    def get_connection(self) -> Iterator[sa.Connection]:
        """Mở một connection mới, tự đóng engine khi xong — giống hệt cách `cli.py` làm
        cho từng lệnh (`sa.create_engine(...).connect()` trong `try/finally`)."""
        url = self.database_url or get_database_url()
        engine = sa.create_engine(url, future=True)
        try:
            with engine.connect() as connection:
                yield connection
        finally:
            engine.dispose()
