# Image dùng chung cho dagster-daemon + dagster-webserver (task 1.10, §16.3 "dagster | build
# local"). Cùng MỘT image, khác nhau ở `command:` override trong docker-compose.yml — daemon
# và webserver dùng chung code/dependency, không cần build 2 image riêng (P8).
FROM python:3.12-slim

# git: cần cho src/intel_bot/publish/git_publish.py (commit+push docs-site/ lên gh-pages,
# task 1.10 §12.1) chạy TỪ BÊN TRONG container — không phải phụ thuộc build-time.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Danh tính git cho các commit tự động do daemon tạo (publish job) — không phải người thật,
# đặt cố định thay vì đọc biến môi trường vì đây KHÔNG phải secret, chỉ là tên hiển thị trên
# commit log (không vi phạm "no hardcoded secrets", đây là danh tính bot, tương tự cách nhiều
# CI hệ thống đặt "github-actions[bot]").
RUN git config --system user.email "industry-intel-bot@users.noreply.github.com" \
    && git config --system user.name "industry-intel-bot" \
    && git config --system --add safe.directory /app

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy trước để tận dụng cache layer Docker — dependency đổi ít hơn code. README.md phải
# có mặt ở bước này (không chỉ ở `COPY . .` bên dưới) vì `pyproject.toml` khai `readme =
# "README.md"` — hatchling đọc file này lúc resolve package, thiếu thì `uv sync` vỡ ngay
# (gặp thật khi build image lần đầu, không phải suy đoán).
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

COPY . .
RUN chmod +x docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONUTF8=1
ENV PYTHONIOENCODING=utf-8

# `docker-entrypoint.sh` chạy `dbt parse` (sinh manifest.json cho dagster_dbt, xem chú thích
# trong file đó) trước khi giao lại cho `command:` thật của từng service — KHÔNG có CMD mặc
# định ở đây, docker-compose.yml override `command:` riêng cho dagster-daemon/dagster-webserver.
ENTRYPOINT ["./docker-entrypoint.sh"]
