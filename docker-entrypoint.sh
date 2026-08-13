#!/bin/sh
# Entrypoint chung cho dagster-daemon + dagster-webserver (task 1.10, §16.3).
#
# `dbt parse` PHẢI chạy trước khi dagster-daemon/dagster-webserver import
# dagster_project/definitions.py: dagster_project/assets/dbt_assets.py truyền
# `manifest=dbt_project.manifest_path` (= dbt_project/target/manifest.json) ngay lúc module
# được import (decorator-time) — file này KHÔNG nằm trong image (`.dockerignore` loại
# dbt_project/target/, đúng — đó là artifact sinh ra khi chạy, không phải source) nên container
# mới khởi động luôn thiếu nó. Đây CHÍNH XÁC là lỗi thật đã gặp và sửa ở CI (task 1.9,
# .github/workflows/ci.yml) — cùng nguyên nhân, khác môi trường (container thay vì CI runner).
#
# CHỈ cần `dbt parse` (không cần seed/build như CI) — daemon/webserver chỉ IMPORT manifest,
# không tự chạy `dbt compile`/`dbt build` lúc khởi động (những lệnh đó chỉ chạy khi một
# dbt asset THẬT SỰ được materialize, tức lúc pipeline chạy, không phải lúc tiến trình khởi
# động) — không có phụ thuộc "bảng gold phải tồn tại trước" như bước seeding của CI.
set -e

dbt parse --project-dir dbt_project --profiles-dir dbt_project

exec "$@"
