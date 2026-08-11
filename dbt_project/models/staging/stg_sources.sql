{{ config(materialized="view") }}

-- Staging cho seed_sources (task 0.10 mục 5): ép kiểu + tách industries thành mảng.
-- seed_sources.csv nạp nguyên văn config/sources.yaml qua `dbt seed` — cột industries lưu
-- dạng chuỗi phân tách bởi "|" (CSV không biểu diễn mảng trực tiếp, và "|" tránh xung đột
-- với dấu phẩy phân tách cột CSV). Bản thân model này KHÔNG có business logic, chỉ ép kiểu
-- — dedup/SCD2 nằm ở snapshot + dim_source.
select
    source_id::text as source_id,
    -- "domain" là tên cột nghiệp vụ thật, không đổi được dù trùng từ khoá SQL.
    domain::text as domain, -- noqa: RF04
    tier::smallint as tier,
    is_enabled::boolean as is_enabled,
    string_to_array(industries, '|')::text[] as industries
from {{ ref('seed_sources') }}
