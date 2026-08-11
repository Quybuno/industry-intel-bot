{{ config(materialized="table") }}

-- SCD2 nguồn tin (PRODUCTION_PLAN §5.6): sinh trực tiếp từ snapshot snap_sources, chỉ đổi
-- tên cột chuẩn của dbt snapshot (dbt_valid_from/to, dbt_scd_id) sang tên nghiệp vụ ở §5.6.
select
    dbt_scd_id::text as source_key,
    source_id,
    domain,
    tier,
    industries,
    is_enabled,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('snap_sources') }}
