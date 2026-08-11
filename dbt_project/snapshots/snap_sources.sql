{% snapshot snap_sources %}

{{
    config(
        target_schema="gold",
        unique_key="source_id",
        strategy="check",
        check_cols=["tier", "is_enabled", "industries"],
    )
}}

-- SCD2 nguồn tin (PRODUCTION_PLAN §5.6, §11.4). Nguồn: stg_sources (ép kiểu từ
-- seed_sources.csv, nạp config/sources.yaml qua `dbt seed`, task 0.10 mục 5). Khi tier /
-- is_enabled / industries đổi, dbt tự đóng bản ghi cũ (dbt_valid_to) và mở bản ghi mới.
    select
        source_id,
        domain,
        tier,
        industries,
        is_enabled
    from {{ ref('stg_sources') }}

{% endsnapshot %}
