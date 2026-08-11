{{
    config(
        materialized="incremental",
        unique_key="score_id",
        incremental_strategy="merge",
    )
}}

-- Bảng fact chính (PRODUCTION_PLAN §5.7, §11.3). Composite đã tính xong ở
-- int_articles_deduped (dedup cấp 2 cần composite để chọn người thắng — xem docstring model
-- đó); ở đây chỉ SELECT lại và persist theo chiến lược incremental merge trên score_id.
--
-- Cửa sổ lookback mặc định 3 ngày (var fct_article_score_lookback_days) để bắt bài đến muộn
-- mà không quét lại toàn bảng. `--vars '{run_date: YYYY-MM-DD}'` ghi đè lookback bằng đúng
-- một ngày cụ thể, hỗ trợ chạy lại một partition (§11.3).
select
    score_id,
    article_id,
    source_id,
    model_name,
    prompt_version,
    first_seen_date,
    first_seen_at,
    published_at,
    published_at_imputed,
    llm_credibility,
    importance,
    depth,
    practicality,
    confidence,
    source_tier,
    source_tier_score,
    credibility_blended,
    recency_boost,
    composite_score,
    content_hash_group_size,
    current_timestamp as computed_at
from {{ ref('int_articles_deduped') }}

{% if is_incremental() %}
    {% if var('run_date', none) is not none %}
        where first_seen_date = '{{ var("run_date") }}'::date
    {% else %}
        where first_seen_date >= (
            select
                max(prior_run.first_seen_date)
                - {{ var('fct_article_score_lookback_days') }}
            from {{ this }} as prior_run
        )
    {% endif %}
{% endif %}
