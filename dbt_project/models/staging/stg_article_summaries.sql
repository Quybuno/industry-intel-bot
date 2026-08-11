{{ config(materialized="view") }}

-- Staging (PRODUCTION_PLAN §11.2): CHỈ đổi tên cột và ép kiểu, KHÔNG business logic. Bổ
-- sung ở task 0.11 — 0.10 chỉ làm stg_articles/stg_article_scores; mart_daily_digest cần
-- summary_vi/why_it_matters_vi để publish có nội dung thật (§12.4), nên thêm staging cho
-- silver.article_summaries trước khi join ở int_summaries_latest.
select
    summary_id::uuid as summary_id,
    article_id::uuid as article_id,
    model_name::text as model_name,
    prompt_version::text as prompt_version,
    summary_vi::jsonb as summary_vi,
    why_it_matters_vi::text as why_it_matters_vi,
    created_at::timestamptz as created_at
from {{ source('silver', 'article_summaries') }}
