{{ config(materialized="ephemeral") }}

-- Lấy bản ghi điểm mới nhất theo (article_id, prompt_version) — PRODUCTION_PLAN §11.2.
-- Một (article_id, prompt_version) có thể có nhiều model_name theo thời gian (chấm lại bằng
-- model khác, §5.4) — giữ bản scored_at gần nhất, không dùng để tính trung bình hay gộp.
with ranked_scores as (
    select
        stg_article_scores.score_id,
        stg_article_scores.article_id,
        stg_article_scores.model_name,
        stg_article_scores.prompt_version,
        stg_article_scores.credibility,
        stg_article_scores.importance,
        stg_article_scores.depth,
        stg_article_scores.practicality,
        stg_article_scores.confidence,
        stg_article_scores.input_tokens,
        stg_article_scores.output_tokens,
        stg_article_scores.cost_usd,
        stg_article_scores.latency_ms,
        stg_article_scores.scored_at,
        row_number() over (
            partition by
                stg_article_scores.article_id, stg_article_scores.prompt_version
            order by stg_article_scores.scored_at desc
        ) as recency_rank
    from {{ ref('stg_article_scores') }} as stg_article_scores
)

select
    score_id,
    article_id,
    model_name,
    prompt_version,
    credibility,
    importance,
    depth,
    practicality,
    confidence,
    input_tokens,
    output_tokens,
    cost_usd,
    latency_ms,
    scored_at
from ranked_scores
where recency_rank = 1
