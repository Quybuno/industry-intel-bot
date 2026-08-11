{{ config(materialized="ephemeral") }}

-- Lấy bản tóm tắt mới nhất theo article_id — cùng mẫu với int_scores_latest (§11.2), nhưng
-- gộp theo article_id thay vì (article_id, prompt_version): một bài chỉ cần MỘT tóm tắt
-- hiển thị trên digest, không phân biệt prompt_version/model_name nào sinh ra nó. Một
-- article_id có thể có nhiều summary (viết lại bằng model mạnh hơn, §5.4) — giữ bản
-- created_at gần nhất.
with ranked_summaries as (
    select
        stg_article_summaries.summary_id,
        stg_article_summaries.article_id,
        stg_article_summaries.model_name,
        stg_article_summaries.prompt_version,
        stg_article_summaries.summary_vi,
        stg_article_summaries.why_it_matters_vi,
        stg_article_summaries.created_at,
        row_number() over (
            partition by stg_article_summaries.article_id
            order by stg_article_summaries.created_at desc
        ) as recency_rank
    from {{ ref('stg_article_summaries') }} as stg_article_summaries
)

select
    summary_id,
    article_id,
    model_name,
    prompt_version,
    summary_vi,
    why_it_matters_vi,
    created_at
from ranked_summaries
where recency_rank = 1
