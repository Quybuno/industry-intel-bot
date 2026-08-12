{{ config(materialized="ephemeral") }}

-- Lấy bản tóm tắt mới nhất theo article_id — cùng mẫu với int_scores_latest (§11.2), nhưng
-- gộp theo article_id thay vì (article_id, prompt_version): một bài chỉ cần MỘT tóm tắt
-- hiển thị trên digest, không phân biệt prompt_version/model_name nào sinh ra nó. Một
-- article_id có thể có nhiều summary (viết lại bằng model mạnh hơn, §5.4) — giữ bản
-- created_at gần nhất.
--
-- Loại provider test/CI (var('non_production_model_names'), vd. 'mock') TRƯỚC khi xếp hạng
-- mới nhất — MockProvider sinh text placeholder cố định ("Gạch đầu dòng giả lập số N...",
-- score/providers/mock.py) để test đường contract/DB, không phải tóm tắt thật. Phát hiện
-- được khi publish thật (task 0.11) hiển thị nguyên văn placeholder này lên digest công
-- khai — một bài chỉ có tóm tắt từ provider test coi như CHƯA có tóm tắt thật, không vào
-- mart_daily_digest (INNER JOIN, xem mart_daily_digest.sql).
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
    where {{ is_production_model('stg_article_summaries.model_name') }}
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
