-- §13.2: mọi summary_vi có đúng 5 phần tử. Để lại ở Phase 1 lúc task 0.10 vì chưa có
-- model summary — nay có stg_article_summaries (task 0.11) nên bổ sung. Test PASS khi
-- query trả về 0 dòng.
select
    article_id,
    jsonb_array_length(summary_vi) as bullet_count
from {{ ref('stg_article_summaries') }}
where jsonb_array_length(summary_vi) != 5
