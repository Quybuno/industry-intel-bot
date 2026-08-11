-- §13.2: mọi score trỏ tới article tồn tại. Test PASS khi query trả về 0 dòng.
select
    scores.score_id,
    scores.article_id
from {{ ref('stg_article_scores') }} as scores
left join {{ ref('stg_articles') }} as articles
    on scores.article_id = articles.article_id
where articles.article_id is null
