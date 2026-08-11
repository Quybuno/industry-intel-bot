-- §13.2: không có published_at > now() + 1h. Test PASS khi query trả về 0 dòng.
select
    article_id,
    published_at
from {{ ref('stg_articles') }}
where published_at > current_timestamp + interval '1 hour'
