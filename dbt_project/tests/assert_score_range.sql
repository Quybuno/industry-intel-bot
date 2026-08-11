-- §13.2: mọi điểm (credibility/importance/depth/practicality) nằm trong 1-10.
-- Test PASS khi query trả về 0 dòng.
select
    score_id,
    'credibility' as failing_column,
    credibility as score_value
from {{ ref('stg_article_scores') }}
where credibility not between 1 and 10

union all

select
    score_id,
    'importance' as failing_column,
    importance as score_value
from {{ ref('stg_article_scores') }}
where importance not between 1 and 10

union all

select
    score_id,
    'depth' as failing_column,
    depth as score_value
from {{ ref('stg_article_scores') }}
where depth not between 1 and 10

union all

select
    score_id,
    'practicality' as failing_column,
    practicality as score_value
from {{ ref('stg_article_scores') }}
where practicality not between 1 and 10
