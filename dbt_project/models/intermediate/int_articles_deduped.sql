{{ config(materialized="ephemeral") }}

-- Dedup cấp 2 (PRODUCTION_PLAN §8.4, §11.2): giữ bản ghi có composite_score cao nhất trong
-- mỗi nhóm content_hash.
--
-- Composite được TÍNH Ở ĐÂY (không phải ở fct_article_score) vì việc chọn "người thắng" của
-- dedup cấp 2 phụ thuộc trực tiếp vào composite_score — không thể dedup trước rồi mới tính
-- điểm sau. Vì vậy model này (intermediate) join tới dim_source (marts) để lấy source_tier:
-- dim_source không phụ thuộc gì vào articles/scores (sinh từ snapshot nguồn), nên tham
-- chiếu ngược thư mục này không tạo vòng lặp trong DAG — chỉ khác với cách sắp xếp thư mục
-- gợi ý ở PRODUCTION_PLAN §6 (đó là quy ước tổ chức file, không phải ràng buộc thứ tự build
-- của dbt, vốn luôn suy ra đúng từ `ref()`).
--
-- fct_article_score chỉ việc SELECT lại kết quả đã dedup + đã tính composite ở đây và
-- persist theo chiến lược incremental — không tính lại công thức lần hai (một nguồn sự thật
-- duy nhất, đặt trong macros/scoring.sql).

{% if execute %}
    {% set dup_count_query %}
        select count(*) as duplicate_groups
        from (
            select content_hash
            from {{ ref('stg_articles') }}
            where content_hash is not null
            group by content_hash
            having count(*) > 1
        ) as grouped_duplicates
    {% endset %}
    {% set dup_results = run_query(dup_count_query) %}
    {% if dup_results %}
        {% set dup_groups = dup_results.columns[0].values()[0] %}
        {{ log(
            "int_articles_deduped: " ~ dup_groups
            ~ " nhóm content_hash có bản ghi trùng, mỗi nhóm giữ lại 1 bản có composite_score cao nhất",
            info=True
        ) }}
    {% endif %}
{% endif %}

with scored_articles as (
    select
        articles.article_id,
        articles.canonical_url,
        articles.content_hash,
        articles.source_id,
        articles.title,
        articles.snippet,
        articles.published_at,
        articles.published_at_imputed,
        articles.first_seen_at,
        articles.first_seen_date,
        articles.status,
        articles.industry_tags,
        scores.score_id,
        scores.model_name,
        scores.prompt_version,
        scores.credibility as llm_credibility,
        scores.importance,
        scores.depth,
        scores.practicality,
        scores.confidence,
        scores.scored_at
    from {{ ref('stg_articles') }} as articles
    inner join {{ ref('int_scores_latest') }} as scores
        on articles.article_id = scores.article_id
),

with_credibility as (
    select
        scored_articles.*,
        src.tier as source_tier,
        {{ source_tier_score('src.tier') }} as source_tier_score
    from scored_articles
    left join {{ ref('dim_source') }} as src
        on
            scored_articles.source_id = src.source_id
            and src.is_current
),

with_composite as (
    select
        with_credibility.*,
        {{ credibility_blended('source_tier_score', 'llm_credibility') }}
            as credibility_blended,
        {{ recency_boost('published_at', 'first_seen_at', 'published_at_imputed') }}
            as recency_boost
    from with_credibility
),

-- Tính composite_score MỘT LẦN ở CTE riêng để "ranked" phía dưới chỉ cần tham chiếu tên
-- cột (không gọi lại macro composite_score() lần thứ hai cho phần ORDER BY của window
-- function — Postgres không cho tham chiếu alias cùng cấp SELECT trong window function).
scored_composite as (
    select
        with_composite.*,
        {{
            composite_score(
                'importance', 'practicality', 'credibility_blended', 'depth', 'recency_boost'
            )
        }} as composite_score
    from with_composite
),

ranked as (
    select
        scored_composite.*,
        count(*) over (
            partition by coalesce(scored_composite.content_hash, scored_composite.article_id::text)
        ) as content_hash_group_size,
        row_number() over (
            partition by coalesce(scored_composite.content_hash, scored_composite.article_id::text)
            order by
                scored_composite.composite_score desc,
                scored_composite.first_seen_at asc,
                scored_composite.article_id asc
        ) as dedup_rank
    from scored_composite
)

select
    article_id,
    canonical_url,
    content_hash,
    source_id,
    title,
    snippet,
    published_at,
    published_at_imputed,
    first_seen_at,
    first_seen_date,
    status,
    industry_tags,
    score_id,
    model_name,
    prompt_version,
    llm_credibility,
    importance,
    depth,
    practicality,
    confidence,
    scored_at,
    source_tier,
    source_tier_score,
    credibility_blended,
    recency_boost,
    composite_score,
    content_hash_group_size
from ranked
where dedup_rank = 1
