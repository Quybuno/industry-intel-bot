{{ config(materialized="table") }}

-- Digest công khai (PRODUCTION_PLAN §5.8, §12.1): publish job (task 0.11) chỉ SELECT * FROM
-- bảng này — toàn bộ dedup/xếp hạng/nhóm ngành/lọc-bài-đã-có-tóm-tắt đã xong ở dbt, không
-- còn business logic ở Python (§12.1). Một dòng một bài (fct_article_score đã dedup cấp 2),
-- cửa sổ theo first_seen_at, sort theo composite_score giảm dần.
--
-- INNER JOIN summary (không LEFT): card công khai bắt buộc có 5 bullet + "tại sao quan
-- trọng" (§12.4) — một bài chưa có tóm tắt tiếng Việt (chưa vào top-K ở §4.4) chưa sẵn sàng
-- publish. Đây là quy tắc "bài nào được xuất bản", cùng nhóm quyết định với dedup/ranking,
-- nên thuộc dbt chứ không phải một điều kiện lọc thêm ở Python.
select
    fct.score_id,
    fct.article_id,
    articles.canonical_url,
    articles.title,
    articles.snippet,
    articles.industry_tags,
    fct.source_id,
    src.domain as source_domain,
    fct.source_tier,
    fct.published_at,
    fct.published_at_imputed,
    fct.first_seen_at,
    fct.credibility_blended,
    fct.importance,
    fct.practicality,
    fct.depth,
    fct.recency_boost,
    fct.composite_score,
    summaries.summary_vi,
    summaries.why_it_matters_vi,
    -- "một dòng một bài" (§5.8) loại trừ việc explode theo từng industry_tag (sẽ ra nhiều
    -- dòng cho bài đa ngành). Lấy tag đầu tiên làm nhóm ngành chính để section hoá trang
    -- digest (§12.4); bài không có tag nào rơi vào 'uncategorized'.
    coalesce(articles.industry_tags[1], 'uncategorized') as industry_group
from {{ ref('fct_article_score') }} as fct
inner join {{ ref('stg_articles') }} as articles
    on fct.article_id = articles.article_id
inner join {{ ref('int_summaries_latest') }} as summaries
    on fct.article_id = summaries.article_id
left join {{ ref('dim_source') }} as src
    on
        fct.source_id = src.source_id
        and src.is_current
where
    fct.first_seen_at
    >= current_timestamp - interval '{{ var("digest_window_hours") }} hours'
order by fct.composite_score desc
