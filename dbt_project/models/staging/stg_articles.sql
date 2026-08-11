{{ config(materialized="view") }}

-- Staging (PRODUCTION_PLAN §11.2): CHỈ đổi tên cột và ép kiểu, KHÔNG business logic.
select
    article_id::uuid as article_id,
    canonical_url::text as canonical_url,
    content_hash::text as content_hash,
    source_id::text as source_id,
    title::text as title,
    snippet::text as snippet,
    raw_url::text as raw_url,
    published_at::timestamptz as published_at,
    published_at_imputed::boolean as published_at_imputed,
    first_seen_at::timestamptz as first_seen_at,
    first_seen_date::date as first_seen_date,
    status::text as status,
    filter_score::numeric(5, 4) as filter_score,
    exclusion_reason::text as exclusion_reason,
    industry_tags::text[] as industry_tags,
    last_published_at::timestamptz as last_published_at
from {{ source('silver', 'articles') }}
