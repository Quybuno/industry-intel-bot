{{ config(materialized="view") }}

-- Staging (PRODUCTION_PLAN §11.2): CHỈ đổi tên cột và ép kiểu, KHÔNG business logic.
select
    score_id::uuid as score_id,
    article_id::uuid as article_id,
    model_name::text as model_name,
    prompt_version::text as prompt_version,
    credibility::smallint as credibility,
    importance::smallint as importance,
    -- "depth" là tên cột nghiệp vụ thật (§5.4), không đổi được dù trùng từ khoá SQL.
    depth::smallint as depth, -- noqa: RF04
    practicality::smallint as practicality,
    confidence::text as confidence,
    input_tokens::integer as input_tokens,
    output_tokens::integer as output_tokens,
    cost_usd::numeric(10, 6) as cost_usd,
    latency_ms::integer as latency_ms,
    scored_at::timestamptz as scored_at
from {{ source('silver', 'article_scores') }}
