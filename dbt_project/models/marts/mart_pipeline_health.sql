{{
    config(
        materialized="incremental",
        unique_key="pipeline_date",
        incremental_strategy="merge",
    )
}}

-- Metrics vận hành (PRODUCTION_PLAN §5.8, §13.3, §11.3): một dòng một ngày. `pipeline_date`
-- là một trục lịch chung — không phải riêng ingest_date — vì §7.3 nói rõ bài ingest chiều
-- hôm trước có thể được chấm điểm sáng hôm sau (scored_at khác ngày với ingest_date của
-- bài). Vì vậy union tất cả các cột ngày liên quan (ingest_date, first_seen_date, scored_at,
-- quarantine created_at, source_health fetch_date) thành một spine rồi LEFT JOIN từng
-- nhóm metric vào — mỗi ngày phản ánh đúng hoạt động xảy ra trong ngày đó, bất kể hoạt
-- động ấy thuộc về bài được ingest ngày nào.
--
-- `--vars '{run_date: YYYY-MM-DD}'` (thêm ở task 0.12 để asset Dagster
-- `mart_pipeline_health` materialize được một partition cụ thể — cùng mẫu
-- fct_article_score.sql) ghi đè lookback bằng đúng một ngày. Không có run_date thì dùng
-- lookback mặc định như cũ.
with dates as (
    select ingest_date as pipeline_date
    from {{ source('bronze', 'raw_articles') }}
    union
    select first_seen_date as pipeline_date from {{ ref('stg_articles') }}
    union
    select scored_at::date as pipeline_date from {{ ref('stg_article_scores') }}
    union
    select created_at::date as pipeline_date
    from {{ source('silver', 'score_quarantine') }}
    union
    select fetch_date as pipeline_date
    from {{ source('silver', 'source_health') }}
),

ingest_daily as (
    select
        ingest_date as pipeline_date,
        count(*) as ingest_count
    from {{ source('bronze', 'raw_articles') }}
    group by 1
),

status_daily as (
    select
        first_seen_date as pipeline_date,
        count(*) filter (
            where status in ('eligible', 'scored', 'quarantined')
        ) as eligible_count,
        count(*) filter (where status = 'excluded') as excluded_count
    from {{ ref('stg_articles') }}
    group by 1
),

quarantine_daily as (
    select
        created_at::date as pipeline_date,
        count(*) as quarantine_count
    from {{ source('silver', 'score_quarantine') }}
    group by 1
),

cost_latency_daily as (
    select
        scored_at::date as pipeline_date,
        sum(cost_usd) as total_cost_usd,
        percentile_cont(0.5) within group (order by latency_ms)
            as latency_p50_ms,
        percentile_cont(0.95) within group (order by latency_ms)
            as latency_p95_ms
    from {{ ref('stg_article_scores') }}
    group by 1
),

source_health_daily as (
    -- "Lỗi" = có error_message, hoặc http_status là mã lỗi thật (4xx/5xx). 304 Not Modified
    -- là phản hồi THÀNH CÔNG của conditional GET (§8.1: ETag/Last-Modified) — không phải
    -- lỗi, dù nằm ngoài khoảng 200-299. http_status NULL không kèm error_message coi là
    -- chưa xác định lỗi (không đủ căn cứ để đếm là fail).
    select
        fetch_date as pipeline_date,
        count(distinct source_id) filter (
            where error_message is not null
            or (http_status is not null and http_status >= 400)
        ) as source_fail_count
    from {{ source('silver', 'source_health') }}
    group by 1
)

select
    dates.pipeline_date,
    cost_latency_daily.latency_p50_ms,
    cost_latency_daily.latency_p95_ms,
    coalesce(ingest_daily.ingest_count, 0) as ingest_count,
    coalesce(status_daily.eligible_count, 0) as eligible_count,
    coalesce(status_daily.excluded_count, 0) as excluded_count,
    case
        when coalesce(ingest_daily.ingest_count, 0) = 0 then 0
        else status_daily.excluded_count::numeric / ingest_daily.ingest_count
    end as excluded_ratio,
    coalesce(quarantine_daily.quarantine_count, 0) as quarantine_count,
    coalesce(cost_latency_daily.total_cost_usd, 0) as total_cost_usd,
    coalesce(source_health_daily.source_fail_count, 0) as source_fail_count,
    current_timestamp as computed_at
from dates
left join ingest_daily on dates.pipeline_date = ingest_daily.pipeline_date
left join status_daily on dates.pipeline_date = status_daily.pipeline_date
left join
    quarantine_daily
    on dates.pipeline_date = quarantine_daily.pipeline_date
left join
    cost_latency_daily
    on dates.pipeline_date = cost_latency_daily.pipeline_date
left join
    source_health_daily
    on dates.pipeline_date = source_health_daily.pipeline_date

{% if is_incremental() %}
    {% if var('run_date', none) is not none %}
        where dates.pipeline_date = '{{ var("run_date") }}'::date
    {% else %}
        where dates.pipeline_date >= (
            select
                max(prior_run.pipeline_date) - {{ var('mart_pipeline_health_lookback_days') }}
            from {{ this }} as prior_run
        )
    {% endif %}
{% endif %}
