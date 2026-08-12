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
    -- Bổ sung task 1.4: mean/stddev_importance (drift), scored_count, empty_tag_count —
    -- CÙNG trục ngày scored_at::date với cost/latency (không phải first_seen_date), vì cả
    -- 3 thứ này đều là hệ quả trực tiếp của MỘT lần chấm điểm (industry_tags được ghi ở
    -- đúng lúc chấm — xem score/runner.py::_persist_score, UPDATE status='scored' kèm
    -- industry_tags trong cùng transaction). Loại provider test (`mock`) bằng
    -- is_production_model() — cùng hàng rào đã dùng ở int_scores_latest (§9 AGENTS.md) —
    -- để không lẫn dữ liệu test/CI vào theo dõi drift/chi phí thật.
    select
        scores.scored_at::date as pipeline_date,
        sum(scores.cost_usd) as total_cost_usd,
        percentile_cont(0.5) within group (order by scores.latency_ms)
            as latency_p50_ms,
        percentile_cont(0.95) within group (order by scores.latency_ms)
            as latency_p95_ms,
        count(*) as scored_count,
        avg(scores.importance) as mean_importance,
        stddev_samp(scores.importance) as stddev_importance,
        count(*) filter (
            where
            articles.industry_tags is null
            or array_length(articles.industry_tags, 1) is null
        ) as empty_tag_count
    from {{ ref('stg_article_scores') }} as scores
    inner join {{ ref('stg_articles') }} as articles
        on scores.article_id = articles.article_id
    where {{ is_production_model('scores.model_name') }}
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
    cost_latency_daily.mean_importance,
    cost_latency_daily.stddev_importance,
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
    -- Bổ sung task 1.4 (§13.3, cột `scored_count` trở xuống): NULL khi không có bài nào
    -- chấm ngày đó — KHÔNG suy diễn về 0 (P4, "không suy diễn hộ") — 0 nghĩa là "importance
    -- trung bình đúng bằng 0", sai với thực tế "chưa hề chấm bài nào". `scored_count`/
    -- `empty_tag_count` là số đếm thật nên coalesce về 0 hợp lý (đếm được, không phải suy
    -- diễn). `mean_importance`/`stddev_importance` (đã đưa lên đầu SELECT ở trên, cạnh
    -- latency, do sqlfluff ST06 yêu cầu cột giản đơn đứng trước biểu thức/aggregate) CŨNG
    -- thuộc nhóm bổ sung này — giữ NULL, không coalesce.
    coalesce(cost_latency_daily.scored_count, 0) as scored_count,
    coalesce(cost_latency_daily.empty_tag_count, 0) as empty_tag_count,
    case
        when coalesce(cost_latency_daily.scored_count, 0) = 0 then null
        else
            cost_latency_daily.empty_tag_count::numeric
            / cost_latency_daily.scored_count
    end as empty_tag_rate,
    case
        when coalesce(cost_latency_daily.scored_count, 0) = 0 then null
        else cost_latency_daily.total_cost_usd / cost_latency_daily.scored_count
    end as cost_per_article,
    -- quarantine_rate: mẫu số = scored_count + quarantine_count (mọi lần "thử chấm" trong
    -- ngày, dù thành công hay quarantine) — KHÔNG dùng eligible_count (§7.2 mục 13, khác
    -- trục ngày first_seen_date, gộp cả bài eligible CHƯA từng được thử chấm).
    case
        when
            coalesce(cost_latency_daily.scored_count, 0)
            + coalesce(quarantine_daily.quarantine_count, 0)
            = 0
            then null
        else
            coalesce(quarantine_daily.quarantine_count, 0)::numeric
            / (
                coalesce(cost_latency_daily.scored_count, 0)
                + coalesce(quarantine_daily.quarantine_count, 0)
            )
    end as quarantine_rate,
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
