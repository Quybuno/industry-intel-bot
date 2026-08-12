{{ config(severity="warn") }}

-- §13.3: cost/bài một ngày lệch TƯƠNG ĐỐI > {{ var('anomaly_cost_drift_pct') }} (vd. 0.50 =
-- 50%) so với trung bình {{ var('anomaly_cost_window_days') }} ngày TRƯỚC đó → prompt phình
-- (input_tokens tăng) hoặc provider đổi bảng giá.
--
-- severity=warn, cùng lý do các test anomaly khác. Quy tắc dữ liệu tối thiểu (nhiệm vụ 4)
-- giống assert_ingest_count_no_anomaly.sql: chỉ đánh giá khi có >
-- anomaly_cost_window_days dòng ĐỨNG TRƯỚC trong mart_pipeline_health.
--
-- `baseline_mean > 0` (thay vì `<> 0`): cost_per_article không âm theo miền giá trị, > 0
-- vừa loại NULL vừa loại 0 trong một điều kiện, tránh chia cho 0 mà không suy diễn "lệch
-- vô cực" khi baseline từng là 0 (vd. toàn bài baseline dùng mock, cost=0 — dù thực tế mock
-- đã bị loại khỏi truy vấn nguồn của mart_pipeline_health rồi, giữ điều kiện này cho chắc).

with windowed as (
    select
        pipeline_date,
        cost_per_article,
        avg(cost_per_article) over (
            order by pipeline_date
            rows between {{ var('anomaly_cost_window_days') }} preceding and 1 preceding
        ) as baseline_mean,
        row_number() over (order by pipeline_date) as day_number
    from {{ ref('mart_pipeline_health') }}
)

select
    pipeline_date,
    cost_per_article,
    baseline_mean
from windowed
where
    day_number > {{ var('anomaly_cost_window_days') }}
    and cost_per_article is not null
    and baseline_mean > 0
    and abs(cost_per_article - baseline_mean) / baseline_mean
    > {{ var('anomaly_cost_drift_pct') }}
