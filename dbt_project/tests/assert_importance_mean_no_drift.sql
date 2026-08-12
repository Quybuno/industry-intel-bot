{{ config(severity="warn") }}

-- §13.3: mean_importance một ngày lệch > {{ var('anomaly_importance_drift_threshold') }}
-- so với trung bình {{ var('anomaly_importance_window_days') }} ngày TRƯỚC đó → MODEL
-- DRIFT — provider cloud âm thầm đổi version model phía sau API, không có gì khác phát
-- hiện được ngoài theo dõi phân phối điểm (§13.3 mô tả đây là kiểm tra đáng chú ý nhất).
--
-- severity=warn, cùng lý do assert_ingest_count_no_anomaly.sql. Quy tắc dữ liệu tối thiểu
-- (nhiệm vụ 4) giống hệt: chỉ đánh giá khi có > anomaly_importance_window_days dòng
-- ĐỨNG TRƯỚC trong mart_pipeline_health.

with windowed as (
    select
        pipeline_date,
        mean_importance,
        avg(mean_importance) over (
            order by pipeline_date
            rows between {{ var('anomaly_importance_window_days') }} preceding and 1 preceding
        ) as baseline_mean,
        row_number() over (order by pipeline_date) as day_number
    from {{ ref('mart_pipeline_health') }}
)

select
    pipeline_date,
    mean_importance,
    baseline_mean
from windowed
where
    day_number > {{ var('anomaly_importance_window_days') }}
    -- Ngày không có bài nào được chấm -> mean_importance NULL, không suy diễn 0.
    and mean_importance is not null
    and baseline_mean is not null
    and abs(mean_importance - baseline_mean)
    > {{ var('anomaly_importance_drift_threshold') }}
