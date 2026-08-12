{{ config(severity="warn") }}

-- §13.3: row count ingest một ngày lệch > {{ var('anomaly_ingest_stddev_threshold') }}σ so
-- với trung bình {{ var('anomaly_ingest_window_days') }} ngày TRƯỚC đó (không tính ngày
-- đang xét) → phát hiện feed chết hoặc feed spam.
--
-- severity=warn (KHÔNG error): một ngày ingest bất thường là tín hiệu cần NHÌN, không phải
-- lỗi phá hỏng pipeline — dbt build phải exit 0 để job dbt/Dagster phía sau (fct_article_score,
-- mart_daily_digest) vẫn chạy tiếp bình thường (đề bài mục 3).
--
-- Quy tắc dữ liệu tối thiểu (nhiệm vụ 4, giải thích đầy đủ ở dbt_project.yml): chỉ đánh giá
-- khi có > anomaly_ingest_window_days dòng ĐỨNG TRƯỚC ngày đang xét trong chính
-- mart_pipeline_health — window function `rows between N preceding and 1 preceding` trên
-- CÁC DÒNG THẬT SỰ TỒN TẠI (không phải N ngày lịch dương lịch — nếu một ngày hoàn toàn
-- không có hoạt động nào thì không có dòng, và cửa sổ tự động bỏ qua ngày đó, không suy diễn
-- ingest_count = 0 cho ngày trống). Test PASS khi query trả về 0 dòng.

with windowed as (
    select
        pipeline_date,
        ingest_count,
        avg(ingest_count) over (
            order by pipeline_date
            rows between {{ var('anomaly_ingest_window_days') }} preceding and 1 preceding
        ) as baseline_mean,
        stddev_samp(ingest_count) over (
            order by pipeline_date
            rows between {{ var('anomaly_ingest_window_days') }} preceding and 1 preceding
        ) as baseline_stddev,
        row_number() over (order by pipeline_date) as day_number
    from {{ ref('mart_pipeline_health') }}
)

select
    pipeline_date,
    ingest_count,
    baseline_mean,
    baseline_stddev
from windowed
where
    day_number > {{ var('anomaly_ingest_window_days') }}
    and baseline_stddev is not null
    and abs(ingest_count - baseline_mean)
    > {{ var('anomaly_ingest_stddev_threshold') }} * baseline_stddev
