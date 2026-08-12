{{ config(severity="warn") }}

-- §13.3: tỷ lệ quarantine > {{ var('anomaly_quarantine_rate_threshold') }} trong một ngày
-- → prompt hỏng hoặc provider đổi hành vi. Ngưỡng TUYỆT ĐỐI theo đúng ngày đang xét (khác
-- 3 kiểm tra so-với-baseline khác) — §13.3 không mô tả kiểm tra này theo cửa sổ lịch sử,
-- nên không cần quy tắc "đủ dữ liệu tối thiểu" (nhiệm vụ 4 chỉ áp dụng cho kiểm tra có
-- baseline nhiều ngày).
--
-- severity=warn: bất thường cần nhìn, không được làm chết dbt build (đề bài mục 3).
--
-- `quarantine_rate` NULL nghĩa là ngày đó chưa có lần "thử chấm" nào (không phải 0% —
-- không suy diễn, xem mart_pipeline_health.sql) → loại khỏi test, không phải bất thường.
select
    pipeline_date,
    quarantine_rate
from {{ ref('mart_pipeline_health') }}
where
    quarantine_rate is not null
    and quarantine_rate > {{ var('anomaly_quarantine_rate_threshold') }}
