{{ config(severity="warn") }}

-- §13.3: tỷ lệ bài có industry_tags rỗng trong một ngày >
-- {{ var('anomaly_empty_tag_rate_threshold') }} → prompt hoặc tập tag (INDUSTRY_TAGS,
-- contracts/llm_score.py) có vấn đề — model không gán được tag nào khớp danh sách hợp lệ.
--
-- Ngưỡng TUYỆT ĐỐI theo đúng ngày đang xét — không cần cửa sổ lịch sử/quy tắc tối thiểu.
--
-- severity=warn, cùng lý do các test anomaly khác.
--
-- `empty_tag_rate` NULL khi ngày đó chưa chấm bài nào (không suy diễn 0%, xem
-- mart_pipeline_health.sql).
select
    pipeline_date,
    empty_tag_rate,
    empty_tag_count,
    scored_count
from {{ ref('mart_pipeline_health') }}
where
    empty_tag_rate is not null
    and empty_tag_rate > {{ var('anomaly_empty_tag_rate_threshold') }}
