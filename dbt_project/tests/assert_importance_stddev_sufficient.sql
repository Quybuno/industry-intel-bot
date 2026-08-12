{{ config(severity="warn") }}

-- §13.3: stddev_importance một ngày < {{ var('anomaly_importance_stddev_min') }} → model
-- dồn điểm, mất khả năng phân biệt bài quan trọng/không quan trọng (rubric vô dụng dù
-- vẫn trả JSON hợp lệ — quarantine/contract test không bắt được lớp lỗi này).
--
-- Ngưỡng TUYỆT ĐỐI theo đúng ngày đang xét (giống assert_quarantine_rate_reasonable.sql) —
-- không cần cửa sổ lịch sử, không cần quy tắc "đủ dữ liệu tối thiểu".
--
-- severity=warn, cùng lý do các test anomaly khác — không được làm chết dbt build.
--
-- `stddev_importance` NULL khi ngày đó có 0 hoặc đúng 1 bài được chấm (STDDEV_SAMP cần
-- ít nhất 2 điểm dữ liệu) — không đủ căn cứ để nói "mất khả năng phân biệt", loại khỏi
-- test thay vì suy diễn.
select
    pipeline_date,
    stddev_importance,
    scored_count
from {{ ref('mart_pipeline_health') }}
where
    stddev_importance is not null
    and stddev_importance < {{ var('anomaly_importance_stddev_min') }}
