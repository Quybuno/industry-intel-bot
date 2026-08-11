-- §13.1: dim_source phải có đúng 1 dòng is_current cho mỗi source_id (không biểu diễn được
-- bằng generic schema test nên viết singular). Test PASS khi query trả về 0 dòng.
select
    source_id,
    count(*) as current_row_count
from {{ ref('dim_source') }}
where is_current
group by source_id
having count(*) <> 1
