-- §13.2: mart_daily_digest có >= 5 dòng. Test PASS khi query trả về 0 dòng (tức không vi
-- phạm ngưỡng tối thiểu).
select count(*) as row_count
from {{ ref('mart_daily_digest') }}
having count(*) < 5
