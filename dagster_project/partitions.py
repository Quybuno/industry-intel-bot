"""Partition theo ngày, múi giờ Asia/Ho_Chi_Minh (task 0.12 mục 2) — dùng chung cho mọi
asset "daily" ở bảng asset graph (§7.2). Asset không dùng `datetime.now()` để chọn ngày xử
lý; ngày LUÔN lấy từ `context.partition_key` do Dagster cấp theo định nghĩa này.

`start_date` = ngày ingest thật đầu tiên có trong DB dev (2026-08-10, xem docs/PROGRESS.md
mục 6) — không đặt sớm hơn để UI không hiện hàng loạt partition trống vô nghĩa trước đó.

`end_offset=1`: mặc định (`end_offset=0`) chỉ coi một partition ngày là "có sẵn" SAU KHI
ngày đó đã trôi qua hết (cửa sổ ngày phải kết thúc trước "now") — nghĩa là partition của
"hôm nay" sẽ không materialize thủ công được cho tới nửa đêm hôm sau, trái với lịch chạy
05:00 (§7.3: chạy CHO partition hôm nay, ngay trong ngày đó). `end_offset=1` mở thêm đúng
một partition đang-diễn-ra để "hôm nay" luôn chọn được (đã tự verify: không có nó,
`daily_partitions.get_partition_keys()` chỉ trả về đến hết ngày hôm qua).
"""

from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(
    start_date="2026-08-10",
    timezone="Asia/Ho_Chi_Minh",
    end_offset=1,
)
