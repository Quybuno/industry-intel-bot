# AGENTS.md — Quy ước làm việc cho AI Coding Agent

## 1. Dự án

`industry-intel-bot` — batch data pipeline chạy hằng ngày: thu thập tin từ ~20 nguồn RSS
và GitHub Search API → chuẩn hoá, khử trùng lặp → chấm điểm và tóm tắt tiếng Việt bằng LLM
→ mô hình hoá bằng dbt → xuất bản trang tĩnh.

Kiến trúc: **Medallion (bronze / silver / gold)** trên PostgreSQL đơn node.
Tài liệu thiết kế đầy đủ: `docs/PRODUCTION_PLAN.md` — **đọc file này trước khi làm bất cứ việc gì.**

Đây KHÔNG phải bài toán big data (~200 bản ghi/ngày). Không đề xuất Spark, Kafka,
hay bất kỳ hạ tầng phân tán nào. Quyết định này đã được chốt (ADR-015).

## 2. Ngăn xếp công nghệ đã chốt

| Hạng mục | Lựa chọn | Ghi chú |
|---|---|---|
| Python | 3.12 | |
| Quản lý gói | `uv` | Không dùng poetry, không dùng pip trực tiếp |
| Điều phối | Dagster | Asset-based, partition theo ngày |
| Biến đổi | dbt-core + dbt-postgres | Toàn bộ business logic nằm ở đây |
| CSDL | PostgreSQL 16 | Chạy bằng Docker Compose |
| Migration | Alembic | |
| Truy cập DB | psycopg 3 + SQLAlchemy Core | KHÔNG dùng ORM |
| Validation | Pydantic v2 + pydantic-settings | |
| HTTP client | httpx (async) | |
| RSS parser | feedparser | |
| CLI | Typer | |
| Logging | structlog, định dạng JSON | |
| Template | Jinja2 | |
| Test | pytest + pytest-asyncio | |
| Lint | ruff (format + lint) | |
| Type check | mypy `--strict` | |
| SQL lint | sqlfluff, dialect postgres | |

**Không thêm bất kỳ dependency nào ngoài danh sách trên.** Nếu thấy cần thêm, DỪNG lại
và giải thích vì sao, chờ người dùng đồng ý.

## 3. Quy ước code

- Type hint đầy đủ cho mọi hàm, kể cả hàm nội bộ. `mypy --strict` phải sạch.
- Không dùng `Any` trừ khi có comment giải thích lý do.
- Hàm thuần (pure function) tách khỏi hàm có I/O. Hàm có I/O nhận connection/client
  qua tham số, không tự tạo bên trong — để test được.
- Mọi hằng số cấu hình đọc từ `config/*.yaml` hoặc biến môi trường.
  **Tuyệt đối không hardcode:** tên model LLM, bảng giá token, ngưỡng lọc, URL nguồn,
  số ngày cửa sổ, giới hạn số bài/ngày.
- Docstring theo chuẩn Google, viết bằng tiếng Việt, chỉ cho hàm public.
- Tên biến và tên hàm bằng tiếng Anh. Comment và docstring bằng tiếng Việt.
- Múi giờ: mọi thời điểm lưu dạng `TIMESTAMPTZ`. Logic nghiệp vụ theo `Asia/Ho_Chi_Minh`.
  Không dùng `datetime.now()` trần — luôn `datetime.now(tz=ZoneInfo("Asia/Ho_Chi_Minh"))`.
- Log dạng structured, không dùng `print`. Mỗi log có `event`, `source_id` (nếu có),
  `partition_date` (nếu có).

## 4. Nguyên tắc kiến trúc bắt buộc (từ plan mục 3)

| Mã | Nguyên tắc | Nghĩa là |
|---|---|---|
| P1 | Tính lũy đẳng | Mọi asset có partition key = ngày. Chạy lại một partition ghi đè đúng phạm vi đó, không đụng ngày khác. Dùng `MERGE`/`ON CONFLICT`, không dùng `INSERT` trần |
| P2 | Dữ liệu thô bất biến | Bảng bronze không bao giờ UPDATE/DELETE |
| P3 | Schema chặt cho contract, lỏng cho payload | Payload gốc lưu JSONB; trường đã bóc tách có kiểu chặt và constraint |
| P4 | Thất bại rõ ràng | Bản ghi không thoả contract → vào bảng quarantine kèm nguyên văn response, đếm được. KHÔNG raise exception làm chết job. KHÔNG clamp giá trị ngoài miền |
| P5 | Tách compute và transform | Python lo I/O và gọi LLM; SQL (dbt) lo business logic. Không nhúng logic tính điểm, xếp hạng, dedup cấp 2 vào Python |
| P6 | Quan sát được mặc định | Mọi asset ghi metadata: row count, chi phí, độ trễ |
| P7 | Tái lập được | Pin dependency, đánh version prompt, migration có thứ tự |
| P8 | Đúng kích cỡ | Không thêm công cụ nào trước khi dữ liệu chứng minh cần nó |

## 5. Điều TUYỆT ĐỐI không làm

1. Không viết code giả lập, không để lại `TODO`, `pass`, `...`, hay comment
   "viết tiếp ở đây". Mọi hàm phải chạy thật.
2. Không tự ý làm trước các bước chưa được giao.
3. Không sửa `docs/PRODUCTION_PLAN.md`, `AGENTS.md`, `.env`, và các file migration
   đã tồn tại — trừ khi được yêu cầu rõ ràng.
4. Không hardcode tên model LLM. **Lưu ý cụ thể:** nhiều model Gemini đã bị ngừng phục vụ.
   Tên model là giá trị trong `config/models.yaml`, không phải hằng số trong code.
   Nếu cần biết tên model hiện hành, DỪNG và hỏi — không tự đoán.
5. Không tự thêm dependency, không tự đổi cấu trúc thư mục ở mục 6 của plan.
6. Không sinh dữ liệu mẫu giả để "cho test pass". Test dùng fixture thật đã lưu.
7. Nếu thiếu thông tin để quyết định, **DỪNG lại và hỏi. Không tự chọn giùm.**

## 6. Quy trình mỗi lần giao việc

1. Đọc `docs/PRODUCTION_PLAN.md` phần liên quan
2. Nêu lại nhiệm vụ bằng 2–3 câu để xác nhận đã hiểu đúng
3. Liệt kê các file sẽ tạo/sửa **trước khi** viết code
4. Viết code
5. Chạy `ruff format`, `ruff check`, `mypy --strict`, `pytest` — sửa cho sạch
6. Báo cáo: file đã tạo, cách kiểm chứng, điều gì còn thiếu