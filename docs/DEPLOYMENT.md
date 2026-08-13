# Deploy production (task 1.10, PRODUCTION_PLAN §16.1-16.4, §17.2)

Máy production do user chọn: **máy remote Windows, đã cài Docker sẵn** (không phải máy dev
này — code được copy/kéo sang máy đó, không phải SSH từ đây, xem `docs/PROGRESS.md` mục 18).
Tài liệu này viết để chạy TRÊN chính máy remote đó.

Mọi lệnh `docker compose`/`dbt`/`alembic` ở đây đã tự chạy thật trên máy dev (build image,
`docker compose up -d`, `docker restart`, `curl` GraphQL xác nhận schedule sống sau restart)
trước khi viết vào tài liệu — xem `docs/PROGRESS.md` mục 18 để biết bằng chứng cụ thể. Phần
DUY NHẤT chưa tự verify được (vì không SSH được vào máy remote) là **hành vi reboot toàn bộ
hệ điều hành** — cơ chế `restart: unless-stopped` của Docker + Task Scheduler ở dưới là cách
làm chuẩn, đã test từng mảnh, nhưng chưa test được chuỗi "tắt máy thật → bật lại thật" trên
đúng máy production. Anh cần tự xác nhận bước đó 1 lần sau khi deploy (xem §5).

## 1. Chuẩn bị (một lần)

1. Copy/clone repo sang máy remote.
2. Tạo `.env` từ `.env.example` — **KHÔNG BAO GIỜ commit `.env` hay copy qua git** (đúng rào
   chắn task 1.10). Điền `DATABASE_URL`/`POSTGRES_*`/`GIT_PUBLISH_TOKEN`/`GITHUB_TOKEN`/
   `DEEPSEEK_API_KEY`/`HEARTBEAT_URL`/`TELEGRAM_*`/`LLM_PROVIDER=deepseek` (production, KHÔNG
   để `mock` — xem gotcha đã ghi ở `docs/PROGRESS.md` mục 5B: `mock` chạy trên bài thật từng
   lọt vào gold).
   - `DAGSTER_WEBSERVER_URL` đặt đúng địa chỉ máy này (vd. `http://<ip-máy-remote>:3000`),
     không để mặc định `localhost` — sensor 1.6 dùng biến này để gắn link "xem run" vào alert
     Telegram.
3. Build image:
   ```powershell
   docker compose build dagster-daemon dagster-webserver
   ```
4. **Bootstrap schema — CHỈ chạy 1 lần trên DB trống** (không phải mỗi lần start container,
   xem `docker-entrypoint.sh` — entrypoint chỉ `dbt parse`, KHÔNG tự chạy migration):
   ```powershell
   docker compose up -d postgres
   # đợi vài giây cho healthcheck pg_isready pass, rồi:
   uv run alembic upgrade head
   ```
5. Nhánh `gh-pages` (nơi GitHub Pages serve, §12.1) đã tồn tại SẴN trên GitHub (tạo 1 lần lúc
   phát triển task 1.10, không phụ thuộc máy nào) — `src/intel_bot/publish/git_publish.py` tự
   `git fetch` + `git worktree add` lúc chạy lần đầu trên máy MỚI, không cần làm gì thêm.

## 2. Chạy production

```powershell
docker compose up -d postgres dagster-daemon dagster-webserver
```

**KHÔNG chạy `ollama`** (profile `local-llm`, chỉ bật khi thật sự dùng Ollama local — provider
production là `deepseek` qua cloud API, xem §16.2 lý do khuyến nghị cloud LLM).

Webserver UI: `http://<ip-máy-remote>:3000` — mặc định `docker-compose.yml` bind
`127.0.0.1:3000` (chỉ máy đó tự truy cập, đúng rào chắn "không expose service không có auth ra
Internet", cùng tinh thần §19.3 dù đó nói riêng Postgres/Ollama). Xem UI từ xa: SSH tunnel
(`ssh -L 3000:localhost:3000 <user>@<máy-remote>`) hoặc đổi thành `"3000:3000"` trong
`docker-compose.yml` nếu chấp nhận rủi ro mở LAN/Internet (tự cân nhắc).

## 3. Start / stop / xem log — lệnh chính xác cho repo này

| Việc | Lệnh |
|---|---|
| Start toàn bộ (postgres + daemon + webserver) | `docker compose up -d postgres dagster-daemon dagster-webserver` |
| Stop toàn bộ (giữ nguyên volume/dữ liệu) | `docker compose stop dagster-daemon dagster-webserver postgres` |
| Restart riêng daemon (vd. sau khi đổi code/config) | `docker compose up -d --build dagster-daemon` |
| Xem log daemon (theo dõi real-time) | `docker compose logs -f dagster-daemon` |
| Xem log webserver | `docker compose logs -f dagster-webserver` |
| Xem 50 dòng log gần nhất, không theo dõi | `docker compose logs --tail 50 dagster-daemon` |
| Kiểm tra container đang chạy | `docker compose ps` |
| Xoá sạch (kể cả volume — **MẤT run history Dagster**, KHÔNG mất dữ liệu Postgres vì volume `pgdata` riêng) | `docker compose down` (không thêm `-v`) |

## 4. Tự khởi động lại sau reboot hệ thống (Windows Task Scheduler)

`restart: unless-stopped` trong `docker-compose.yml` (đã có sẵn cho cả 3 service) khiến
Docker tự khởi động lại CONTAINER khi Docker Engine khởi động — nhưng trên Windows, bản thân
**Docker Desktop cần được khởi động trước** (không tự chạy nền như một service thuần tuý khi
chưa có phiên đăng nhập Windows nào, khác Docker Engine trên Linux/systemd). Cách chuẩn: tạo
1 Scheduled Task chạy Docker Desktop lúc khởi động máy.

```powershell
# Chạy 1 lần, quyền Administrator
$action = New-ScheduledTaskAction -Execute "C:\Program Files\Docker\Docker\Docker Desktop.exe"
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERNAME" -LogonType Interactive -RunLevel Highest
Register-ScheduledTask -TaskName "DockerDesktopAutoStart" -Action $action -Trigger $trigger -Principal $principal -Description "Tu khoi dong Docker Desktop sau reboot (task 1.10)"
```

Docker Desktop có tuỳ chọn riêng **Settings → General → "Start Docker Desktop when you log
in"** — bật thêm tuỳ chọn này (bổ sung, không thay thế Task ở trên) để phòng trường hợp máy
cấu hình tự đăng nhập (auto-logon) sau reboot.

Vì Docker Desktop containers có `restart: unless-stopped`, MỘT KHI Docker Desktop khởi động
lại xong, `postgres`/`dagster-daemon`/`dagster-webserver` tự khởi động lại theo — không cần
chạy tay `docker compose up -d` lần nữa. Nếu muốn chắc chắn tuyệt đối (Docker Desktop có độ
trễ khởi động, container có thể chưa kịp tự phục hồi ngay), thêm một Scheduled Task thứ 2 chạy
SAU task trên vài phút:

```powershell
$action2 = New-ScheduledTaskAction -Execute "docker" -Argument "compose up -d postgres dagster-daemon dagster-webserver" -WorkingDirectory "D:\industry-intel-bot"
$trigger2 = New-ScheduledTaskTrigger -AtStartup
$trigger2.Delay = "PT3M"  # đợi 3 phút cho Docker Desktop khởi động xong hẳn
Register-ScheduledTask -TaskName "IntelBotComposeUp" -Action $action2 -Trigger $trigger2 -Principal $principal -Description "Dam bao docker compose up sau khi Docker Desktop san sang (task 1.10)"
```

**Chưa tự verify được bằng reboot thật** (xem đầu file) — anh tự khởi động lại máy 1 lần sau
khi deploy, đợi ~5 phút, rồi chạy `docker compose ps` xác nhận cả 3 container đang "Up".

## 5. §17.2 — vì sao KHÔNG dùng `pipeline.yml` cron trên GitHub Actions

PRODUCTION_PLAN §17.2 đưa ra 2 phương án chạy hằng ngày: (A) `pipeline.yml` cron trên GitHub
Actions hosted runner, HOẶC (B) Dagster schedule thật nếu Dagster chạy daemon 24/7 trên máy/VPS
riêng — nói rõ *"Nếu Dagster chạy trên VPS thì dùng Dagster schedule và bỏ workflow này"*.

Task 1.10 triển khai chính xác phương án (B): `dagster-daemon` chạy 24/7 thật (§3 ở trên, đã
verify chạy thật + sống sau restart), 3 schedule (`daily_pipeline_job_schedule` 05:00 — tên
schedule THẬT hiện trên UI/GraphQL, khác tên biến Python `daily_pipeline_schedule` trong
`schedules.py`, xem cách `build_schedule_from_partitioned_job` tự đặt tên;
`midday_ingest_schedule` 12:00, `evening_ingest_schedule` 18:00) đặt `default_status=RUNNING`
(đổi từ STOPPED — xem `dagster_project/schedules.py`, lý do STOPPED trước đây đúng là "chưa có
daemon production thật", giờ đã có). Thêm `pipeline.yml` cron chạy CÙNG lịch 05:00 trên GitHub
Actions sẽ tạo ra **2 tiến trình độc lập cùng chạy pipeline cho cùng một partition** — dù
`ON CONFLICT`/idempotency (P1) khiến kết quả DATA không sai, nhưng: (a) tốn gấp đôi lượt gọi
LLM thật (chi phí thật, không phải rủi ro lý thuyết — §16.2 đã tính cost theo ~1 lần chạy/
ngày), (b) 2 tiến trình cùng `git push` docs-site/ lên `gh-pages` có thể đụng nhau (push
conflict, phải retry), (c) không có lý do vận hành nào cần "backup chạy dự phòng qua CI" khi
đã có daemon thật 24/7 — CLI `pipeline` (task 1.8/1.9) đã đóng đúng vai trò "đường dự phòng
KHI Dagster không dùng được", chạy tay khi cần, không cần thêm một cron song song.

**Quyết định: KHÔNG tạo `pipeline.yml`.** `ci.yml` (task 1.9) vẫn giữ nguyên — đó là CI cho
MỖI PR, không phải lịch chạy hằng ngày, không liên quan quyết định này.
