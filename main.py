import feedparser

# SỬA LẠI DÒNG NÀY (Dùng link RSS chuẩn, không dùng link trang chủ)
rss_url = "https://venturebeat.com" 
# Hoặc link dự phòng: "https://venturebeat.com"

print(f"Đang đọc dữ liệu từ: {rss_url}...\n")

feed = feedparser.parse(rss_url)

if len(feed.entries) == 0:
    print("Không tìm thấy bài viết nào. Hãy kiểm tra lại link RSS!")
else:
    # Lấy ra 5 bài viết mới nhất
    for index, entry in enumerate(feed.entries[:5], start=1):
        print(f"{index}. {entry.title}")
        print(f"   Link: {entry.link}")
        print("-" * 50)
