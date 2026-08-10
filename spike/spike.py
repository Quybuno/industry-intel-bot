"""Spike (Phase -1) — trả lời 3 câu hỏi kiến trúc trước khi xây pipeline thật.

File này ĐỘC LẬP, không phụ thuộc vào `src/intel_bot`. Lưu tạm bằng SQLite
(`spike/spike.db`). Chạy tay:

    python spike/spike.py q1 --sources-file spike/sources.txt
    python spike/spike.py q2 --limit 50
    python spike/spike.py q3 --limit 20

Q1: Mỗi ngày thực sự có bao nhiêu bài? (fetch 5 nguồn RSS, đếm entry mới trong 24h)
Q2: LLM chấm điểm 4 tiêu chí có phân biệt được không? (thống kê phân phối + tương quan)
Q3: Tóm tắt 5 bullet từ snippet có đáng đọc không? (sinh ra spike/summaries.md để tự chấm)

Tham số LLM (base URL, tên model, API key) đọc từ biến môi trường — KHÔNG hardcode:
    SPIKE_LLM_BASE_URL   ví dụ http://localhost:11434/v1 (Ollama) hoặc https://api.openai.com/v1
    SPIKE_LLM_MODEL      ví dụ qwen2.5:14b hoặc gpt-4o-mini
    SPIKE_LLM_API_KEY    tuỳ chọn — để trống nếu backend không cần (vd. Ollama local)

Endpoint gọi là `{SPIKE_LLM_BASE_URL}/chat/completions` theo định dạng tương thích OpenAI
(Ollama >= 0.1.26 hỗ trợ sẵn định dạng này, không cần thêm dependency ngoài httpx).
"""

from __future__ import annotations

import argparse
import calendar
import json
import logging
import math
import random
import re
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import feedparser  # type: ignore[import-untyped]
import httpx
from dotenv import load_dotenv

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")  # console Windows mặc định cp1252, không in được tiếng Việt
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stderr)
logger = logging.getLogger("spike")

SPIKE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = SPIKE_DIR / "spike.db"
DEFAULT_RAW_RESPONSES_PATH = SPIKE_DIR / "raw_responses.jsonl"
DEFAULT_SUMMARIES_PATH = SPIKE_DIR / "summaries.md"
FRESHNESS_WINDOW = timedelta(hours=24)
SCORE_CRITERIA = ("credibility", "importance", "depth", "practicality")


@dataclass(frozen=True)
class LlmConfig:
    """Cấu hình LLM đọc từ biến môi trường, không hardcode tên model."""

    base_url: str
    model: str
    api_key: str | None


@dataclass(frozen=True)
class Article:
    """Một bài đã fetch, đọc lại từ SQLite."""

    id: int
    source_url: str
    title: str
    link: str
    summary: str


def load_llm_config() -> LlmConfig:
    """Đọc cấu hình LLM từ biến môi trường. Dừng rõ ràng nếu thiếu, không đoán giá trị."""
    load_dotenv()
    base_url = _require_env("SPIKE_LLM_BASE_URL")
    model = _require_env("SPIKE_LLM_MODEL")
    api_key = _optional_env("SPIKE_LLM_API_KEY")
    return LlmConfig(base_url=base_url.rstrip("/"), model=model, api_key=api_key)


def _require_env(name: str) -> str:
    """Đọc biến môi trường bắt buộc; nếu thiếu, dừng lại và báo rõ chứ không đoán."""
    value = _optional_env(name)
    if not value:
        sys.exit(
            f"Thiếu biến môi trường {name}. Đặt trong .env, ví dụ:\n"
            f"  SPIKE_LLM_BASE_URL=http://localhost:11434/v1\n"
            f"  SPIKE_LLM_MODEL=qwen2.5:14b\n"
            f"  SPIKE_LLM_API_KEY=  # để trống nếu backend không cần key\n"
            "Không có giá trị mặc định hardcode — bạn phải tự chọn model."
        )
    return value


def _optional_env(name: str) -> str | None:
    """Đọc biến môi trường tuỳ chọn từ os.environ (qua dotenv đã load)."""
    import os

    value = os.environ.get(name)
    return value if value else None


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Mở kết nối SQLite và đảm bảo schema tồn tại."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT NOT NULL,
            title TEXT NOT NULL,
            link TEXT NOT NULL UNIQUE,
            summary TEXT NOT NULL DEFAULT '',
            published_at TEXT,
            has_date_field INTEGER NOT NULL,
            fetched_at TEXT NOT NULL,
            raw_entry_json TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _json_safe(value: object) -> object:
    """Chuyển đối tượng feedparser (vd. time.struct_time) thành dạng JSON hoá được."""
    if isinstance(value, time_struct_types()):
        return datetime.fromtimestamp(calendar.timegm(value), tz=timezone.utc).isoformat()  # type: ignore[arg-type]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def time_struct_types() -> tuple[type, ...]:
    """Trả về tuple kiểu time.struct_time để dùng với isinstance (tránh import lặp)."""
    import time

    return (time.struct_time,)


def extract_published_at(entry: Any) -> tuple[datetime | None, bool]:
    """Lấy thời điểm xuất bản (UTC) và cờ 'có trường ngày' từ một entry feedparser.

    Trả về (published_at hoặc None nếu không parse được, has_date_field).
    has_date_field = True nếu entry có bất kỳ trường published/updated thô nào,
    kể cả khi parse thất bại — để Q1 phân biệt "không có ngày" với "có ngày nhưng lỗi định dạng".
    """
    has_date_field = bool(entry.get("published") or entry.get("updated"))
    for field in ("published_parsed", "updated_parsed"):
        struct_time = entry.get(field)
        if struct_time:
            return datetime.fromtimestamp(calendar.timegm(struct_time), tz=timezone.utc), has_date_field
    return None, has_date_field


def fetch_feed(client: httpx.Client, url: str) -> Any:
    """Fetch một RSS feed qua httpx rồi parse bằng feedparser."""
    response = client.get(url, timeout=30.0, follow_redirects=True)
    response.raise_for_status()
    return feedparser.parse(response.content)


def read_sources(sources_arg: str | None, sources_file: Path | None) -> list[str]:
    """Đọc danh sách URL nguồn RSS từ tham số dòng lệnh hoặc file txt (mỗi dòng một URL)."""
    urls: list[str] = []
    if sources_arg:
        urls.extend(u.strip() for u in sources_arg.split(",") if u.strip())
    if sources_file:
        for line in sources_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                urls.append(stripped)
    if not urls:
        sys.exit("Không có nguồn RSS nào. Truyền --sources hoặc --sources-file.")
    if len(urls) != 5:
        logger.warning("event=source_count_mismatch expected=5 got=%d", len(urls))
    return urls


def store_entries(conn: sqlite3.Connection, source_url: str, feed: Any) -> int:
    """Lưu các entry của một feed vào bảng articles (bỏ qua trùng link). Trả về số dòng mới."""
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    inserted = 0
    for entry in feed.entries:
        link = entry.get("link") or entry.get("id")
        if not link:
            continue
        published_at, has_date_field = extract_published_at(entry)
        raw_json = json.dumps(_json_safe(dict(entry)), ensure_ascii=False)
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO articles
                (source_url, title, link, summary, published_at, has_date_field, fetched_at, raw_entry_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_url,
                entry.get("title", ""),
                link,
                entry.get("summary", "") or entry.get("description", ""),
                published_at.isoformat() if published_at else None,
                1 if has_date_field else 0,
                now_iso,
                raw_json,
            ),
        )
        inserted += cursor.rowcount
    conn.commit()
    return inserted


def cmd_q1(args: argparse.Namespace) -> None:
    """Q1 — Mỗi ngày thực sự có bao nhiêu bài? Fetch nguồn, đếm entry, in bảng."""
    sources = read_sources(args.sources, args.sources_file)
    conn = get_connection(args.db)
    now = datetime.now(tz=timezone.utc)
    rows: list[tuple[str, int, int, str]] = []
    with httpx.Client() as client:
        for url in sources:
            try:
                feed = fetch_feed(client, url)
            except httpx.HTTPError as exc:
                logger.warning("event=fetch_failed source_url=%s error=%s", url, exc)
                rows.append((url, 0, 0, "fetch_error"))
                continue
            store_entries(conn, url, feed)
            total = len(feed.entries)
            recent = 0
            any_date_field = False
            for entry in feed.entries:
                published_at, has_date_field = extract_published_at(entry)
                any_date_field = any_date_field or has_date_field
                if published_at and now - published_at <= FRESHNESS_WINDOW:
                    recent += 1
            rows.append((url, total, recent, "yes" if any_date_field else "no"))
    conn.close()
    _print_q1_table(rows)


def _print_q1_table(rows: list[tuple[str, int, int, str]]) -> None:
    """In bảng thống kê Q1: nguồn | tổng entry | entry mới 24h | có trường ngày."""
    headers = ("nguồn", "tổng entry", "mới trong 24h", "có trường ngày")
    col_widths = [len(headers[0]), len(headers[1]), len(headers[2]), len(headers[3])]
    str_rows = [(url, str(total), str(recent), has_date) for url, total, recent, has_date in rows]
    for row in str_rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))
    def fmt_row(cells: tuple[str, str, str, str]) -> str:
        return " | ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(cells))
    print(fmt_row(headers))
    print("-+-".join("-" * w for w in col_widths))
    for row in str_rows:
        print(fmt_row(row))
    total_entries = sum(r[1] for r in rows)
    total_recent = sum(r[2] for r in rows)
    print(f"\nTổng: {total_entries} entry, {total_recent} entry mới trong 24h qua {len(rows)} nguồn.")


def call_llm(config: LlmConfig, messages: list[dict[str, str]]) -> dict[str, Any]:
    """Gọi endpoint chat completions tương thích OpenAI, temperature=0. Trả về JSON thô."""
    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    payload = {"model": config.model, "messages": messages, "temperature": 0}
    response = httpx.post(
        f"{config.base_url}/chat/completions", json=payload, headers=headers, timeout=120.0
    )
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    return result


def parse_json_object(text: str) -> dict[str, Any] | None:
    """Bóc JSON thuần từ nội dung trả về của LLM (chấp nhận cả khi bọc trong ```json fence)."""
    candidate = text.strip()
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    fence_match = re.search(r"```(?:json)?\s*(.*?)```", candidate, flags=re.DOTALL)
    if fence_match:
        try:
            parsed = json.loads(fence_match.group(1).strip())
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def fetch_sample_articles(conn: sqlite3.Connection, limit: int) -> list[Article]:
    """Lấy ngẫu nhiên `limit` bài đã fetch từ SQLite."""
    rows = conn.execute(
        "SELECT id, source_url, title, link, summary FROM articles ORDER BY RANDOM() LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        Article(id=r["id"], source_url=r["source_url"], title=r["title"], link=r["link"], summary=r["summary"])
        for r in rows
    ]


def build_scoring_messages(article: Article) -> list[dict[str, str]]:
    """Tạo prompt chấm điểm 4 tiêu chí, yêu cầu trả JSON thuần."""
    system = (
        "Bạn là chuyên gia đánh giá tin tức công nghệ/công nghiệp. "
        "Chấm bài viết theo 4 tiêu chí, thang điểm số nguyên 1-10: "
        "credibility (độ tin cậy nguồn), importance (mức độ quan trọng), "
        "depth (độ sâu nội dung), practicality (tính ứng dụng thực tế). "
        "CHỈ trả về JSON thuần với đúng 4 khoá này, không giải thích thêm, "
        'ví dụ: {"credibility": 7, "importance": 6, "depth": 5, "practicality": 8}'
    )
    user = f"Tiêu đề: {article.title}\n\nTóm tắt/snippet: {article.summary}"
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def cmd_q2(args: argparse.Namespace) -> None:
    """Q2 — LLM chấm điểm 4 tiêu chí có phân biệt được không? Thống kê phân phối + tương quan."""
    config = load_llm_config()
    conn = get_connection(args.db)
    articles = fetch_sample_articles(conn, args.limit)
    conn.close()
    if not articles:
        sys.exit("Chưa có bài nào trong DB. Chạy `q1` trước để fetch dữ liệu.")
    if len(articles) < args.limit:
        logger.warning("event=insufficient_articles requested=%d available=%d", args.limit, len(articles))

    scores: dict[str, list[float]] = {c: [] for c in SCORE_CRITERIA}
    parse_failures = 0
    with args.raw_responses.open("w", encoding="utf-8") as raw_file:
        for article in articles:
            messages = build_scoring_messages(article)
            record: dict[str, Any] = {"article_id": article.id, "link": article.link, "title": article.title}
            try:
                response = call_llm(config, messages)
            except httpx.HTTPError as exc:
                logger.warning("event=llm_call_failed article_id=%d error=%s", article.id, exc)
                record["error"] = str(exc)
                raw_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                parse_failures += 1
                continue
            record["raw_response"] = response
            content = response["choices"][0]["message"]["content"]
            parsed = parse_json_object(content)
            if parsed is None or not all(k in parsed for k in SCORE_CRITERIA):
                logger.warning("event=score_parse_failed article_id=%d", article.id)
                record["error"] = "parse_failed"
                raw_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                parse_failures += 1
                continue
            record["parsed"] = {k: parsed[k] for k in SCORE_CRITERIA}
            raw_file.write(json.dumps(record, ensure_ascii=False) + "\n")
            for criterion in SCORE_CRITERIA:
                value = parsed[criterion]
                if isinstance(value, (int, float)) and 1 <= value <= 10:
                    scores[criterion].append(float(value))

    print(f"Đã chấm {len(articles)} bài, {parse_failures} lỗi parse/gọi LLM (xem {args.raw_responses}).\n")
    _print_q2_stats(scores)


def _print_q2_stats(scores: dict[str, list[float]]) -> None:
    """In thống kê mô tả + biểu đồ tần suất + ma trận tương quan cho 4 tiêu chí."""
    print("Trung bình / độ lệch chuẩn theo tiêu chí:")
    for criterion in SCORE_CRITERIA:
        values = scores[criterion]
        if not values:
            print(f"  {criterion}: (không có dữ liệu hợp lệ)")
            continue
        mean = sum(values) / len(values)
        stdev = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))
        print(f"  {criterion}: n={len(values)} mean={mean:.2f} stdev={stdev:.2f}")

    importance = scores["importance"]
    if importance:
        print("\nBiểu đồ tần suất importance (1-10):")
        counts = Counter(int(v) for v in importance)
        max_count = max(counts.values())
        for value in range(1, 11):
            count = counts.get(value, 0)
            bar_len = round(count / max_count * 40) if max_count else 0
            print(f"  {value:2d} | {'#' * bar_len} {count}")

        top3 = counts.most_common(3)
        top3_ratio = sum(c for _, c in top3) / len(importance)
        top3_desc = ", ".join(f"{v}({c})" for v, c in top3)
        print(f"\n3 giá trị importance phổ biến nhất: {top3_desc} — chiếm {top3_ratio:.1%} tổng số bài.")

    print("\nMa trận tương quan (Pearson) giữa 4 tiêu chí:")
    _print_correlation_matrix(scores)


def _print_correlation_matrix(scores: dict[str, list[float]]) -> None:
    """Tính và in ma trận tương quan Pearson 4x4, chỉ dùng thư viện chuẩn."""
    common_ids = None
    per_criterion_len = {c: len(v) for c, v in scores.items()}
    if len(set(per_criterion_len.values())) != 1:
        logger.warning("event=score_length_mismatch lengths=%s", per_criterion_len)
    n = min(per_criterion_len.values()) if per_criterion_len.values() else 0
    if n < 2:
        print("  (không đủ dữ liệu để tính tương quan)")
        return
    aligned = {c: scores[c][:n] for c in SCORE_CRITERIA}

    header = "          " + "".join(f"{c[:10]:>12}" for c in SCORE_CRITERIA)
    print(header)
    for c1 in SCORE_CRITERIA:
        row_cells = []
        for c2 in SCORE_CRITERIA:
            r = _pearson_correlation(aligned[c1], aligned[c2])
            row_cells.append(f"{r:12.2f}")
        print(f"{c1[:10]:<10}" + "".join(row_cells))
    _ = common_ids


def _pearson_correlation(xs: list[float], ys: list[float]) -> float:
    """Hệ số tương quan Pearson, tự cài đặt (không dùng numpy)."""
    n = len(xs)
    if n == 0:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = math.sqrt(var_x * var_y)
    return covariance / denom if denom else 0.0


def build_summary_messages(article: Article) -> list[dict[str, str]]:
    """Tạo prompt sinh tóm tắt 5 gạch đầu dòng tiếng Việt."""
    system = (
        "Bạn tóm tắt tin tức công nghệ/công nghiệp bằng tiếng Việt. "
        "Trả về đúng 5 gạch đầu dòng (mỗi dòng bắt đầu bằng '- '), "
        "súc tích, không lặp tiêu đề, không thêm lời dẫn hay kết luận."
    )
    user = f"Tiêu đề: {article.title}\n\nSnippet: {article.summary}"
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def cmd_q3(args: argparse.Namespace) -> None:
    """Q3 — Sinh tóm tắt 5 bullet tiếng Việt cho từng bài, ghi ra summaries.md."""
    config = load_llm_config()
    conn = get_connection(args.db)
    articles = fetch_sample_articles(conn, args.limit)
    conn.close()
    if not articles:
        sys.exit("Chưa có bài nào trong DB. Chạy `q1` trước để fetch dữ liệu.")
    if len(articles) < args.limit:
        logger.warning("event=insufficient_articles requested=%d available=%d", args.limit, len(articles))

    ok_count = 0
    with args.summaries_out.open("w", encoding="utf-8") as md_file:
        md_file.write(f"# Spike Q3 — tóm tắt 5 bullet ({len(articles)} bài)\n\n")
        for i, article in enumerate(articles, start=1):
            messages = build_summary_messages(article)
            md_file.write(f"## {i}. {article.title}\n\n")
            md_file.write(f"Nguồn: {article.source_url}\nLink: {article.link}\n\n")
            try:
                response = call_llm(config, messages)
                content = response["choices"][0]["message"]["content"].strip()
            except httpx.HTTPError as exc:
                logger.warning("event=llm_call_failed article_id=%d error=%s", article.id, exc)
                md_file.write(f"*(lỗi gọi LLM: {exc})*\n\n---\n\n")
                continue
            md_file.write(content + "\n\n---\n\n")
            ok_count += 1
    print(f"Đã sinh tóm tắt cho {ok_count}/{len(articles)} bài vào {args.summaries_out}")


def build_arg_parser() -> argparse.ArgumentParser:
    """Xây argparse cho 3 subcommand q1/q2/q3."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Đường dẫn SQLite dùng chung")
    subparsers = parser.add_subparsers(dest="command", required=True)

    q1 = subparsers.add_parser("q1", help="Mỗi ngày thực sự có bao nhiêu bài?")
    q1.add_argument("--sources", type=str, default=None, help="Danh sách URL RSS, phân cách bởi dấu phẩy")
    q1.add_argument("--sources-file", type=Path, default=None, help="File txt, mỗi dòng một URL RSS")
    q1.set_defaults(func=cmd_q1)

    q2 = subparsers.add_parser("q2", help="LLM chấm điểm có phân biệt được không?")
    q2.add_argument("--limit", type=int, default=50, help="Số bài lấy mẫu để chấm")
    q2.add_argument("--raw-responses", type=Path, default=DEFAULT_RAW_RESPONSES_PATH, dest="raw_responses")
    q2.set_defaults(func=cmd_q2)

    q3 = subparsers.add_parser("q3", help="Tóm tắt 5 bullet từ snippet có đáng đọc không?")
    q3.add_argument("--limit", type=int, default=20, help="Số bài lấy mẫu để tóm tắt")
    q3.add_argument("--summaries-out", type=Path, default=DEFAULT_SUMMARIES_PATH, dest="summaries_out")
    q3.set_defaults(func=cmd_q3)

    return parser


def main() -> None:
    """Điểm vào CLI: `python spike/spike.py {q1,q2,q3} ...`."""
    parser = build_arg_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
