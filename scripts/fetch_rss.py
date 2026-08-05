#!/usr/bin/env python3
"""Fetch RSS feeds from a CSV list and store articles into SQLite."""
import argparse
import csv
import os
import sqlite3
import logging
from datetime import datetime

import feedparser


DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed TEXT,
    guid TEXT,
    title TEXT,
    link TEXT UNIQUE,
    published TEXT,
    summary TEXT,
    fetched_at TEXT
);
"""


def ensure_db(db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(DB_SCHEMA)
    conn.commit()
    return conn


def read_feeds(feeds_csv: str):
    feeds = []
    with open(feeds_csv, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = row.get('url') or row.get('feed')
            name = row.get('name') or url
            if url:
                feeds.append((name.strip(), url.strip()))
    return feeds


def parse_and_store(conn, feed_name, feed_url, limit=None):
    d = feedparser.parse(feed_url)
    if d.bozo:
        logging.warning('Parse error for %s: %s', feed_url, getattr(d, 'bozo_exception', ''))
    entries = d.entries[:limit] if limit else d.entries
    inserted = 0
    for e in entries:
        guid = e.get('id') or e.get('guid') or e.get('link')
        title = e.get('title', '')
        link = e.get('link', '')
        published = e.get('published', e.get('updated', ''))
        summary = e.get('summary', '')
        fetched_at = datetime.utcnow().isoformat()
        try:
            conn.execute(
                'INSERT OR IGNORE INTO articles (feed, guid, title, link, published, summary, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (feed_name, guid, title, link, published, summary, fetched_at),
            )
            inserted += conn.total_changes
        except Exception as ex:
            logging.exception('DB insert failed for %s: %s', link, ex)
    conn.commit()
    return inserted


def main():
    parser = argparse.ArgumentParser(description='Fetch RSS feeds and store into SQLite')
    parser.add_argument('--feeds', default='feeds.csv', help='Path to feeds CSV')
    parser.add_argument('--db', default='data/rss_articles.db', help='SQLite DB path')
    parser.add_argument('--limit', type=int, default=0, help='Limit entries per feed (0 = all)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    feeds = read_feeds(args.feeds)
    if not feeds:
        logging.error('No feeds found in %s', args.feeds)
        return

    conn = ensure_db(args.db)
    total = 0
    for name, url in feeds:
        logging.info('Fetching %s -> %s', name, url)
        try:
            added = parse_and_store(conn, name, url, limit=args.limit or None)
            logging.info('Inserted %d items from %s', added, name)
            total += added
        except Exception as e:
            logging.exception('Failed to fetch %s: %s', url, e)

    logging.info('Done. Total inserted: %d', total)


if __name__ == '__main__':
    main()
