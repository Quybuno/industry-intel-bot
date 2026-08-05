#!/usr/bin/env python3
"""Quick view of ingested data in the database."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import sqlite3
import os

db_url = os.getenv('DATABASE_URL', 'sqlite:///data/dev.db')
db_path = db_url.replace('sqlite:///', '')
if not Path(db_path).is_absolute():
    db_path = str(ROOT / db_path)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

print(f'Database: {db_path}\n')

print('=== TABLES ===')
for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    name = row[0]
    count = conn.execute(f'SELECT COUNT(*) FROM [{name}]').fetchone()[0]
    print(f'  {name}: {count} rows')

print('\n=== articles (latest 5) ===')
for r in conn.execute(
    '''
    SELECT title, source_id, source_type, status, canonical_url, first_seen_at
    FROM articles ORDER BY first_seen_at DESC LIMIT 5
    '''
):
    title = (r['title'] or '')[:70]
    print(f"- [{r['source_id']}] {title}")
    print(f"  url: {r['canonical_url']}")
    print(f"  type={r['source_type']} status={r['status']} seen={r['first_seen_at']}\n")

print('=== job_runs (latest) ===')
for r in conn.execute(
    'SELECT job_name, status, items_processed, items_failed, started_at FROM job_runs ORDER BY started_at DESC LIMIT 3'
):
    print(dict(r))

print('\n=== source_health (failed sources) ===')
for r in conn.execute(
    'SELECT source_id, consecutive_failures, last_error FROM source_health WHERE consecutive_failures > 0'
):
    print(dict(r))
