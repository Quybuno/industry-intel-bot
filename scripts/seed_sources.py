#!/usr/bin/env python3
"""Seed `sources` table from config/sources.yaml"""
import sys
from pathlib import Path

# ensure project root on sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.intel_bot.config import load_yaml
from src.intel_bot.db.session import get_session
from src.intel_bot.db.models import Source


def seed(config_path: str = 'config/sources.yaml'):
    data = load_yaml(config_path)
    sources = data.get('sources', [])
    with get_session() as sess:
        for s in sources:
            existing = sess.get(Source, s.get('id'))
            if existing:
                continue
            obj = Source(
                id=s.get('id'),
                type=s.get('type'),
                url_or_query=s.get('url_or_query'),
                tier=s.get('tier'),
                industries=s.get('industries'),
                enabled=s.get('enabled', True),
            )
            sess.add(obj)
    print(f'Seeded {len(sources)} sources')


if __name__ == '__main__':
    seed()
