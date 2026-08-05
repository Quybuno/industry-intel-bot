#!/usr/bin/env python3
"""Initialize database tables."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.intel_bot.db.session import ensure_tables


def main():
    ensure_tables()
    print('Database tables created.')


if __name__ == '__main__':
    main()
