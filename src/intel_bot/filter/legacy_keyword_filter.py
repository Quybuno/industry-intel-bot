"""Filter theo industry group (v1) — CHỈ để `jobs/filter_job.py` (ORM cũ, không qua CLI
nữa) còn import được. Task 0.6 thay bằng blocklist thuần trong `keyword_filter.py`.
"""

from __future__ import annotations

import re
from typing import Any


def load_keyword_groups(keywords_data: dict[str, Any]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = keywords_data.get("groups", keywords_data)
    return result


def _normalize_industries(industries: list[Any] | None) -> list[str]:
    if not industries:
        return []
    return [str(i).lower() for i in industries]


def match_keyword_groups(text: str, keyword_groups: dict[str, list[str]]) -> list[str]:
    """Return list of matched group names (unique, stable order)."""
    if not text:
        return []
    text_lower = text.lower()
    matched: list[str] = []
    for group, keywords in keyword_groups.items():
        for kw in keywords:
            pattern = r"\b" + re.escape(kw.lower()) + r"\b"
            if re.search(pattern, text_lower):
                matched.append(group)
                break
    return matched


def keyword_pass(
    text: str,
    source_industries: list[str],
    keyword_groups: dict[str, list[str]],
) -> tuple[bool, list[str], str | None]:
    """
    Pass if: match >= 1 keyword in >= 1 group
    AND (source industry overlaps matched group OR matched >= 2 groups).

    Returns (passed, matched_groups, rejection_reason).
    """
    matched = match_keyword_groups(text, keyword_groups)
    if not matched:
        return False, [], "keyword_miss"

    source_inds = _normalize_industries(source_industries)
    source_overlap = any(g in source_inds for g in matched)
    if source_overlap or len(matched) >= 2:
        return True, matched, None

    return False, matched, "keyword_miss"
