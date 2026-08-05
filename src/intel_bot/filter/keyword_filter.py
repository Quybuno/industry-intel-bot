"""Stage 2a — keyword filter against industry groups."""
from __future__ import annotations

import re
from typing import Optional


def load_keyword_groups(keywords_data: dict) -> dict[str, list[str]]:
    return keywords_data.get('groups', keywords_data)


def _normalize_industries(industries: Optional[list]) -> list[str]:
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
            pattern = r'\b' + re.escape(kw.lower()) + r'\b'
            if re.search(pattern, text_lower):
                matched.append(group)
                break
    return matched


def keyword_pass(
    text: str,
    source_industries: list[str],
    keyword_groups: dict[str, list[str]],
) -> tuple[bool, list[str], Optional[str]]:
    """
    Pass if: match >= 1 keyword in >= 1 group
    AND (source industry overlaps matched group OR matched >= 2 groups).

    Returns (passed, matched_groups, rejection_reason).
    """
    matched = match_keyword_groups(text, keyword_groups)
    if not matched:
        return False, [], 'keyword_miss'

    source_inds = _normalize_industries(source_industries)
    source_overlap = any(g in source_inds for g in matched)
    if source_overlap or len(matched) >= 2:
        return True, matched, None

    return False, matched, 'keyword_miss'
