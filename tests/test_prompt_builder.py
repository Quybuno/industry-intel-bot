"""Unit test cho prompt_builder.py — đọc file thật trong repo, không cần env var."""

from __future__ import annotations

from src.intel_bot.score.prompt_builder import (
    build_score_prompt,
    build_summary_prompt,
    load_rubric_text,
)


def test_score_prompt_version_comes_from_filename() -> None:
    built = build_score_prompt(title="T", snippet="S")
    assert built.prompt_version == "score_v2.0.0"


def test_summary_prompt_version_comes_from_filename() -> None:
    built = build_summary_prompt(title="T", snippet="S")
    assert built.prompt_version == "summary_v2.0.0"


def test_score_prompt_contains_title_and_snippet() -> None:
    built = build_score_prompt(
        title="Tiêu đề đặc biệt XYZ", snippet="Snippet đặc biệt ABC"
    )
    assert "Tiêu đề đặc biệt XYZ" in built.text
    assert "Snippet đặc biệt ABC" in built.text


def test_score_prompt_contains_rubric_and_closed_tag_list() -> None:
    built = build_score_prompt(title="T", snippet="S")
    assert "credibility" in built.text
    assert "ai, construction, hvac, iot, manufacturing" in built.text


def test_score_prompt_contains_anti_clustering_constraint() -> None:
    built = build_score_prompt(title="T", snippet="S")
    assert "30%" in built.text


def test_summary_prompt_contains_title_and_snippet() -> None:
    built = build_summary_prompt(title="Tiêu đề riêng", snippet="Snippet riêng")
    assert "Tiêu đề riêng" in built.text
    assert "Snippet riêng" in built.text


def test_load_rubric_text_contains_all_four_criteria() -> None:
    text = load_rubric_text()
    for criterion in ("credibility", "importance", "depth", "practicality"):
        assert criterion in text


def test_load_rubric_text_contains_1_5_10_anchors() -> None:
    text = load_rubric_text()
    assert "**1**" in text
    assert "**5**" in text
    assert "**10**" in text
