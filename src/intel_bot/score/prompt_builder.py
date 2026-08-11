"""Dựng prompt LLM — đọc template + rubric từ file, KHÔNG hardcode nội dung (§10.6, §10.7).

`prompt_version` LẤY TỪ TÊN FILE template (vd. `score_v2.0.0.md` → `score_v2.0.0`), không
phải hằng số trong code — đổi rubric = tạo file version mới, không sửa code.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from src.intel_bot.contracts.llm_score import INDUSTRY_TAGS

DEFAULT_SCORE_TEMPLATE_PATH = Path("prompts/score_v2.0.0.md")
DEFAULT_SUMMARY_TEMPLATE_PATH = Path("prompts/summary_v2.0.0.md")
DEFAULT_RUBRIC_PATH = Path("config/rubric.yaml")


@dataclass(frozen=True)
class BuiltPrompt:
    """Prompt đã dựng xong, kèm version — version lấy từ tên file template."""

    text: str
    prompt_version: str


def load_rubric_text(rubric_path: Path | str = DEFAULT_RUBRIC_PATH) -> str:
    """Đọc config/rubric.yaml, render thành văn bản mốc 1/5/10 cho từng tiêu chí."""
    raw = yaml.safe_load(Path(rubric_path).read_text(encoding="utf-8")) or {}
    criteria: dict[str, dict[str, object]] = raw.get("criteria", {})

    lines: list[str] = []
    for name, spec in criteria.items():
        label = spec.get("label", name)
        lines.append(f"### {label} ({name})")
        anchors_raw = spec.get("anchors", {})
        anchors: dict[int, str] = (
            {int(k): str(v) for k, v in anchors_raw.items()}
            if isinstance(anchors_raw, dict)
            else {}
        )
        for score in sorted(anchors):
            lines.append(f"- **{score}**: {anchors[score]}")
        lines.append("")
    return "\n".join(lines).strip()


def _prompt_version_from_path(template_path: Path) -> str:
    """Version lấy từ tên file (vd. score_v2.0.0.md -> score_v2.0.0) — không hardcode."""
    return template_path.stem


def build_score_prompt(
    *,
    title: str,
    snippet: str,
    template_path: Path | str = DEFAULT_SCORE_TEMPLATE_PATH,
    rubric_path: Path | str = DEFAULT_RUBRIC_PATH,
) -> BuiltPrompt:
    """Dựng prompt chấm điểm cho một bài — ghép rubric (config/rubric.yaml) vào template."""
    template_path = Path(template_path)
    template = template_path.read_text(encoding="utf-8")
    rubric_text = load_rubric_text(rubric_path)
    tags_text = ", ".join(sorted(INDUSTRY_TAGS))
    text = template.format(
        rubric=rubric_text, industry_tags=tags_text, title=title, snippet=snippet
    )
    return BuiltPrompt(
        text=text, prompt_version=_prompt_version_from_path(template_path)
    )


def build_summary_prompt(
    *,
    title: str,
    snippet: str,
    template_path: Path | str = DEFAULT_SUMMARY_TEMPLATE_PATH,
) -> BuiltPrompt:
    """Dựng prompt tóm tắt cho một bài."""
    template_path = Path(template_path)
    template = template_path.read_text(encoding="utf-8")
    text = template.format(title=title, snippet=snippet)
    return BuiltPrompt(
        text=text, prompt_version=_prompt_version_from_path(template_path)
    )
