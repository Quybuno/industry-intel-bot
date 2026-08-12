"""D4 — test cho `dagster_project/`, tầng DUY NHẤT chưa có test hồi quy tự động trước D4
(0.12/0.13 chỉ verify bằng chạy `dagster dev`/`dagster asset materialize` thật, xem
docs/PROGRESS.md mục 5C "cố ý chưa làm").

Chỉ LOAD `Definitions`/resource, KHÔNG materialize asset nào — nên KHÔNG cần Postgres thật,
KHÔNG gọi LLM thật (đúng yêu cầu D4). `PostgresResource`/`LLMResource` không tự kết nối lúc
khởi tạo (chỉ khi gọi `.get_connection()`/`.build()` — xem docstring từng resource), và
`resolve_asset_graph()` chỉ phân tích tĩnh đồ thị asset đã khai báo.
"""

from __future__ import annotations

import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from dagster import AssetKey, AssetsDefinition, Failure

import dagster_project.definitions as definitions_module
from dagster_project.assets import silver
from dagster_project.partitions import daily_partitions
from dagster_project.resources.llm import LLMResource

# ---------------------------------------------------------------------------
# Đồ thị phụ thuộc mong đợi — PRODUCTION_PLAN §7.2 + 2 khoảng trống rút gọn đã ghi lại có
# chủ đích trong docs/PROGRESS.md: `articles_normalized` (mục 5C) và `article_summaries`
# đổi deps từ `article_scores` sang `fct_article_score` (D1, mục 9/10). Key TRẦN, không
# prefix schema — xem `_SourceAwareDbtTranslator` ở dagster_project/assets/dbt_assets.py.
# ---------------------------------------------------------------------------
EXPECTED_DEPS: dict[str, frozenset[str]] = {
    "raw_rss": frozenset(),
    "articles_normalized": frozenset({"raw_rss"}),
    "stg_articles": frozenset({"articles_normalized"}),
    "articles_filtered": frozenset({"stg_articles"}),
    "article_scores": frozenset({"articles_filtered"}),
    "stg_article_scores": frozenset({"article_scores"}),
    "article_summaries": frozenset({"fct_article_score"}),
    "stg_article_summaries": frozenset({"article_summaries"}),
    "seed_sources": frozenset(),
    "stg_sources": frozenset({"seed_sources"}),
    "snap_sources": frozenset({"stg_sources"}),
    "dim_source": frozenset({"snap_sources"}),
    "fct_article_score": frozenset(
        {"stg_articles", "stg_article_scores", "dim_source"}
    ),
    "mart_daily_digest": frozenset(
        {"fct_article_score", "stg_articles", "stg_article_summaries", "dim_source"}
    ),
    "mart_pipeline_health": frozenset(
        {
            "raw_rss",
            "stg_articles",
            "stg_article_scores",
            "silver/source_health",
            "silver/score_quarantine",
        }
    ),
    "published_site": frozenset({"mart_daily_digest"}),
    "silver/source_health": frozenset(),
    "silver/score_quarantine": frozenset(),
}

# 2 asset "external" KHÔNG được _SourceAwareDbtTranslator override (2 source dbt khác nhau
# không được trỏ chung asset key — DagsterInvalidDefinitionError nếu cố, xem docstring
# translator) — đây là 2 chỗ DUY NHẤT hợp lệ có dấu "/" trong asset key.
ALLOWED_SLASH_KEYS = {"silver/source_health", "silver/score_quarantine"}


@pytest.fixture(scope="module")
def asset_graph() -> Any:
    """Resolve đồ thị asset một lần cho cả file — không materialize, không cần Postgres/LLM.

    Kiểu trả về là `dagster._core.definitions.assets.graph.asset_graph.AssetGraph`, nằm
    trong module private (`_core`) của dagster nên không import để annotate — dùng `Any`."""
    return definitions_module.defs.resolve_asset_graph()


def test_definitions_load_with_expected_asset_count(asset_graph: Any) -> None:
    """`Definitions` load được (không lỗi import/circular dep) và đủ 18 asset — khớp
    docs/PROGRESS.md mục 5C/6."""
    keys = {k.to_user_string() for k in asset_graph.get_all_asset_keys()}
    assert len(keys) == 18
    assert keys == set(EXPECTED_DEPS)


def test_no_asset_key_has_stray_schema_prefix(asset_graph: Any) -> None:
    """Regression cho lỗi #2 đã gặp ở 0.12 (docs/PROGRESS.md mục 5C): dbt asset key mặc định
    có prefix schema (vd. `gold/stg_articles`) không khớp `deps=["stg_articles"]` mà asset
    Python khai — tạo node mồ côi song song với node thật, đồ thị bị chia đôi.
    `_SourceAwareDbtTranslator.get_asset_key()` sửa bằng cách bỏ prefix cho model/seed/snapshot."""
    keys = {k.to_user_string() for k in asset_graph.get_all_asset_keys()}
    stray_prefixed = {k for k in keys if "/" in k} - ALLOWED_SLASH_KEYS
    assert stray_prefixed == set(), (
        f"Asset key có prefix lạ (nghi lỗi translator 0.12 tái phát): {stray_prefixed}"
    )
    assert "gold/stg_articles" not in keys
    assert "gold/fct_article_score" not in keys


@pytest.mark.parametrize("asset_key", sorted(EXPECTED_DEPS))
def test_asset_dependency_graph_matches_expected(
    asset_graph: Any, asset_key: str
) -> None:
    """Mỗi asset có đúng tập `deps` như bảng §7.2 (+ `articles_normalized` bổ sung có chủ
    đích, + `article_summaries` đổi deps sang `fct_article_score` ở D1)."""
    ak = AssetKey.from_user_string(asset_key)
    actual = {parent.to_user_string() for parent in asset_graph.get(ak).parent_keys}
    assert actual == set(EXPECTED_DEPS[asset_key])


def test_article_scores_and_summaries_are_independent_assets_not_multi_asset() -> None:
    """D1: `article_scores`/`article_summaries` không còn là MỘT multi_asset dùng
    `internal_asset_deps` (nguồn của lỗi CheckError #4 đã gặp ở 0.12, docs/PROGRESS.md
    mục 5C — "một khi đã dùng internal_asset_deps, PHẢI liệt kê đủ mọi input cho MỌI output").
    Giờ là hai `@asset` độc lập — cấu trúc không còn khả năng mắc lại đúng lớp lỗi đó."""
    assert not hasattr(silver, "article_scores_and_summaries")
    assert isinstance(silver.article_scores, AssetsDefinition)
    assert isinstance(silver.article_summaries, AssetsDefinition)
    assert silver.article_scores.keys == {AssetKey("article_scores")}
    assert silver.article_summaries.keys == {AssetKey("article_summaries")}


def test_daily_partitions_end_offset_allows_today() -> None:
    """Lỗi #5 đã gặp ở 0.12 (docs/PROGRESS.md mục 5C): `DailyPartitionsDefinition` mặc định
    (`end_offset=0`) không coi "hôm nay" là partition hợp lệ cho tới sau nửa đêm hôm sau —
    lịch 05:00 (§7.3) cần materialize được partition hôm nay NGAY TRONG ngày đó.
    `end_offset=1` mở thêm đúng một partition đang-diễn-ra."""
    assert daily_partitions.end_offset == 1

    vn_tz = ZoneInfo("Asia/Ho_Chi_Minh")
    fixed_now = dt.datetime(
        2026, 8, 20, 9, 0, 0, tzinfo=vn_tz
    )  # mốc cố định, không phụ thuộc ngày chạy test thật
    keys = daily_partitions.get_partition_keys(current_time=fixed_now)
    assert "2026-08-20" in keys  # "hôm nay" phải có mặt, không chỉ tới hôm qua.


def test_daily_partitions_end_offset_zero_would_exclude_today() -> None:
    """Đối chứng cho test trên: xác nhận `end_offset=0` (giá trị mặc định của Dagster nếu
    không có dòng ghi đè này) THẬT SỰ loại "hôm nay" — chứng minh dòng `end_offset=1` không
    phải cấu hình thừa."""
    from dagster import DailyPartitionsDefinition

    default_offset_partitions = DailyPartitionsDefinition(
        start_date=daily_partitions.start, timezone="Asia/Ho_Chi_Minh"
    )
    vn_tz = ZoneInfo("Asia/Ho_Chi_Minh")
    fixed_now = dt.datetime(2026, 8, 20, 9, 0, 0, tzinfo=vn_tz)
    keys = default_offset_partitions.get_partition_keys(current_time=fixed_now)
    assert "2026-08-20" not in keys


def test_llm_resource_requires_provider_name_no_default() -> None:
    """Resource `llm` KHÔNG có default cho `provider_name` (khác CLI, mặc định `mock`) —
    thiếu `LLM_PROVIDER` (rỗng) phải raise `Failure` rõ ràng, KHÔNG âm thầm rơi về mock
    (P4 — bài học §5B: dữ liệu mock từng lọt vào gold vì quên đổi provider)."""
    resource = LLMResource(provider_name="")
    with pytest.raises(Failure):
        resource.build()


def test_llm_resource_unsupported_provider_name_raises_failure() -> None:
    resource = LLMResource(provider_name="not-a-real-provider")
    with pytest.raises(Failure):
        resource.build()


def test_llm_resource_deepseek_without_api_key_raises_failure_not_a_guess() -> None:
    """Không tự bịa API key — thiếu key khi `provider_name=deepseek` phải dừng rõ ràng."""
    resource = LLMResource(provider_name="deepseek", deepseek_api_key="")
    with pytest.raises(Failure):
        resource.build()


def test_llm_resource_mock_builds_without_any_env_or_network() -> None:
    """`provider_name=mock` không cần API key/Postgres/mạng — `build()` trả về ngay được."""
    resource = LLMResource(provider_name="mock")
    provider, _pricing, batch_size = resource.build()
    assert provider is not None
    assert batch_size > 0
