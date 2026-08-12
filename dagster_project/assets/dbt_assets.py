"""Nạp `dbt_project/` thành asset Dagster (task 0.12 mục 3, PRODUCTION_PLAN §7.2, §11).

**Lệch khỏi chữ đúng của đề bài — cần ghi rõ:** đề bài nói nạp bằng
`load_assets_from_dbt_project`. Hàm đó ĐÃ BỊ GỠ khỏi `dagster-dbt` hiện hành (bản mới nhất
cài được lúc làm task này: `dagster==1.13.17`, `dagster-dbt==0.29.17`) — verify bằng
`from dagster_dbt import load_assets_from_dbt_project` → `ImportError`, không đoán. API
thay thế CHÍNH THỨC của dagster-dbt (không phải lựa chọn tự nghĩ ra) là decorator
`@dbt_assets` + `DbtProject` + `DbtCliResource`, dùng ở dưới đây. KHÔNG viết lại logic dbt
nào trong Python — mọi lệnh thật đều là `dbt build` (thứ tự transform vẫn 100% do dbt/SQL
quyết định), phần Python chỉ gọi CLI dbt và inject `--vars run_date` cho asset có partition.

**Chia làm 2 nhóm `@dbt_assets`** vì bảng asset gốc §7.2 có asset dbt "daily" (stg_articles,
fct_article_score, mart_pipeline_health) VÀ asset dbt không partition (dim_source,
mart_daily_digest) trong CÙNG một dbt project — `@dbt_assets` áp một `partitions_def` cho
toàn bộ node được `select`, nên phải gọi decorator 2 lần với `select` khác nhau.

**Không có `from __future__ import annotations`** — xem giải thích ở `assets/bronze.py`.
"""

import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, ClassVar

from dagster import (
    AssetCheckEvaluation,
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    Output,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_project.partitions import daily_partitions

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt_project"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(project_dir=dbt_project)

#: Node dbt có ý nghĩa theo-ngày (đọc/ghi cột first_seen_date/pipeline_date, hỗ trợ
#: `--vars run_date`) — khớp cột "Partition: daily" ở bảng §7.2.
_DAILY_DBT_SELECT = (
    "stg_articles stg_article_scores stg_article_summaries "
    "int_articles_deduped int_scores_latest int_summaries_latest "
    "fct_article_score mart_pipeline_health"
)
#: Node dbt không gắn với một ngày cụ thể (SCD2, seed cấu hình, snapshot, rolling-window
#: digest) — khớp cột "Partition: —" ở bảng §7.2.
_UNPARTITIONED_DBT_SELECT = (
    "seed_sources snap_sources stg_sources dim_source mart_daily_digest"
)


class _SourceAwareDbtTranslator(DagsterDbtTranslator):
    """Ánh xạ `source()` trong dbt sang ĐÚNG asset Python đã ghi bảng đó, để đồ thị Dagster
    có cạnh phụ thuộc thật xuyên Python/dbt (DONE WHEN: "đồ thị hiển thị đủ các asset và
    quan hệ phụ thuộc") — nếu không override, các source() này biến thành asset "external"
    rời rạc, không nối được với raw_rss/article_scores/article_summaries đã có.

    **Chỉ ánh xạ 1-1** (mỗi asset Python nhận đúng MỘT source dbt): dagster-dbt bắt buộc mỗi
    dbt resource (kể cả source) phải có asset key riêng, không cho hai source khác nhau
    cùng trỏ một key (đã tự verify — import lỗi `DagsterInvalidDefinitionError` khi thử ánh
    xạ `score_quarantine`/`source_health` chung key với `article_scores`/`raw_rss`, dù cả
    hai bảng đó thật sự do đúng asset kia ghi). `score_quarantine`/`source_health` vì vậy
    KHÔNG override — dagster-dbt tự coi chúng là asset "external" riêng, vẫn hiện trên đồ
    thị (mart_pipeline_health vẫn nối được tới chúng), chỉ là không gộp chung định danh với
    raw_rss/article_scores.

    **Model/seed/snapshot lấy asset key TRẦN (không prefix schema).** Mặc định dagster-dbt
    ghép `[schema, name]` (mọi model ở đây cùng ghi schema `gold`, xem dbt_project.yml) —
    ra `AssetKey(["gold", "stg_articles"])`, hiển thị `gold/stg_articles`, KHÔNG khớp
    `deps=["stg_articles"]` mà các asset Python ở `silver.py`/`serve.py` khai (đã tự verify:
    lúc chưa override, đồ thị có cả `stg_articles` mồ côi lẫn `gold/stg_articles` thật —
    hai node rời nhau, chính là lỗi đồ thị DONE WHEN yêu cầu tránh). Override để asset key
    luôn là tên model trần, khớp toàn bộ cách đặt tên phẳng đã dùng xuyên suốt task 0.12.
    """

    _SOURCE_TABLE_TO_ASSET_KEY: ClassVar[dict[tuple[str, str], str]] = {
        ("bronze", "raw_articles"): "raw_rss",
        ("silver", "articles"): "articles_normalized",
        ("silver", "article_scores"): "article_scores",
        ("silver", "article_summaries"): "article_summaries",
    }

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            table_key = (
                dbt_resource_props["source_name"],
                dbt_resource_props["name"],
            )
            mapped = self._SOURCE_TABLE_TO_ASSET_KEY.get(table_key)
            if mapped is not None:
                return AssetKey(mapped)
            return super().get_asset_key(dbt_resource_props)
        # model / seed / snapshot: luôn tên trần, bỏ qua schema.
        return AssetKey(dbt_resource_props["name"])


_translator = _SourceAwareDbtTranslator()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    select=_DAILY_DBT_SELECT,
    partitions_def=daily_partitions,
    dagster_dbt_translator=_translator,
)
def daily_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[
    Output[Any]
    | AssetMaterialization
    | AssetObservation
    | AssetCheckResult
    | AssetCheckEvaluation
]:
    """`dbt build --vars '{"run_date": "<partition_key>"}'` — chỉ `fct_article_score` và
    `mart_pipeline_health` thật sự đọc `run_date` (§11.3); các model còn lại trong nhóm này
    bỏ qua var không dùng tới (dbt không lỗi vì var thừa)."""
    run_date_vars = json.dumps({"run_date": context.partition_key})
    yield from dbt.cli(["build", "--vars", run_date_vars], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    select=_UNPARTITIONED_DBT_SELECT,
    dagster_dbt_translator=_translator,
)
def snapshot_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[
    Output[Any]
    | AssetMaterialization
    | AssetObservation
    | AssetCheckResult
    | AssetCheckEvaluation
]:
    """Không partition — SCD2 (`dim_source`) và digest cửa sổ-lăn (`mart_daily_digest`)
    luôn phản ánh trạng thái HIỆN TẠI, không phải một ngày cụ thể trong quá khứ (§5.6, §5.8)."""
    yield from dbt.cli(["build"], context=context).stream()
