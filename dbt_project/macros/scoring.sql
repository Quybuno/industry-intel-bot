{# Công thức điểm §5.7 — TOÀN BỘ số nằm ở dbt_project.yml `vars:`, không hardcode ở đây. #}

{% macro source_tier_score(tier_column) %}
    {#- Ánh xạ tier nguồn (1/2/3) -> điểm credibility trên thang 1-10, theo
       var('source_tier_score_map'). Tier không khớp map nào (nguồn chưa có trong
       dim_source, LEFT JOIN không match) -> 0, không suy diễn hộ (P4 tinh thần: rõ ràng
       hơn là che giấu bằng một giá trị mặc định lạc quan). #}
    case {{ tier_column }}
        {%- for tier, score in var('source_tier_score_map').items() %}
        when {{ tier }} then {{ score }}
        {%- endfor %}
        else 0
    end
{% endmacro %}

{% macro credibility_blended(source_tier_score_expr, llm_credibility_expr) %}
    {#- §5.7 "Sửa lỗi 4": 80% source tier (tín hiệu cứng) / 20% điểm LLM thô. #}
    ({{ source_tier_score_expr }} * {{ var('credibility_source_tier_weight') }}
        + {{ llm_credibility_expr }} * {{ var('credibility_llm_weight') }})
{% endmacro %}

{% macro recency_boost(published_at_expr, first_seen_at_expr, imputed_expr) %}
    {#- §5.7: published_at (NULL thì first_seen_at) trong 12h -> +1.0; 24h -> +0.5; còn lại
       0. published_at_imputed=true (không có published_at gốc từ feed) -> trừ thêm phạt vì
       mốc thời gian là suy luận. "now" = current_timestamp tại thời điểm dbt run — recency
       phải tính lại mỗi lần build digest, không đóng băng theo thời điểm chấm điểm. #}
    (
        case
            when coalesce({{ published_at_expr }}, {{ first_seen_at_expr }})
                >= (current_timestamp - interval '{{ var("recency_window_full_hours") }} hours')
                then {{ var('recency_boost_full') }}
            when coalesce({{ published_at_expr }}, {{ first_seen_at_expr }})
                >= (current_timestamp - interval '{{ var("recency_window_half_hours") }} hours')
                then {{ var('recency_boost_half') }}
            else {{ var('recency_boost_none') }}
        end
        - case when {{ imputed_expr }} then {{ var('recency_imputed_penalty') }} else 0 end
    )
{% endmacro %}

{% macro composite_score(importance_expr, practicality_expr, credibility_blended_expr, depth_expr, recency_boost_expr) %}
    {#- composite = importance*0.40 + practicality*0.30 + credibility_blended*0.30
       + depth*0.00 + recency_boost (§5.7). depth vẫn nhân trọng số (0 theo var) để đổi lại
       chỉ cần sửa dbt_project.yml, không sửa SQL, nếu Phase sau bật lại depth. #}
    (
        {{ importance_expr }} * {{ var('composite_weight_importance') }}
        + {{ practicality_expr }} * {{ var('composite_weight_practicality') }}
        + {{ credibility_blended_expr }} * {{ var('composite_weight_credibility') }}
        + {{ depth_expr }} * {{ var('composite_weight_depth') }}
        + {{ recency_boost_expr }}
    )
{% endmacro %}
