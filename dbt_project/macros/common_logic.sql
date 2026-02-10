/*
================================================================================
dbt Macros：共通ロジック集
================================================================================

【目的】
  複数のモデル間で再利用される SQL ロジックをマクロとして定義します。
  DRY（Don't Repeat Yourself）原則に基づいて、コード重複を削減します。

【マクロの利点】
  1. コード再利用
  2. 保守性向上
  3. 統一性の確保
  4. テンプレート化

【使用方法】
  {{ macro_name(arg1, arg2) }}
  でモデル内から呼び出します。
*/


-- =====================================================================
-- Macro 1: funnel_stage_generator
-- =====================================================================
-- 【目的】イベント種別からファネルステージを生成
-- 【利用】stg_events.sql で使用

{% macro funnel_stage_generator(event_type_col) %}
    CASE
        WHEN {{ event_type_col }} = 'PAGE_VIEW' THEN 'Engagement'
        WHEN {{ event_type_col }} IN ('CLICK', 'ADD_TO_CART') THEN 'Consideration'
        WHEN {{ event_type_col }} IN ('CHECKOUT', 'PURCHASE') THEN 'Conversion'
        WHEN {{ event_type_col }} = 'SIGN_UP' THEN 'Acquisition'
        ELSE 'Other'
    END
{% endmacro %}


-- =====================================================================
-- Macro 2: user_segment_generator
-- =====================================================================
-- 【目的】プランタイプとアクティブフラグからセグメント生成
-- 【利用】stg_users.sql で使用

{% macro user_segment_generator(plan_type_col, is_active_col) %}
    CASE
        WHEN {{ plan_type_col }} = 'premium' AND {{ is_active_col }} = TRUE THEN 'Premium Active'
        WHEN {{ plan_type_col }} = 'premium' AND {{ is_active_col }} = FALSE THEN 'Premium Inactive'
        WHEN {{ plan_type_col }} = 'free' AND {{ is_active_col }} = TRUE THEN 'Free Active'
        ELSE 'Free Inactive'
    END
{% endmacro %}


-- =====================================================================
-- Macro 3: cohort_generator
-- =====================================================================
-- 【目的】登録日からコホートを生成（新規、成長期等）
-- 【利用】stg_users.sql で使用

{% macro cohort_generator(signup_date_col) %}
    CASE
        WHEN DATEDIFF(day, {{ signup_date_col }}, CURRENT_DATE()) <= 7 THEN 'New'
        WHEN DATEDIFF(day, {{ signup_date_col }}, CURRENT_DATE()) <= 30 THEN 'Onboarding'
        WHEN DATEDIFF(day, {{ signup_date_col }}, CURRENT_DATE()) <= 365 THEN 'Established'
        ELSE 'Long-term'
    END
{% endmacro %}


-- =====================================================================
-- Macro 4: event_count_by_type
-- =====================================================================
-- 【目的】イベント種別ごとの件数を計算（CASE+COUNT パターン）
-- 【利用】int_daily_events.sql で使用

{% macro event_count_by_type(event_type_col, event_id_col) %}
    COUNT(DISTINCT CASE WHEN {{ event_type_col }} = 'PAGE_VIEW' THEN {{ event_id_col }} END) AS pageview_events,
    COUNT(DISTINCT CASE WHEN {{ event_type_col }} = 'CLICK' THEN {{ event_id_col }} END) AS click_events,
    COUNT(DISTINCT CASE WHEN {{ event_type_col }} = 'ADD_TO_CART' THEN {{ event_id_col }} END) AS add_to_cart_events,
    COUNT(DISTINCT CASE WHEN {{ event_type_col }} = 'CHECKOUT' THEN {{ event_id_col }} END) AS checkout_events,
    COUNT(DISTINCT CASE WHEN {{ event_type_col }} = 'PURCHASE' THEN {{ event_id_col }} END) AS purchase_events
{% endmacro %}


-- =====================================================================
-- Macro 5: conversion_rate_calculator
-- =====================================================================
-- 【目的】コンバージョンレートを安全に計算（0除算回避）
-- 【利用】daily_summary.sql や週単位の集計で使用

{% macro conversion_rate_calculator(numerator, denominator, decimal_places=4) %}
    ROUND({{ numerator }}::FLOAT / NULLIF({{ denominator }}, 0), {{ decimal_places }})
{% endmacro %}


-- =====================================================================
-- Macro 6: wow_change_calculator
-- =====================================================================
-- 【目的】前週比（WoW）の変化率を計算
-- 【利用】weekly_summary.sql で使用

{% macro wow_change_calculator(current_value, previous_value, decimal_places=4) %}
    ROUND(({{ current_value }} - NULLIF({{ previous_value }}, 0))::FLOAT / NULLIF({{ previous_value }}, 0), {{ decimal_places }})
{% endmacro %}


-- =====================================================================
-- Macro 7: get_date_range
-- =====================================================================
-- 【目的】分析期間を取得（デフォルト: 過去30日）
-- 【利用】モデル内でのフィルタリングに使用

{% macro get_date_range(lookback_days=30) %}
    DATE_RANGE: DATEADD(day, -{{ lookback_days }}, CURRENT_DATE())
    TO CURRENT_DATE()
{% endmacro %}


-- =====================================================================
-- Macro 8: null_safe_average
-- =====================================================================
-- 【目的】NULL を考慮した平均値計算
-- 【利用】複数メトリクスの平均値計算

{% macro null_safe_average(values_col, decimal_places=2) %}
    ROUND(AVG(NULLIF({{ values_col }}, 0)), {{ decimal_places }})
{% endmacro %}


-- =====================================================================
-- Macro 9: source_freshness_check
-- =====================================================================
-- 【目的】ソーステーブルの新鮮性チェック
-- 【利用】dbt tests で実行

{% macro source_freshness_check(source_name, table_name, max_hours=24) %}
    {%- if execute -%}
        {%- set last_updated = run_query(
            "SELECT MAX(created_at) FROM " ~ source_name ~ "." ~ table_name
        ).columns[0][0] -%}

        {%- if last_updated is none or last_updated < (now() - max_hours * 3600) -%}
            {{ exceptions.warn("Source " ~ table_name ~ " has not been updated in the last " ~ max_hours ~ " hours") }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}


-- =====================================================================
-- Macro 10: create_snowflake_tags
-- =====================================================================
-- 【目的】Snowflake のタグ管理（メタデータ）
-- 【利用】モデルのドキュメンテーション

{% macro create_snowflake_tags() %}
    {%- if execute -%}
        CREATE TAG IF NOT EXISTS BUSINESS_CRITICAL;
        CREATE TAG IF NOT EXISTS PII;
        CREATE TAG IF NOT EXISTS SENSITIVE;
    {%- endif -%}
{% endmacro %}


-- =====================================================================
-- 使用例
-- =====================================================================

/*
モデル内での使用例：

{{ config(
    materialized='table'
) }}

WITH events AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_timestamp,
        {{ funnel_stage_generator('event_type') }} AS funnel_stage
    FROM {{ ref('stg_events') }}
),

users AS (
    SELECT
        user_id,
        plan_type,
        is_active,
        signup_date,
        {{ user_segment_generator('plan_type', 'is_active') }} AS user_segment,
        {{ cohort_generator('signup_date') }} AS cohort
    FROM {{ ref('stg_users') }}
),

aggregated AS (
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(event_id) AS total_events,
        {{ event_count_by_type('event_type', 'event_id') }},
        {{ conversion_rate_calculator('purchase_events', 'unique_users') }} AS purchase_rate
    FROM events
    GROUP BY event_date
)

SELECT * FROM aggregated
*/
