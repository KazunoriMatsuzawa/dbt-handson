/*
================================================================================
Fact Model: weekly_summary
================================================================================

【目的】
  週単位のパフォーマンス分析テーブルです。
  トレンド分析や長期的なパターン認識に使用されます。

【特性】
  - Materialization: TABLE
  - 入力：ref('int_daily_events')（日別集計を週単位に集計）
  - 出力：WEEKLY_SUMMARY テーブル

【ビジネス要件】
  - 週単位（月曜～日曜）での集計
  - 前週比（WoW）の計算を可能にする構造
  - 国別・プラン別の比較可能
  - トレンド分析向けメトリクス

【出力】
  ANALYTICS.MARTS.WEEKLY_SUMMARY
*/

{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'weekly'],
    description='週別パフォーマンスサマリー',
    unique_key=['WEEK_START_DATE', 'COUNTRY', 'PLAN_TYPE'],
) }}

WITH daily_events AS (
    SELECT * FROM {{ ref('int_daily_events') }}
),

weekly_aggregated AS (
    -- ステップ1：日別データを週別に集計
    SELECT
        DATE_TRUNC('WEEK', EVENT_DATE) AS WEEK_START_DATE,
        DATEADD(day, 6, DATE_TRUNC('WEEK', EVENT_DATE)) AS WEEK_END_DATE,
        DATEDIFF(week, DATE_TRUNC('WEEK', '{{ var("start_date", "2025-01-01") }}'), DATE_TRUNC('WEEK', EVENT_DATE)) AS WEEK_NUMBER,
        COUNTRY,
        PLAN_TYPE,

        -- ステップ2：週間メトリクス集計
        COUNT(DISTINCT EVENT_DATE) AS ACTIVE_DAYS,
        SUM(UNIQUE_USERS) AS TOTAL_WEEKLY_USERS,
        SUM(UNIQUE_SESSIONS) AS TOTAL_WEEKLY_SESSIONS,
        SUM(TOTAL_EVENTS) AS TOTAL_WEEKLY_EVENTS,
        SUM(PURCHASE_EVENTS) AS TOTAL_WEEKLY_PURCHASES,
        SUM(ACQUIRED_USERS) AS TOTAL_ACQUIRED_USERS,
        SUM(CONVERTED_USERS) AS TOTAL_CONVERTED_USERS,

        -- ステップ3：平均値計算
        ROUND(SUM(TOTAL_EVENTS)::FLOAT / NULLIF(SUM(UNIQUE_USERS), 0), 2) AS AVG_EVENTS_PER_USER,
        ROUND(SUM(PURCHASE_EVENTS)::FLOAT / NULLIF(SUM(UNIQUE_USERS), 0), 4) AS PURCHASE_RATE,

        -- ステップ4：メタデータ
        MIN(EVENT_DATE) AS FIRST_DATA_DATE,
        MAX(EVENT_DATE) AS LAST_DATA_DATE,
        CURRENT_TIMESTAMP() AS DBT_CREATED_AT
    FROM daily_events
    GROUP BY
        DATE_TRUNC('WEEK', EVENT_DATE),
        DATEADD(day, 6, DATE_TRUNC('WEEK', EVENT_DATE)),
        DATEDIFF(week, DATE_TRUNC('WEEK', '{{ var("start_date", "2025-01-01") }}'), DATE_TRUNC('WEEK', EVENT_DATE)),
        COUNTRY,
        PLAN_TYPE
),

final_metrics AS (
    -- ステップ5：前週比（WoW）計算を可能にする構造を準備
    SELECT
        WEEK_START_DATE,
        WEEK_END_DATE,
        WEEK_NUMBER,
        COUNTRY,
        PLAN_TYPE,
        ACTIVE_DAYS,
        TOTAL_WEEKLY_USERS,
        TOTAL_WEEKLY_SESSIONS,
        TOTAL_WEEKLY_EVENTS,
        TOTAL_WEEKLY_PURCHASES,
        TOTAL_ACQUIRED_USERS,
        TOTAL_CONVERTED_USERS,
        AVG_EVENTS_PER_USER,
        PURCHASE_RATE,
        FIRST_DATA_DATE,
        LAST_DATA_DATE,
        DBT_CREATED_AT,

        -- ステップ6：Week-over-Week 計算用カラム
        LAG(TOTAL_WEEKLY_USERS) OVER (PARTITION BY COUNTRY, PLAN_TYPE ORDER BY WEEK_START_DATE) AS PREV_WEEK_USERS,
        LAG(TOTAL_WEEKLY_EVENTS) OVER (PARTITION BY COUNTRY, PLAN_TYPE ORDER BY WEEK_START_DATE) AS PREV_WEEK_EVENTS,
        LAG(TOTAL_WEEKLY_PURCHASES) OVER (PARTITION BY COUNTRY, PLAN_TYPE ORDER BY WEEK_START_DATE) AS PREV_WEEK_PURCHASES
    FROM weekly_aggregated
)

SELECT
    WEEK_START_DATE,
    WEEK_END_DATE,
    WEEK_NUMBER,
    COUNTRY,
    PLAN_TYPE,
    ACTIVE_DAYS,
    TOTAL_WEEKLY_USERS,
    TOTAL_WEEKLY_SESSIONS,
    TOTAL_WEEKLY_EVENTS,
    TOTAL_WEEKLY_PURCHASES,
    TOTAL_ACQUIRED_USERS,
    TOTAL_CONVERTED_USERS,
    AVG_EVENTS_PER_USER,
    PURCHASE_RATE,
    FIRST_DATA_DATE,
    LAST_DATA_DATE,
    DBT_CREATED_AT,
    PREV_WEEK_USERS,
    PREV_WEEK_EVENTS,
    PREV_WEEK_PURCHASES,

    -- ステップ7：WoW 変化率計算
    ROUND((TOTAL_WEEKLY_USERS - NULLIF(PREV_WEEK_USERS, 0))::FLOAT / NULLIF(PREV_WEEK_USERS, 0), 4) AS WOW_USER_CHANGE,
    ROUND((TOTAL_WEEKLY_EVENTS - NULLIF(PREV_WEEK_EVENTS, 0))::FLOAT / NULLIF(PREV_WEEK_EVENTS, 0), 4) AS WOW_EVENT_CHANGE,
    ROUND((TOTAL_WEEKLY_PURCHASES - NULLIF(PREV_WEEK_PURCHASES, 0))::FLOAT / NULLIF(PREV_WEEK_PURCHASES, 0), 4) AS WOW_PURCHASE_CHANGE
FROM final_metrics
