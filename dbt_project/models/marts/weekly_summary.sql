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
  - 出力：weekly_summary テーブル

【ビジネス要件】
  - 週単位（月曜～日曜）での集計
  - 前週比（WoW）の計算を可能にする構造
  - 国別・プラン別の比較可能
  - トレンド分析向けメトリクス

【出力】
  analytics.marts.weekly_summary
*/

{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'weekly'],
    description='週別パフォーマンスサマリー',
    unique_key=['week_start_date', 'country', 'plan_type'],
) }}

WITH daily_events AS (
    SELECT * FROM {{ ref('int_daily_events') }}
),

weekly_aggregated AS (
    -- ステップ1：日別データを週別に集計
    SELECT
        DATE_TRUNC('WEEK', event_date) AS week_start_date,
        DATEADD(day, 6, DATE_TRUNC('WEEK', event_date)) AS week_end_date,
        DATEDIFF(week, DATE_TRUNC('WEEK', '{{ var("start_date", "2025-01-01") }}'), DATE_TRUNC('WEEK', event_date)) AS week_number,
        country,
        plan_type,

        -- ステップ2：週間メトリクス集計
        COUNT(DISTINCT event_date) AS active_days,
        SUM(unique_users) AS total_weekly_users,
        SUM(unique_sessions) AS total_weekly_sessions,
        SUM(total_events) AS total_weekly_events,
        SUM(purchase_events) AS total_weekly_purchases,
        SUM(acquired_users) AS total_acquired_users,
        SUM(converted_users) AS total_converted_users,

        -- ステップ3：平均値計算
        ROUND(SUM(total_events)::FLOAT / NULLIF(SUM(unique_users), 0), 2) AS avg_events_per_user,
        ROUND(SUM(purchase_events)::FLOAT / NULLIF(SUM(unique_users), 0), 4) AS purchase_rate,

        -- ステップ4：メタデータ
        MIN(event_date) AS first_data_date,
        MAX(event_date) AS last_data_date,
        CURRENT_TIMESTAMP() AS dbt_created_at
    FROM daily_events
    GROUP BY
        DATE_TRUNC('WEEK', event_date),
        DATEADD(day, 6, DATE_TRUNC('WEEK', event_date)),
        DATEDIFF(week, DATE_TRUNC('WEEK', '{{ var("start_date", "2025-01-01") }}'), DATE_TRUNC('WEEK', event_date)),
        country,
        plan_type
),

final_metrics AS (
    -- ステップ5：前週比（WoW）計算を可能にする構造を準備
    SELECT
        week_start_date,
        week_end_date,
        week_number,
        country,
        plan_type,
        active_days,
        total_weekly_users,
        total_weekly_sessions,
        total_weekly_events,
        total_weekly_purchases,
        total_acquired_users,
        total_converted_users,
        avg_events_per_user,
        purchase_rate,
        first_data_date,
        last_data_date,
        dbt_created_at,

        -- ステップ6：Week-over-Week 計算用カラム
        LAG(total_weekly_users) OVER (PARTITION BY country, plan_type ORDER BY week_start_date) AS prev_week_users,
        LAG(total_weekly_events) OVER (PARTITION BY country, plan_type ORDER BY week_start_date) AS prev_week_events,
        LAG(total_weekly_purchases) OVER (PARTITION BY country, plan_type ORDER BY week_start_date) AS prev_week_purchases
    FROM weekly_aggregated
)

SELECT
    week_start_date,
    week_end_date,
    week_number,
    country,
    plan_type,
    active_days,
    total_weekly_users,
    total_weekly_sessions,
    total_weekly_events,
    total_weekly_purchases,
    total_acquired_users,
    total_converted_users,
    avg_events_per_user,
    purchase_rate,
    first_data_date,
    last_data_date,
    dbt_created_at,
    prev_week_users,
    prev_week_events,
    prev_week_purchases,

    -- ステップ7：WoW 変化率計算
    ROUND((total_weekly_users - NULLIF(prev_week_users, 0))::FLOAT / NULLIF(prev_week_users, 0), 4) AS wow_user_change,
    ROUND((total_weekly_events - NULLIF(prev_week_events, 0))::FLOAT / NULLIF(prev_week_events, 0), 4) AS wow_event_change,
    ROUND((total_weekly_purchases - NULLIF(prev_week_purchases, 0))::FLOAT / NULLIF(prev_week_purchases, 0), 4) AS wow_purchase_change
FROM final_metrics
