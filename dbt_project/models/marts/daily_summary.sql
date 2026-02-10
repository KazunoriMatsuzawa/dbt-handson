/*
================================================================================
Fact Model: daily_summary
================================================================================

【目的】
  ビジネスユーザー向けの日別レポートテーブルです。
  日単位でのパフォーマンス分析を提供します。

【特性】
  - Materialization: TABLE（パフォーマンス重視）
  - 入力：ref('int_daily_events')
  - 出力：daily_summary テーブル

【ビジネス要件】
  - 日別に国別・プラン別の集計を保持
  - コンバージョン率、ユーザー/セッション数を含む
  - KPI指標を自動計算
  - リアルタイム分析向け

【推奨パーティション】
  event_date でパーティショニング
  Snowflake では CLUSTER BY で最適化

【出力】
  analytics.marts.daily_summary
*/

{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'daily'],
    description='日別パフォーマンスサマリー',
    unique_key=['event_date', 'country', 'plan_type', 'user_segment'],
    indexes=[
        {'columns': ['event_date']},
        {'columns': ['country', 'plan_type']}
    ]
) }}

WITH daily_events AS (
    SELECT * FROM {{ ref('int_daily_events') }}
),

calculated_metrics AS (
    -- ステップ1：KPI計算
    SELECT
        event_date,
        country,
        plan_type,
        user_segment,
        unique_users,
        unique_sessions,
        total_events,
        pageview_events,
        click_events,
        add_to_cart_events,
        checkout_events,
        purchase_events,
        acquired_users,
        converted_users,

        -- ステップ2：計算メトリクス
        ROUND(total_events::FLOAT / NULLIF(unique_users, 0), 2) AS avg_events_per_user,
        ROUND(total_events::FLOAT / NULLIF(unique_sessions, 0), 2) AS avg_events_per_session,
        ROUND(unique_sessions::FLOAT / NULLIF(unique_users, 0), 2) AS avg_sessions_per_user,
        ROUND(purchase_events::FLOAT / NULLIF(unique_users, 0), 4) AS purchase_rate,
        ROUND(converted_users::FLOAT / NULLIF(unique_users, 0), 4) AS user_conversion_rate,
        ROUND(purchase_events::FLOAT / NULLIF(total_events, 0), 4) AS purchase_event_ratio,
        ROUND(checkout_events::FLOAT / NULLIF(add_to_cart_events, 0), 4) AS checkout_rate,

        -- ステップ3：メタデータ
        CURRENT_TIMESTAMP() AS dbt_created_at,
        '{{ run_started_at }}' AS dbt_run_started_at
    FROM daily_events
)

SELECT * FROM calculated_metrics
