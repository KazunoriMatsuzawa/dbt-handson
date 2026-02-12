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
  - 出力：DAILY_SUMMARY テーブル

【ビジネス要件】
  - 日別に国別・プラン別の集計を保持
  - コンバージョン率、ユーザー/セッション数を含む
  - KPI指標を自動計算
  - リアルタイム分析向け

【推奨パーティション】
  EVENT_DATE でパーティショニング
  Snowflake では CLUSTER BY で最適化

【出力】
  DIESELPJ_TEST.DBT_HANDSON_MARTS.DAILY_SUMMARY
*/

{{ config(
    materialized='table',
    schema='marts',
    tags=['marts', 'daily'],
    description='日別パフォーマンスサマリー',
    unique_key=['EVENT_DATE', 'COUNTRY', 'PLAN_TYPE', 'USER_SEGMENT'],
    indexes=[
        {'columns': ['EVENT_DATE']},
        {'columns': ['COUNTRY', 'PLAN_TYPE']}
    ]
) }}

WITH daily_events AS (
    SELECT * FROM {{ ref('int_daily_events') }}
),

calculated_metrics AS (
    -- ステップ1：KPI計算
    SELECT
        EVENT_DATE,
        COUNTRY,
        PLAN_TYPE,
        USER_SEGMENT,
        UNIQUE_USERS,
        UNIQUE_SESSIONS,
        TOTAL_EVENTS,
        PAGEVIEW_EVENTS,
        CLICK_EVENTS,
        ADD_TO_CART_EVENTS,
        CHECKOUT_EVENTS,
        PURCHASE_EVENTS,
        ACQUIRED_USERS,
        CONVERTED_USERS,

        -- ステップ2：計算メトリクス
        ROUND(TOTAL_EVENTS::FLOAT / NULLIF(UNIQUE_USERS, 0), 2) AS AVG_EVENTS_PER_USER,
        ROUND(TOTAL_EVENTS::FLOAT / NULLIF(UNIQUE_SESSIONS, 0), 2) AS AVG_EVENTS_PER_SESSION,
        ROUND(UNIQUE_SESSIONS::FLOAT / NULLIF(UNIQUE_USERS, 0), 2) AS AVG_SESSIONS_PER_USER,
        ROUND(PURCHASE_EVENTS::FLOAT / NULLIF(UNIQUE_USERS, 0), 4) AS PURCHASE_RATE,
        ROUND(CONVERTED_USERS::FLOAT / NULLIF(UNIQUE_USERS, 0), 4) AS USER_CONVERSION_RATE,
        ROUND(PURCHASE_EVENTS::FLOAT / NULLIF(TOTAL_EVENTS, 0), 4) AS PURCHASE_EVENT_RATIO,
        ROUND(CHECKOUT_EVENTS::FLOAT / NULLIF(ADD_TO_CART_EVENTS, 0), 4) AS CHECKOUT_RATE,

        -- ステップ3：メタデータ
        CURRENT_TIMESTAMP() AS DBT_CREATED_AT,
        '{{ run_started_at }}' AS DBT_RUN_STARTED_AT
    FROM daily_events
)

SELECT * FROM calculated_metrics
