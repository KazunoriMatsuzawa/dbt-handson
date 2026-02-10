/*
================================================================================
Intermediate Model: int_daily_events
================================================================================

【目的】
  staging層の stg_events と stg_users を結合して、
  日別・国別のイベント集計を作成します。

【特性】
  - Materialization: VIEW
  - 入力： ref('stg_events'), ref('stg_users')
  - 出力： 日付・国別の集計データ

【ビジネスロジック】
  - イベント種別ごとの件数計算
  - コンバージョン漏斗の段階別集計
  - ユーザー・セッションの集計

【出力】
  ref('int_daily_events') で参照可能
  最終的には fct_daily_summary で利用
*/

{{ config(
    materialized='view',
    tags=['intermediate', 'events'],
    description='日別・国別イベント集計（中間モデル）'
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

joined_events AS (
    -- ステップ1：イベント + ユーザー属性を結合
    SELECT
        e.event_date,
        e.event_hour,
        e.event_type,
        e.funnel_stage,
        e.device_type,
        u.country,
        u.plan_type,
        u.user_segment,
        u.cohort,
        e.user_id,
        e.session_id,
        e.event_timestamp,
        e.event_id
    FROM events e
    INNER JOIN users u
        ON e.user_id = u.user_id
),

daily_aggregated AS (
    -- ステップ2：日別・国別・プラン別に集計
    SELECT
        event_date,
        country,
        plan_type,
        user_segment,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT session_id) AS unique_sessions,
        COUNT(event_id) AS total_events,
        COUNT(DISTINCT CASE WHEN event_type = 'PAGE_VIEW' THEN event_id END) AS pageview_events,
        COUNT(DISTINCT CASE WHEN event_type = 'CLICK' THEN event_id END) AS click_events,
        COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' THEN event_id END) AS add_to_cart_events,
        COUNT(DISTINCT CASE WHEN event_type = 'CHECKOUT' THEN event_id END) AS checkout_events,
        COUNT(DISTINCT CASE WHEN event_type = 'PURCHASE' THEN event_id END) AS purchase_events,
        COUNT(DISTINCT CASE WHEN funnel_stage = 'Acquisition' THEN user_id END) AS acquired_users,
        COUNT(DISTINCT CASE WHEN funnel_stage = 'Conversion' THEN user_id END) AS converted_users
    FROM joined_events
    GROUP BY event_date, country, plan_type, user_segment
)

SELECT * FROM daily_aggregated
