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
        e.EVENT_DATE,
        e.EVENT_HOUR,
        e.EVENT_TYPE,
        e.FUNNEL_STAGE,
        e.DEVICE_TYPE,
        u.COUNTRY,
        u.PLAN_TYPE,
        u.USER_SEGMENT,
        u.COHORT,
        e.USER_ID,
        e.SESSION_ID,
        e.EVENT_TIMESTAMP,
        e.EVENT_ID
    FROM events e
    INNER JOIN users u
        ON e.USER_ID = u.USER_ID
),

daily_aggregated AS (
    -- ステップ2：日別・国別・プラン別に集計
    SELECT
        EVENT_DATE,
        COUNTRY,
        PLAN_TYPE,
        USER_SEGMENT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
        COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS,
        COUNT(EVENT_ID) AS TOTAL_EVENTS,
        COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'PAGE_VIEW' THEN EVENT_ID END) AS PAGEVIEW_EVENTS,
        COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'CLICK' THEN EVENT_ID END) AS CLICK_EVENTS,
        COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'ADD_TO_CART' THEN EVENT_ID END) AS ADD_TO_CART_EVENTS,
        COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'CHECKOUT' THEN EVENT_ID END) AS CHECKOUT_EVENTS,
        COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'PURCHASE' THEN EVENT_ID END) AS PURCHASE_EVENTS,
        COUNT(DISTINCT CASE WHEN FUNNEL_STAGE = 'Acquisition' THEN USER_ID END) AS ACQUIRED_USERS,
        COUNT(DISTINCT CASE WHEN FUNNEL_STAGE = 'Conversion' THEN USER_ID END) AS CONVERTED_USERS
    FROM joined_events
    GROUP BY EVENT_DATE, COUNTRY, PLAN_TYPE, USER_SEGMENT
)

SELECT * FROM daily_aggregated
