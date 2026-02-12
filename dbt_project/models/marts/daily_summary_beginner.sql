/*
================================================================================
Marts Model: daily_summary_beginner（初心者コース用）
================================================================================

【目的】
  初心者向けの日別サマリーテーブルです。
  intermediate層をスキップし、stagingから直接集計します。
  シンプルなKPIのみ（event_count, unique_users, purchase_count）。

【特性】
  - Materialization: TABLE
  - intermediate層をスキップ（初心者にわかりやすい構成）
  - ref() で staging モデルを参照（依存管理を体験）

【出力】
  DAILY_SUMMARY_BEGINNER テーブル
*/

{{ config(
    materialized='table',
    tags=['marts', 'beginner'],
    description='日別サマリー（初心者コース用・stagingから直接集計）'
) }}

WITH events AS (
    -- staging モデルからイベントデータを取得
    SELECT * FROM {{ ref('stg_events_beginner') }}
),

users AS (
    -- staging モデルからユーザーデータを取得
    SELECT * FROM {{ ref('stg_users_beginner') }}
),

joined AS (
    -- イベントとユーザーを結合（SQL Session 1 の JOIN を dbt で体験）
    SELECT
        e.EVENT_DATE,
        u.COUNTRY,
        e.EVENT_ID,
        e.USER_ID,
        e.EVENT_TYPE
    FROM events e
    INNER JOIN users u
        ON e.USER_ID = u.USER_ID
),

daily_aggregated AS (
    -- 日別・国別に集計（SQL Session 1 の GROUP BY を dbt で体験）
    SELECT
        EVENT_DATE,
        COUNTRY,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
        COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'PURCHASE' THEN EVENT_ID END) AS PURCHASE_COUNT
    FROM joined
    GROUP BY EVENT_DATE, COUNTRY
)

SELECT * FROM daily_aggregated
