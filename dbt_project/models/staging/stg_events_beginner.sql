/*
================================================================================
Staging Model: stg_events_beginner（初心者コース用）
================================================================================

【目的】
  生のイベントログ（RAW_EVENTS）をシンプルに前処理します。
  マクロを使わず、SQLのみで完結する初心者向けモデルです。

【特性】
  - Materialization: VIEW
  - マクロ不使用（初心者が読みやすいよう直接記述）
  - 最低限のクリーニングのみ

【出力】
  ref('stg_events_beginner') で参照可能
*/

{{ config(
    materialized='view',
    tags=['staging', 'beginner'],
    description='前処理済みイベントログ（初心者コース用・マクロ不使用）'
) }}

WITH source_events AS (
    -- ソーステーブルからデータを取得
    SELECT
        EVENT_ID,
        USER_ID,
        SESSION_ID,
        EVENT_TYPE,
        EVENT_TIMESTAMP,
        DEVICE_TYPE,
        COUNTRY
    FROM {{ source('analytics', 'RAW_EVENTS') }}
),

cleaned_events AS (
    -- データクリーニング：大文字化・日付変換・ファネルステージ付与
    SELECT
        EVENT_ID,
        USER_ID,
        SESSION_ID,
        UPPER(EVENT_TYPE) AS EVENT_TYPE,
        EVENT_TIMESTAMP,
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        DEVICE_TYPE,
        COUNTRY,

        -- ファネルステージを直接CASEで記述（マクロ不使用）
        CASE
            WHEN UPPER(EVENT_TYPE) = 'PAGE_VIEW' THEN 'Engagement'
            WHEN UPPER(EVENT_TYPE) IN ('CLICK', 'ADD_TO_CART') THEN 'Consideration'
            WHEN UPPER(EVENT_TYPE) IN ('CHECKOUT', 'PURCHASE') THEN 'Conversion'
            WHEN UPPER(EVENT_TYPE) = 'SIGN_UP' THEN 'Acquisition'
            ELSE 'Other'
        END AS FUNNEL_STAGE
    FROM source_events
    WHERE EVENT_ID IS NOT NULL
      AND USER_ID IS NOT NULL
)

SELECT * FROM cleaned_events
