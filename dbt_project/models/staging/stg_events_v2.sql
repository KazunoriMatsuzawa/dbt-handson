/*
================================================================================
Staging Model: stg_events_v2（強化版Approach B・初心者コース用）
================================================================================

【目的】
  生のイベントログ（RAW_EVENTS）を最小限のクリーニングで整えます。
  1コマ目 Step A で学んだ SELECT + WHERE + DATE() だけで構成。

【1コマ目との対応】
  Step A: SELECT, WHERE, DATE() → そのまま使用
  Step C: V_DAILY_EVENTS の CREATE VIEW → {{ config(materialized='view') }} に置き換え

【特性】
  - Materialization: VIEW
  - 1コマ目で学んでいないSQL構文は使わない
  - staging = 生データをきれいにする層（各ソーステーブルに1つ）
*/

{{ config(
    materialized='view',
    tags=['staging', 'beginner']
) }}

SELECT
    EVENT_ID,
    USER_ID,
    SESSION_ID,
    EVENT_TYPE,
    EVENT_TIMESTAMP,
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE
FROM {{ source('raw_data', 'RAW_EVENTS') }}
WHERE EVENT_ID IS NOT NULL
  AND USER_ID IS NOT NULL
