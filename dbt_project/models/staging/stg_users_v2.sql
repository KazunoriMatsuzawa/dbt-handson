/*
================================================================================
Staging Model: stg_users_v2（強化版Approach B・初心者コース用）
================================================================================

【目的】
  ユーザーマスタ（USERS）を最小限のクリーニングで整えます。
  stg_events_v2 と同じパターン → 「staging層は各ソースに1つ」を体験。

【1コマ目との対応】
  Step A: SELECT → そのまま使用

【特性】
  - Materialization: VIEW
  - stg_events_v2と同じ構造パターン（反復で定着）
*/

{{ config(
    materialized='view',
    tags=['staging', 'beginner']
) }}

SELECT
    USER_ID,
    SIGNUP_DATE,
    UPPER(COUNTRY) AS COUNTRY,
    PLAN_TYPE,
    IS_ACTIVE
FROM {{ source('raw_data', 'USERS') }}
