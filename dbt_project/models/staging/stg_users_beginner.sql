/*
================================================================================
Staging Model: stg_users_beginner（初心者コース用）
================================================================================

【目的】
  ユーザーマスタ（USERS）をシンプルに前処理します。
  マクロを使わず、基本的なデータクリーニングのみ行います。

【特性】
  - Materialization: VIEW
  - マクロ不使用（初心者が読みやすいよう直接記述）
  - セグメンテーションやコホート計算は含めない

【出力】
  ref('stg_users_beginner') で参照可能
*/

{{ config(
    materialized='view',
    tags=['staging', 'beginner'],
    description='前処理済みユーザーマスタ（初心者コース用・マクロ不使用）'
) }}

WITH source_users AS (
    -- ソーステーブルからデータを取得
    SELECT
        USER_ID,
        SIGNUP_DATE,
        COUNTRY,
        PLAN_TYPE,
        IS_ACTIVE
    FROM {{ source('raw_data', 'USERS') }}
),

cleaned_users AS (
    -- 基本的なデータクリーニング
    SELECT
        USER_ID,
        SIGNUP_DATE,
        UPPER(COALESCE(COUNTRY, 'XX')) AS COUNTRY,
        LOWER(PLAN_TYPE) AS PLAN_TYPE,
        IS_ACTIVE
    FROM source_users
)

SELECT * FROM cleaned_users
