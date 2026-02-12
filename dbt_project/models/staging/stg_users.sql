/*
================================================================================
Staging Model: stg_users
================================================================================

【目的】
  ユーザーマスタ（USERS）を前処理します。
  - カラム名の標準化
  - データ型の統一
  - セグメンテーション情報の追加

【特性】
  - Materialization: VIEW（軽量）
  - 対象テーブル: USERS
  - 出力カラム数: USERS + セグメンテーション

【ビジネス要件】
  - プランタイプは free or premium
  - IS_ACTIVE フラグで非アクティブユーザーを識別
  - USER_SEGMENT を計算

【出力】
  ref('stg_users') で参照可能
*/

{{ config(
    materialized='view',
    tags=['staging', 'users'],
    description='前処理済みユーザーマスタ'
) }}

WITH source_users AS (
    -- ステップ1：ソーステーブルからデータを抽出
    SELECT
        USER_ID,
        SIGNUP_DATE,
        COUNTRY,
        PLAN_TYPE,
        IS_ACTIVE,
        UPDATED_AT
    FROM {{ source('raw_data', 'USERS') }}
),

enriched_users AS (
    -- ステップ2：セグメンテーション情報を追加
    SELECT
        USER_ID,
        SIGNUP_DATE,
        UPPER(COALESCE(COUNTRY, 'XX')) AS COUNTRY,
        LOWER(PLAN_TYPE) AS PLAN_TYPE,
        IS_ACTIVE,
        UPDATED_AT,

        -- ステップ3：計算カラム（マクロを使ってDRY原則を実践）
        DATEDIFF(day, SIGNUP_DATE, CURRENT_DATE()) AS DAYS_SINCE_SIGNUP,
        -- マクロで USER_SEGMENT を生成（common_logic.sql で定義）
        {{ user_segment_generator('PLAN_TYPE', 'IS_ACTIVE') }} AS USER_SEGMENT,
        -- マクロでコホートを生成（common_logic.sql で定義）
        {{ cohort_generator('SIGNUP_DATE') }} AS COHORT
    FROM source_users
)

SELECT * FROM enriched_users
