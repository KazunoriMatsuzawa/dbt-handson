/*
================================================================================
Staging Model: stg_users
================================================================================

【目的】
  ユーザーマスタ（users）を前処理します。
  - カラム名の標準化
  - データ型の統一
  - セグメンテーション情報の追加

【特性】
  - Materialization: VIEW（軽量）
  - 対象テーブル: users
  - 出力カラム数: users + セグメンテーション

【ビジネス要件】
  - プランタイプは free or premium
  - is_active フラグで非アクティブユーザーを識別
  - user_segment を計算

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
        user_id,
        signup_date,
        country,
        plan_type,
        is_active,
        updated_at
    FROM {{ source('analytics', 'users') }}
),

enriched_users AS (
    -- ステップ2：セグメンテーション情報を追加
    SELECT
        user_id,
        signup_date,
        UPPER(COALESCE(country, 'XX')) AS country,
        LOWER(plan_type) AS plan_type,
        is_active,
        updated_at,

        -- ステップ3：計算カラム（マクロを使ってDRY原則を実践）
        DATEDIFF(day, signup_date, CURRENT_DATE()) AS days_since_signup,
        -- マクロで user_segment を生成（common_logic.sql で定義）
        {{ user_segment_generator('plan_type', 'is_active') }} AS user_segment,
        -- マクロでコホートを生成（common_logic.sql で定義）
        {{ cohort_generator('signup_date') }} AS cohort
    FROM source_users
)

SELECT * FROM enriched_users
