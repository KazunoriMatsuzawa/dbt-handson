/*
================================================================================
Staging Model: stg_events
================================================================================

【目的】
  生のイベントログ（RAW_EVENTS）を前処理します。
  - カラム名の標準化
  - データ型の統一
  - 基本的なバリデーション
  - NULL値の処理

【特性】
  - Materialization: VIEW（軽量、リアルタイムデータ）
  - 対象テーブル: RAW_EVENTS
  - 出力カラム数: RAW_EVENTS + 計算カラム

【ビジネス要件】
  - イベントタイプは定義済みのものに限定
  - USER_ID は NOT NULL
  - EVENT_TIMESTAMP は UTC基準

【出力】
  ref('stg_events') で参照可能
*/

{{ config(
    materialized='view',
    tags=['staging', 'events'],
    description='前処理済みイベントログ'
) }}

WITH source_events AS (
    -- ステップ1：ソーステーブルからデータを抽出
    SELECT
        EVENT_ID,
        USER_ID,
        SESSION_ID,
        EVENT_TYPE,
        PAGE_URL,
        EVENT_TIMESTAMP,
        DEVICE_TYPE,
        COUNTRY,
        CREATED_AT
    FROM {{ source('raw_data', 'RAW_EVENTS') }}
),

cleaned_events AS (
    -- ステップ2：NULL値チェック、データ型統一
    SELECT
        EVENT_ID,
        USER_ID,
        SESSION_ID,
        UPPER(EVENT_TYPE) AS EVENT_TYPE,
        LOWER(COALESCE(PAGE_URL, '/unknown')) AS PAGE_URL,
        EVENT_TIMESTAMP,
        LOWER(COALESCE(DEVICE_TYPE, 'unknown')) AS DEVICE_TYPE,
        UPPER(COALESCE(COUNTRY, 'XX')) AS COUNTRY,
        CREATED_AT,

        -- ステップ3：計算カラム（dbtで追加）
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        EXTRACT(HOUR FROM EVENT_TIMESTAMP) AS EVENT_HOUR,
        -- マクロを使ってファネルステージを生成（common_logic.sql で定義）
        -- 注：UPPER(EVENT_TYPE) を渡すことで、大文字に変換済みの値で比較する
        {{ funnel_stage_generator('UPPER(EVENT_TYPE)') }} AS FUNNEL_STAGE
    FROM source_events

    -- ステップ4：バリデーション（WHERE句）
    WHERE
        -- 必須フィールド
        EVENT_ID IS NOT NULL
        AND USER_ID IS NOT NULL
        AND EVENT_TIMESTAMP IS NOT NULL
        -- 合理的な日付範囲（過去180日以内）
        AND DATE(EVENT_TIMESTAMP) >= DATEADD(day, -180, CURRENT_DATE())
)

SELECT * FROM cleaned_events
