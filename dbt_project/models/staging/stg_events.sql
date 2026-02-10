/*
================================================================================
Staging Model: stg_events
================================================================================

【目的】
  生のイベントログ（raw_events）を前処理します。
  - カラム名の標準化
  - データ型の統一
  - 基本的なバリデーション
  - NULL値の処理

【特性】
  - Materialization: VIEW（軽量、リアルタイムデータ）
  - 対象テーブル: raw_events
  - 出力カラム数: raw_events + 計算カラム

【ビジネス要件】
  - イベントタイプは定義済みのものに限定
  - user_id は NOT NULL
  - event_timestamp は UTC基準

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
        event_id,
        user_id,
        session_id,
        event_type,
        page_url,
        event_timestamp,
        device_type,
        country,
        created_at
    FROM {{ source('analytics', 'raw_events') }}
),

cleaned_events AS (
    -- ステップ2：NULL値チェック、データ型統一
    SELECT
        event_id,
        user_id,
        session_id,
        UPPER(event_type) AS event_type,
        LOWER(COALESCE(page_url, '/unknown')) AS page_url,
        event_timestamp,
        LOWER(COALESCE(device_type, 'unknown')) AS device_type,
        UPPER(COALESCE(country, 'XX')) AS country,
        created_at,

        -- ステップ3：計算カラム（dbtで追加）
        DATE(event_timestamp) AS event_date,
        EXTRACT(HOUR FROM event_timestamp) AS event_hour,
        -- マクロを使ってファネルステージを生成（common_logic.sql で定義）
        -- 注：UPPER(event_type) を渡すことで、大文字に変換済みの値で比較する
        {{ funnel_stage_generator('UPPER(event_type)') }} AS funnel_stage
    FROM source_events

    -- ステップ4：バリデーション（WHERE句）
    WHERE
        -- 必須フィールド
        event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND event_timestamp IS NOT NULL
        -- 合理的な日付範囲（過去180日以内）
        AND DATE(event_timestamp) >= DATEADD(day, -180, CURRENT_DATE())
)

SELECT * FROM cleaned_events
