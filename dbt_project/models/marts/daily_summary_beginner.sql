/*
================================================================================
Marts Model: daily_summary_beginner（初心者コース用）
================================================================================

【目的】
  日別・国別のイベントサマリーを作成します。
  1コマ目 Step A で学んだ JOIN + GROUP BY + CASE をそのまま使用。

【1コマ目との対応】
  Step A: INNER JOIN, GROUP BY, COUNT, CASE → そのまま使用
  Step D: SPの DELETE + INSERT → {{ config(materialized='table') }} に置き換え
  Step E: Task の AFTER句 → {{ ref() }} に置き換え

【特性】
  - Materialization: TABLE
  - ref() で staging モデルへの依存を自動管理
  - marts = ビジネスの答えを出す層（staging を組み合わせる）
*/

{{ config(
    materialized='table',
    tags=['marts', 'beginner']
) }}

-- CTE：stagingモデルを結合（1コマ目では5段CTEだったが、ファイル分割で1段に）
WITH joined AS (
    SELECT
        E.EVENT_DATE,
        U.COUNTRY,
        E.EVENT_ID,
        E.USER_ID,
        E.EVENT_TYPE
    FROM {{ ref('stg_events_beginner') }} E
    INNER JOIN {{ ref('stg_users_beginner') }} U
        ON E.USER_ID = U.USER_ID
)

SELECT
    EVENT_DATE,
    COUNTRY,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(CASE WHEN EVENT_TYPE = 'purchase' THEN 1 END) AS PURCHASE_COUNT
FROM joined
GROUP BY EVENT_DATE, COUNTRY
