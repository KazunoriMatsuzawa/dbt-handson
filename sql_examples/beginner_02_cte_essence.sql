/*
================================================================================
Step B：CTE体験 -「SQLが長くなる問題」（8分）
================================================================================

【目的】
  CTE（WITH句）で複雑なクエリを整理する方法を学びます。
  同時に「CTEが長くなると管理できない」という壁を体験します。

【壁1】CTE長大化 → ファイル分割できない
*/


-- =====================================================================
-- CTE基礎：1つのCTEで中間結果を作る
-- =====================================================================

WITH daily_events AS (
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
    FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP)
)
SELECT *
FROM daily_events
WHERE EVENT_DATE >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC;

/*
WITH句（CTE = Common Table Expression）：
  クエリの中に「名前付きの中間テーブル」を定義できる
  → 複雑なロジックを段階的に組み立てられる
*/


-- =====================================================================
-- 複数CTE：段階的にロジックを構築する
-- =====================================================================

WITH daily_events AS (
    -- ステップ1：日別の全イベント集計
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
    FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP)
),

daily_purchases AS (
    -- ステップ2：日別の購入イベント集計
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS PURCHASE_COUNT,
        COUNT(DISTINCT USER_ID) AS PURCHASING_USERS
    FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
    WHERE EVENT_TYPE = 'purchase'
    GROUP BY DATE(EVENT_TIMESTAMP)
)

-- ステップ3：結合して購入率を計算
SELECT
    E.EVENT_DATE,
    E.EVENT_COUNT,
    E.UNIQUE_USERS,
    COALESCE(P.PURCHASE_COUNT, 0) AS PURCHASE_COUNT,
    ROUND(COALESCE(P.PURCHASE_COUNT, 0)::FLOAT / E.EVENT_COUNT, 4) AS PURCHASE_RATE
FROM daily_events E
LEFT JOIN daily_purchases P
    ON E.EVENT_DATE = P.EVENT_DATE
ORDER BY E.EVENT_DATE DESC;

/*
複数CTEのメリット：
  - 各ステップを個別に実行して確認できる（デバッグが楽）
  - ロジックが段階的で読みやすい
*/


-- =====================================================================
-- 実務規模のCTE：3〜4段で複雑化していく
-- =====================================================================

WITH daily_performance AS (
    -- ステップ1：基本集計
    SELECT
        DATE(E.EVENT_TIMESTAMP) AS EVENT_DATE,
        U.COUNTRY,
        U.PLAN_TYPE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT E.USER_ID) AS USER_COUNT,
        COUNT(DISTINCT CASE WHEN E.EVENT_TYPE = 'purchase' THEN E.EVENT_ID END) AS PURCHASE_COUNT
    FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS E
    INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS U ON E.USER_ID = U.USER_ID
    WHERE E.EVENT_TIMESTAMP >= DATEADD(DAY, -30, CURRENT_DATE())
    GROUP BY DATE(E.EVENT_TIMESTAMP), U.COUNTRY, U.PLAN_TYPE
),

performance_with_metrics AS (
    -- ステップ2：メトリクス計算
    SELECT
        EVENT_DATE,
        COUNTRY,
        PLAN_TYPE,
        EVENT_COUNT,
        USER_COUNT,
        PURCHASE_COUNT,
        ROUND(EVENT_COUNT::FLOAT / USER_COUNT, 2) AS EVENTS_PER_USER,
        ROUND(PURCHASE_COUNT::FLOAT / USER_COUNT, 4) AS PURCHASE_RATE,
        CASE
            WHEN ROUND(PURCHASE_COUNT::FLOAT / USER_COUNT, 4) >= 0.05 THEN 'High'
            WHEN ROUND(PURCHASE_COUNT::FLOAT / USER_COUNT, 4) >= 0.02 THEN 'Medium'
            ELSE 'Low'
        END AS CONVERSION_TIER
    FROM daily_performance
)

-- ステップ3：最終出力
SELECT *
FROM performance_with_metrics
ORDER BY EVENT_DATE DESC, PURCHASE_RATE DESC;


/*
================================================================================
【壁1：CTE長大化の問題】
================================================================================

このクエリは3段階のCTEで構成されています。実務ではさらに増えます：

  daily_performance（基本集計）
   → performance_with_metrics（メトリクス計算）
     → conversion_tier（ティア分類）
       → weekly_aggregation（週次集約）
         → wow_comparison（前週比較）
           → ... もっと続く

問題点：
  1. 1つのSQLファイルが数百行になる → 可読性の低下
  2. 途中のCTEを別のクエリで再利用できない → コピペが増える
  3. ファイルを分割できない → 「daily_performance」だけ別ファイルにしたい

  → dbt ではこの問題を「モデル分割 + ref()」で解決します。
     各CTEを独立したSQLファイルにして、ref()で参照できます。

  例：
    stg_events.sql     → daily_performanceに相当
    int_daily_events.sql → performance_with_metricsに相当
    daily_summary.sql   → 最終出力に相当

  これが2コマ目（dbt入門）で体験する内容です。
*/
