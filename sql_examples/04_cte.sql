/*
================================================================================
ステップ4：CTE（WITH句）- 複雑なクエリの可読性向上
================================================================================

【目的】
  複雑なクエリを小分けにして、段階的に実装します。
  クエリの可読性・保守性が大幅に向上します。

【学習ポイント】
  - WITH句でCTEを定義
  - 複数のCTEを使った段階的な構築
  - CTEの利点：可読性向上、デバッグ効率化
  - サブクエリとの比較

【実務での応用】
  - 複雑な加工ロジックを段階的に実装
  - dbtの下地となる知識
  - チーム開発での可読性確保
*/

-- =====================================================================
-- CTE1：シンプルなCTE
-- =====================================================================

WITH daily_events AS (
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
    FROM RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP)
)
SELECT *
FROM daily_events
ORDER BY EVENT_DATE DESC;

/*
【WITH句の構文】
  WITH cte_name AS (
      -- クエリ
  )
  SELECT * FROM cte_name;

【CTE(Common Table Expression)のメリット】
  1. クエリが読みやすく、段階的に理解できる
  2. 複雑なロジックを分割できる
  3. 同じCTEを複数回参照できる
  4. デバッグが容易（各ステップを個別に確認）
*/


-- =====================================================================
-- CTE2：複数のCTEを使った段階的な構築
-- =====================================================================

WITH daily_events AS (
    -- ステップ1：日別のイベント集計
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
    FROM RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP)
),

daily_purchases AS (
    -- ステップ2：日別の購入イベント集計
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS PURCHASE_COUNT,
        COUNT(DISTINCT USER_ID) AS PURCHASING_USERS
    FROM RAW_EVENTS
    WHERE EVENT_TYPE = 'purchase'
    GROUP BY DATE(EVENT_TIMESTAMP)
)

SELECT
    e.EVENT_DATE,
    e.EVENT_COUNT,
    e.UNIQUE_USERS,
    COALESCE(p.PURCHASE_COUNT, 0) AS PURCHASE_COUNT,
    COALESCE(p.PURCHASING_USERS, 0) AS PURCHASING_USERS,
    ROUND(COALESCE(p.PURCHASE_COUNT, 0)::FLOAT / e.EVENT_COUNT, 4) AS PURCHASE_RATE
FROM daily_events e
LEFT JOIN daily_purchases p
    ON e.EVENT_DATE = p.EVENT_DATE
ORDER BY e.EVENT_DATE DESC;

/*
【複数CTE の構造】
  WITH
    cte1 AS (SELECT ...),
    cte2 AS (SELECT ...),
    cte3 AS (SELECT ...)
  SELECT
    FROM cte1
    LEFT JOIN cte2 ON ...
    LEFT JOIN cte3 ON ...;

【メリット】
  - 各ステップが独立して確認可能
  - ロジックの修正が容易
  - 他のクエリ開発者が理解しやすい

実務での応用：
  - データ品質チェック用のCTE群
  - 中間結果の確認用CTE
  - 最終レポート生成
*/


-- =====================================================================
-- CTE3：WITH RECURSIVE（自己参照CTE）
-- =====================================================================

-- 注：シンプルなRANGE生成の例
WITH date_range AS (
    SELECT DATEADD(day, -30, CURRENT_DATE()) AS DATE_VAL
    UNION ALL
    SELECT DATEADD(day, 1, DATE_VAL)
    FROM date_range
    WHERE DATE_VAL < CURRENT_DATE()
)
SELECT DATE_VAL
FROM date_range
LIMIT 31;

/*
【RECURSIVE CTEの説明】
  - アンカークエリ：最初のセット
  - 再帰クエリ：前の結果をもとに新しいセットを生成
  - UNION ALL で結合

この例：
  アンカー：CURRENT_DATE()から30日前
  再帰：1日ずつ加算

実務での応用：
  - 日付マスタの生成
  - 階層データの処理（組織図など）
  ただし、Snowflakeでは GENERATOR や SEQUENCE の方が効率的な場合が多い
*/


-- =====================================================================
-- CTE4：サブクエリ vs CTE の比較
-- =====================================================================

-- パターン1：サブクエリを使った方法（ネストが深い）
SELECT
    main.EVENT_DATE,
    main.EVENT_COUNT,
    rank_cte.RANK
FROM (
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT
    FROM RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP)
) AS main
LEFT JOIN (
    SELECT
        EVENT_DATE,
        ROW_NUMBER() OVER (ORDER BY EVENT_COUNT DESC) AS RANK
    FROM (
        SELECT
            DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
            COUNT(*) AS EVENT_COUNT
        FROM RAW_EVENTS
        GROUP BY DATE(EVENT_TIMESTAMP)
    )
) AS rank_cte
ON main.EVENT_DATE = rank_cte.EVENT_DATE
ORDER BY main.EVENT_DATE DESC;

-- パターン2：CTEを使った方法（可読性が高い）
WITH daily_events AS (
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT
    FROM RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP)
)
SELECT
    de.EVENT_DATE,
    de.EVENT_COUNT,
    ROW_NUMBER() OVER (ORDER BY de.EVENT_COUNT DESC) AS RANK
FROM daily_events de
ORDER BY de.EVENT_DATE DESC;

/*
【CTEのメリット（サブクエリ比較）】
  1. ネストが減り、可読性が高い
  2. CTEの定義部分が分かりやすい
  3. 複数の参照が容易
  4. デバッグ時に各CTEを個別に実行可能
*/


-- =====================================================================
-- CTE5：ファネル分析の例（複数CTE活用）
-- =====================================================================

WITH user_events AS (
    -- ステップ1：各ユーザーの最初と最後のイベント
    SELECT
        USER_ID,
        MIN(EVENT_TIMESTAMP) AS FIRST_EVENT_TIME,
        MAX(EVENT_TIMESTAMP) AS LAST_EVENT_TIME,
        COUNT(*) AS TOTAL_EVENTS
    FROM RAW_EVENTS
    GROUP BY USER_ID
),

user_conversions AS (
    -- ステップ2：各ユーザーのコンバージョンステップ
    SELECT
        USER_ID,
        COUNT(CASE WHEN EVENT_TYPE = 'page_view' THEN 1 END) > 0 AS VIEWED_PAGE,
        COUNT(CASE WHEN EVENT_TYPE = 'add_to_cart' THEN 1 END) > 0 AS ADDED_TO_CART,
        COUNT(CASE WHEN EVENT_TYPE = 'checkout' THEN 1 END) > 0 AS STARTED_CHECKOUT,
        COUNT(CASE WHEN EVENT_TYPE = 'purchase' THEN 1 END) > 0 AS COMPLETED_PURCHASE
    FROM RAW_EVENTS
    GROUP BY USER_ID
)

SELECT
    ue.TOTAL_EVENTS,
    COUNT(*) AS USER_COUNT,
    SUM(CASE WHEN uc.VIEWED_PAGE THEN 1 ELSE 0 END) AS VIEWED_PAGE_USERS,
    SUM(CASE WHEN uc.ADDED_TO_CART THEN 1 ELSE 0 END) AS ADDED_TO_CART_USERS,
    SUM(CASE WHEN uc.STARTED_CHECKOUT THEN 1 ELSE 0 END) AS STARTED_CHECKOUT_USERS,
    SUM(CASE WHEN uc.COMPLETED_PURCHASE THEN 1 ELSE 0 END) AS COMPLETED_PURCHASE_USERS,
    ROUND(
        SUM(CASE WHEN uc.COMPLETED_PURCHASE THEN 1 ELSE 0 END)::FLOAT /
        COUNT(*),
        4
    ) AS OVERALL_CONVERSION_RATE
FROM user_events ue
INNER JOIN user_conversions uc ON ue.USER_ID = uc.USER_ID
GROUP BY ue.TOTAL_EVENTS
ORDER BY ue.TOTAL_EVENTS;

/*
このクエリの流れ：
  1. user_events：各ユーザーの基本統計
  2. user_conversions：各ユーザーのコンバージョン状況
  3. 最終SELECT：イベント数別のコンバージョン率分析

実務での応用：
  - ファネル分析（段階的なユーザー喪失の可視化）
  - コンバージョン最適化のための分析
*/


-- =====================================================================
-- CTE6：CTE内でのJOIN
-- =====================================================================

WITH event_summary AS (
    SELECT
        e.USER_ID,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT e.SESSION_ID) AS SESSION_COUNT,
        MAX(e.EVENT_TIMESTAMP) AS LAST_EVENT
    FROM RAW_EVENTS e
    GROUP BY e.USER_ID
),

user_info AS (
    SELECT
        USER_ID,
        COUNTRY,
        PLAN_TYPE,
        IS_ACTIVE
    FROM USERS
)

SELECT
    ui.USER_ID,
    ui.COUNTRY,
    ui.PLAN_TYPE,
    ui.IS_ACTIVE,
    es.EVENT_COUNT,
    es.SESSION_COUNT,
    es.LAST_EVENT,
    CASE
        WHEN es.LAST_EVENT IS NULL THEN 'Never Active'
        WHEN es.LAST_EVENT < DATEADD(day, -30, CURRENT_DATE()) THEN 'Churned'
        WHEN es.LAST_EVENT < DATEADD(day, -7, CURRENT_DATE()) THEN 'Inactive'
        ELSE 'Active'
    END AS STATUS
FROM user_info ui
LEFT JOIN event_summary es ON ui.USER_ID = es.USER_ID
WHERE ui.IS_ACTIVE = TRUE
ORDER BY es.EVENT_COUNT DESC NULLS LAST;

/*
【CTE + JOIN の利点】
  - 各テーブルの処理を分離
  - 中間結果が明確
  - 段階的にロジックを追加可能

実務での応用：
  - ユーザーセグメンテーション
  - チャーン予測分析
  - アクティブユーザーの定義と分類
*/


-- =====================================================================
-- CTE7：CASE文を組み込んだ複雑なCTE
-- =====================================================================

WITH daily_performance AS (
    SELECT
        DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
        u.COUNTRY,
        u.PLAN_TYPE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
        COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END) AS PURCHASE_COUNT
    FROM RAW_EVENTS e
    INNER JOIN USERS u ON e.USER_ID = u.USER_ID
    WHERE e.EVENT_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY DATE(e.EVENT_TIMESTAMP), u.COUNTRY, u.PLAN_TYPE
),

performance_with_metrics AS (
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

SELECT
    EVENT_DATE,
    COUNTRY,
    PLAN_TYPE,
    EVENT_COUNT,
    USER_COUNT,
    PURCHASE_COUNT,
    EVENTS_PER_USER,
    PURCHASE_RATE,
    CONVERSION_TIER
FROM performance_with_metrics
ORDER BY EVENT_DATE DESC, PURCHASE_RATE DESC;

/*
このクエリの3段階：
  1. daily_performance：基本集計
  2. performance_with_metrics：メトリクス計算 + ティア分け
  3. 最終SELECT：レポート出力

実務での応用：
  - パフォーマンスダッシュボードのベースクエリ
  - レポート自動生成システム
  - アラートトリガーの閾値判定
*/


-- =====================================================================
-- CTE8：テスト用のダミーデータ生成CTE
-- =====================================================================

WITH sample_data AS (
    SELECT
        EVENT_ID,
        USER_ID,
        EVENT_TYPE,
        EVENT_TIMESTAMP
    FROM RAW_EVENTS
    WHERE DATE(EVENT_TIMESTAMP) = CURRENT_DATE()
    LIMIT 100
)

SELECT
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT EVENT_TYPE) AS EVENT_TYPES,
    MIN(EVENT_TIMESTAMP) AS EARLIEST,
    MAX(EVENT_TIMESTAMP) AS LATEST
FROM sample_data;

/*
【CTE による効率的なテスト】
  実際のデータで試す前に、CTEでサンプル抽出
  パフォーマンスに影響を与えずにロジック検証

実務でのワークフロー：
  1. CTE で LIMIT 100 でテスト
  2. 全データで実行する前に検証
  3. 問題がなければ全体実行
*/


-- =====================================================================
-- CTEのベストプラクティス
-- =====================================================================

/*
【推奨】
1. 論理的な順序で CTE を配置
   - 基本データ取得 → 加工 → 集計 → 最終整形

2. CTE の名前は説明的に
   ✓ WITH daily_purchase_summary AS (...)
   ❌ WITH x AS (...)

3. 複雑な集計は1つのCTE で（3-5ステップ程度）
   それ以上なら複数CTEに分割

4. 各CTEが独立実行可能になるように設計
   デバッグ時に個別確認可能

5. 最後の SELECT は単純に
   最終的なカラム選択とソートのみ

6. パフォーマンスが悪い場合
   Snowflakeのクエリ実行計画（EXPLAIN）で確認
*/


-- =====================================================================
-- まとめ：実務で最もよく使うCTEテンプレート
-- =====================================================================

WITH filtered_events AS (
    -- ステップ1：対象データを抽出
    SELECT
        EVENT_ID,
        USER_ID,
        SESSION_ID,
        EVENT_TYPE,
        EVENT_TIMESTAMP,
        DEVICE_TYPE
    FROM RAW_EVENTS
    WHERE EVENT_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE())
),

event_summary AS (
    -- ステップ2：集計
    SELECT
        DATE(fe.EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT fe.USER_ID) AS UNIQUE_USERS
    FROM filtered_events fe
    GROUP BY DATE(fe.EVENT_TIMESTAMP)
),

user_attributes AS (
    -- ステップ3：ユーザー属性を追加
    SELECT
        fe.EVENT_DATE,
        u.COUNTRY,
        u.PLAN_TYPE,
        COUNT(fe.EVENT_ID) AS EVENT_COUNT,
        COUNT(DISTINCT fe.USER_ID) AS USER_COUNT
    FROM filtered_events fe
    INNER JOIN USERS u ON fe.USER_ID = u.USER_ID
    GROUP BY fe.EVENT_DATE, u.COUNTRY, u.PLAN_TYPE
)

-- ステップ4：最終レポート
SELECT
    EVENT_DATE,
    COUNTRY,
    PLAN_TYPE,
    EVENT_COUNT,
    USER_COUNT,
    ROUND(EVENT_COUNT::FLOAT / USER_COUNT, 2) AS AVG_EVENTS_PER_USER
FROM user_attributes
ORDER BY EVENT_DATE DESC, EVENT_COUNT DESC;

/*
このテンプレート構造：
  1. filtered_events：フィルタリング（WHERE）
  2. event_summary：基本集計（GROUP BY）
  3. user_attributes：属性追加（JOIN）
  4. 最終SELECT：成形・ソート

このパターンは実務で最も頻繁に使用されます。
dbtに移行する際も、このロジック構造が基本になります。
*/
