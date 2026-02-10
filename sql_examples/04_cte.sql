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
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM raw_events
    GROUP BY DATE(event_timestamp)
)
SELECT *
FROM daily_events
ORDER BY event_date DESC;

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
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM raw_events
    GROUP BY DATE(event_timestamp)
),

daily_purchases AS (
    -- ステップ2：日別の購入イベント集計
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS purchase_count,
        COUNT(DISTINCT user_id) AS purchasing_users
    FROM raw_events
    WHERE event_type = 'purchase'
    GROUP BY DATE(event_timestamp)
)

SELECT
    e.event_date,
    e.event_count,
    e.unique_users,
    COALESCE(p.purchase_count, 0) AS purchase_count,
    COALESCE(p.purchasing_users, 0) AS purchasing_users,
    ROUND(COALESCE(p.purchase_count, 0)::FLOAT / e.event_count, 4) AS purchase_rate
FROM daily_events e
LEFT JOIN daily_purchases p
    ON e.event_date = p.event_date
ORDER BY e.event_date DESC;

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
    SELECT DATEADD(day, -30, CURRENT_DATE()) AS date_val
    UNION ALL
    SELECT DATEADD(day, 1, date_val)
    FROM date_range
    WHERE date_val < CURRENT_DATE()
)
SELECT date_val
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
    main.event_date,
    main.event_count,
    rank_cte.rank
FROM (
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count
    FROM raw_events
    GROUP BY DATE(event_timestamp)
) AS main
LEFT JOIN (
    SELECT
        event_date,
        ROW_NUMBER() OVER (ORDER BY event_count DESC) AS rank
    FROM (
        SELECT
            DATE(event_timestamp) AS event_date,
            COUNT(*) AS event_count
        FROM raw_events
        GROUP BY DATE(event_timestamp)
    )
) AS rank_cte
ON main.event_date = rank_cte.event_date
ORDER BY main.event_date DESC;

-- パターン2：CTEを使った方法（可読性が高い）
WITH daily_events AS (
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count
    FROM raw_events
    GROUP BY DATE(event_timestamp)
)
SELECT
    de.event_date,
    de.event_count,
    ROW_NUMBER() OVER (ORDER BY de.event_count DESC) AS rank
FROM daily_events de
ORDER BY de.event_date DESC;

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
        user_id,
        MIN(event_timestamp) AS first_event_time,
        MAX(event_timestamp) AS last_event_time,
        COUNT(*) AS total_events
    FROM raw_events
    GROUP BY user_id
),

user_conversions AS (
    -- ステップ2：各ユーザーのコンバージョンステップ
    SELECT
        user_id,
        COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) > 0 AS viewed_page,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) > 0 AS added_to_cart,
        COUNT(CASE WHEN event_type = 'checkout' THEN 1 END) > 0 AS started_checkout,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0 AS completed_purchase
    FROM raw_events
    GROUP BY user_id
)

SELECT
    ue.total_events,
    COUNT(*) AS user_count,
    SUM(CASE WHEN uc.viewed_page THEN 1 ELSE 0 END) AS viewed_page_users,
    SUM(CASE WHEN uc.added_to_cart THEN 1 ELSE 0 END) AS added_to_cart_users,
    SUM(CASE WHEN uc.started_checkout THEN 1 ELSE 0 END) AS started_checkout_users,
    SUM(CASE WHEN uc.completed_purchase THEN 1 ELSE 0 END) AS completed_purchase_users,
    ROUND(
        SUM(CASE WHEN uc.completed_purchase THEN 1 ELSE 0 END)::FLOAT /
        COUNT(*),
        4
    ) AS overall_conversion_rate
FROM user_events ue
INNER JOIN user_conversions uc ON ue.user_id = uc.user_id
GROUP BY ue.total_events
ORDER BY ue.total_events;

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
        e.user_id,
        COUNT(*) AS event_count,
        COUNT(DISTINCT e.session_id) AS session_count,
        MAX(e.event_timestamp) AS last_event
    FROM raw_events e
    GROUP BY e.user_id
),

user_info AS (
    SELECT
        user_id,
        country,
        plan_type,
        is_active
    FROM users
)

SELECT
    ui.user_id,
    ui.country,
    ui.plan_type,
    ui.is_active,
    es.event_count,
    es.session_count,
    es.last_event,
    CASE
        WHEN es.last_event IS NULL THEN 'Never Active'
        WHEN es.last_event < DATEADD(day, -30, CURRENT_DATE()) THEN 'Churned'
        WHEN es.last_event < DATEADD(day, -7, CURRENT_DATE()) THEN 'Inactive'
        ELSE 'Active'
    END AS status
FROM user_info ui
LEFT JOIN event_summary es ON ui.user_id = es.user_id
WHERE ui.is_active = TRUE
ORDER BY es.event_count DESC NULLS LAST;

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
        DATE(e.event_timestamp) AS event_date,
        u.country,
        u.plan_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT e.user_id) AS user_count,
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchase_count
    FROM raw_events e
    INNER JOIN users u ON e.user_id = u.user_id
    WHERE e.event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY DATE(e.event_timestamp), u.country, u.plan_type
),

performance_with_metrics AS (
    SELECT
        event_date,
        country,
        plan_type,
        event_count,
        user_count,
        purchase_count,
        ROUND(event_count::FLOAT / user_count, 2) AS events_per_user,
        ROUND(purchase_count::FLOAT / user_count, 4) AS purchase_rate,
        CASE
            WHEN ROUND(purchase_count::FLOAT / user_count, 4) >= 0.05 THEN 'High'
            WHEN ROUND(purchase_count::FLOAT / user_count, 4) >= 0.02 THEN 'Medium'
            ELSE 'Low'
        END AS conversion_tier
    FROM daily_performance
)

SELECT
    event_date,
    country,
    plan_type,
    event_count,
    user_count,
    purchase_count,
    events_per_user,
    purchase_rate,
    conversion_tier
FROM performance_with_metrics
ORDER BY event_date DESC, purchase_rate DESC;

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
        event_id,
        user_id,
        event_type,
        event_timestamp
    FROM raw_events
    WHERE DATE(event_timestamp) = CURRENT_DATE()
    LIMIT 100
)

SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT event_type) AS event_types,
    MIN(event_timestamp) AS earliest,
    MAX(event_timestamp) AS latest
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
        event_id,
        user_id,
        session_id,
        event_type,
        event_timestamp,
        device_type
    FROM raw_events
    WHERE event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
),

event_summary AS (
    -- ステップ2：集計
    SELECT
        DATE(fe.event_timestamp) AS event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT fe.user_id) AS unique_users
    FROM filtered_events fe
    GROUP BY DATE(fe.event_timestamp)
),

user_attributes AS (
    -- ステップ3：ユーザー属性を追加
    SELECT
        fe.event_date,
        u.country,
        u.plan_type,
        COUNT(fe.event_id) AS event_count,
        COUNT(DISTINCT fe.user_id) AS user_count
    FROM filtered_events fe
    INNER JOIN users u ON fe.user_id = u.user_id
    GROUP BY fe.event_date, u.country, u.plan_type
)

-- ステップ4：最終レポート
SELECT
    event_date,
    country,
    plan_type,
    event_count,
    user_count,
    ROUND(event_count::FLOAT / user_count, 2) AS avg_events_per_user
FROM user_attributes
ORDER BY event_date DESC, event_count DESC;

/*
このテンプレート構造：
  1. filtered_events：フィルタリング（WHERE）
  2. event_summary：基本集計（GROUP BY）
  3. user_attributes：属性追加（JOIN）
  4. 最終SELECT：成形・ソート

このパターンは実務で最も頻繁に使用されます。
dbtに移行する際も、このロジック構造が基本になります。
*/
