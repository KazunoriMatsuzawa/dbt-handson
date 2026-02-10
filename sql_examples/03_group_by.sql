/*
================================================================================
ステップ3：GROUP BY、集計関数（AGGREGATE FUNCTIONS）
================================================================================

【目的】
  大量のデータを集計して、意味のあるメトリクスに変換します。
  データ分析の中核となるスキルです。

【学習ポイント】
  - GROUP BY：グループ分割
  - 集計関数：COUNT、SUM、AVG、MAX、MIN等
  - HAVING：集計後のフィルタリング
  - 複数キーでのグループ化

【実務での応用】
  - 日別・国別・デバイス別の集計
  - KPI計算（購入数、ユーザー数等）
  - セグメント分析
*/

-- =====================================================================
-- 集計1：日別のイベント数
-- =====================================================================

SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS event_count
FROM raw_events
GROUP BY DATE(event_timestamp)
ORDER BY event_date DESC;

/*
【GROUP BY の動作】
  DATE(event_timestamp) でグループ化
  各グループの行数を COUNT(*) で計算

実行結果：
  日付ごとのイベント数が表示されます

【DATE関数】
  TIMESTAMP型を DATE型に変換
  これでグループ化時に時刻を無視
*/


-- =====================================================================
-- 集計2：複数の集計関数を組み合わせ
-- =====================================================================

SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM raw_events
GROUP BY DATE(event_timestamp)
ORDER BY event_date DESC;

/*
このクエリから得られる情報：
  - total_events：その日のイベント総数
  - unique_users：その日のアクティブユーザー数
  - unique_sessions：その日のセッション数

実務での応用：
  - 日別のサイトアクティビティ監視
  - トラフィック分析
*/


-- =====================================================================
-- 集計3：COUNT、SUM、AVG、MAX、MIN
-- =====================================================================

SELECT
    device_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT user_id), 2) AS avg_events_per_user,
    MAX(event_timestamp) AS latest_event,
    MIN(event_timestamp) AS earliest_event
FROM raw_events
GROUP BY device_type
ORDER BY event_count DESC;

/*
【各集計関数の説明】
  - COUNT(*)：行数（全て）
  - COUNT(DISTINCT user_id)：ユニークユーザー数
  - COUNT(*)::FLOAT：型キャスト（分割用）
  - MAX(event_timestamp)：最新のイベント時刻
  - MIN(event_timestamp)：最古のイベント時刻

実務での応用：
  - デバイス別のユーザー行動分析
*/


-- =====================================================================
-- 集計4：SUM、AVG による数値集計
-- =====================================================================

SELECT
    DATE(s.session_start) AS session_date,
    COUNT(*) AS session_count,
    SUM(s.page_views) AS total_page_views,
    ROUND(AVG(s.page_views), 2) AS avg_page_views,
    MAX(s.page_views) AS max_page_views,
    MIN(s.page_views) AS min_page_views
FROM sessions s
GROUP BY DATE(s.session_start)
ORDER BY session_date DESC;

/*
【数値集計関数】
  - SUM：合計値
  - AVG：平均値（ROUND で小数点2位）
  - MAX：最大値
  - MIN：最小値

実務での応用：
  - セッション当たりのページビュー分析
  - ユーザー行動パターンの把握
*/


-- =====================================================================
-- 集計5：HAVING句によるフィルタリング
-- =====================================================================

SELECT
    user_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS session_count
FROM raw_events
GROUP BY user_id
HAVING COUNT(*) > 100  -- 100イベント以上のユーザーのみ
ORDER BY event_count DESC;

/*
【HAVING vs WHERE】
  - WHERE：グループ化前のフィルタ（個別行の条件）
  - HAVING：グループ化後のフィルタ（集計結果の条件）

このクエリ：
  100イベント以上を生成した活発なユーザーを抽出

実務での応用：
  - パワーユーザーの特定
  - 活動閾値を超えたユーザーセグメント
*/


-- =====================================================================
-- 集計6：WHERE + GROUP BY + HAVING の組み合わせ
-- =====================================================================

SELECT
    u.country,
    u.plan_type,
    COUNT(DISTINCT e.user_id) AS user_count,
    COUNT(e.event_id) AS event_count,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchase_count
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
WHERE e.event_timestamp >= DATEADD(day, -7, CURRENT_DATE())  -- 過去7日間
GROUP BY u.country, u.plan_type
HAVING COUNT(DISTINCT e.user_id) >= 10  -- 10ユーザー以上
ORDER BY event_count DESC;

/*
【実行順序】
  1. WHERE：e.event_timestamp >= ... でフィルタ
  2. JOIN：テーブル結合
  3. GROUP BY：country, plan_type でグループ化
  4. HAVING：ユーザー数 >= 10 でフィルタ
  5. ORDER BY：結果をソート

実務での応用：
  - 信頼できるセグメント分析（サンプルサイズ確保）
  - 国別・プラン別の週単位分析
*/


-- =====================================================================
-- 集計7：複数キーでのグループ化
-- =====================================================================

SELECT
    DATE(e.event_timestamp) AS event_date,
    e.event_type,
    e.device_type,
    u.country,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.user_id) AS unique_users
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
GROUP BY
    DATE(e.event_timestamp),
    e.event_type,
    e.device_type,
    u.country
ORDER BY event_date DESC, event_count DESC
LIMIT 50;

/*
【複数キーでのグループ化】
  4つのディメンションでグループ化
  細粒度（granular）な分析が可能

注意：
  GROUP BY に指定したカラムは、
  SELECT で集計関数なしで指定可能

例：
  ✓ SELECT DATE(...), event_type, COUNT(*) FROM ... GROUP BY DATE(...), event_type
  ❌ SELECT DATE(...), event_type, device_type, COUNT(*) FROM ... GROUP BY DATE(...), event_type
     （device_type が GROUP BY にない）
*/


-- =====================================================================
-- 集計8：CASE文を使った条件付き集計
-- =====================================================================

SELECT
    u.country,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) AS purchase_events,
    COUNT(CASE WHEN e.event_type = 'checkout' THEN 1 END) AS checkout_events,
    COUNT(CASE WHEN e.event_type = 'add_to_cart' THEN 1 END) AS cart_events,
    COUNT(CASE WHEN e.event_type = 'page_view' THEN 1 END) AS pageview_events
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
GROUP BY u.country
ORDER BY total_events DESC;

/*
【CASE文による分岐集計】
  1つのグループ内で異なる条件の件数を同時に計算

このクエリ：
  国別に、イベント種別ごとの件数を分けて計算

実務での応用：
  - ファネル分析（page_view → add_to_cart → checkout → purchase）
  - コンバージョンステップの可視化
*/


-- =====================================================================
-- 集計9：ウィンドウ関数との比較（参考）
-- =====================================================================

-- GROUP BYを使った集計
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS daily_events
FROM raw_events
GROUP BY DATE(event_timestamp)
ORDER BY event_date;

-- ウィンドウ関数を使った集計（参考）
SELECT DISTINCT
    DATE(event_timestamp) AS event_date,
    COUNT(*) OVER (PARTITION BY DATE(event_timestamp)) AS daily_events
FROM raw_events
ORDER BY event_date;

/*
【GROUP BY vs ウィンドウ関数】
  どちらも同じ結果ですが、用途が異なります

  GROUP BY：
  - 集計結果のみ欲しい場合
  - 行数を減らしたい場合

  ウィンドウ関数：
  - 個別行と集計値を同時に表示したい場合
  - ランキングが必要な場合
  - 詳細情報 + 集計が必要な場合

本ハンズオンではウィンドウ関数は扱いませんが、
実務では頻繁に使用される重要なテクニックです
*/


-- =====================================================================
-- 集計10：NULL値の取り扱い
-- =====================================================================

SELECT
    COALESCE(device_type, 'Unknown') AS device_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS user_count
FROM raw_events
GROUP BY device_type
ORDER BY event_count DESC;

/*
【NULL値の処理】
  - COALESCE(device_type, 'Unknown')：
    NULLの場合は'Unknown'に置き換え

実務での応用：
  - レポートでNULL値を明示的に表示
  - カテゴリ分析でUnknownカテゴリを認識
*/


-- =====================================================================
-- 集計11：集計後の四則演算
-- =====================================================================

SELECT
    u.plan_type,
    COUNT(DISTINCT e.user_id) AS user_count,
    COUNT(e.event_id) AS event_count,
    ROUND(COUNT(e.event_id)::FLOAT / COUNT(DISTINCT e.user_id), 2) AS avg_events_per_user,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchase_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END)::FLOAT /
        COUNT(DISTINCT e.user_id),
        4
    ) AS purchase_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END)::FLOAT /
        COUNT(e.event_id),
        4
    ) AS purchase_event_ratio
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
GROUP BY u.plan_type
ORDER BY user_count DESC;

/*
【集計後の計算】
  - avg_events_per_user：ユーザー当たりイベント数
  - purchase_rate：購入ユーザー数 / 全ユーザー数
  - purchase_event_ratio：購入イベント数 / 全イベント数

これらのKPI計算は実務で頻繁に行われます
*/


-- =====================================================================
-- 集計12：GROUP BY ALL（Snowflakeの拡張機能）
-- =====================================================================

SELECT
    DATE(e.event_timestamp) AS event_date,
    e.event_type,
    e.device_type,
    COUNT(*) AS event_count
FROM raw_events e
GROUP BY ALL  -- SELECT の全カラムで自動的にグループ化
ORDER BY event_date DESC, event_count DESC;

/*
【GROUP BY ALL】
  Snowflakeの便利な機能で、SELECTの全カラムで自動グループ化

上記は以下と同等：
  GROUP BY DATE(e.event_timestamp), e.event_type, e.device_type
*/


-- =====================================================================
-- 集計13：パフォーマンス最適化のコツ
-- =====================================================================

/*
【GROUP BY のベストプラクティス】

1. 不要なカラムはSELECTに含めない
   ✓ SELECT country, COUNT(*)
   ❌ SELECT country, user_id, COUNT(*)  (user_id で更にグループ化される)

2. フィルタリングはWHEREで実施
   WHERE event_timestamp >= '2025-12-01'  (グループ化前に行数削減)

3. 集計後のフィルタはHAVINGで実施
   HAVING COUNT(*) > 10

4. DISTINCT の多用を避ける
   COUNT(DISTINCT user_id) は処理が重い場合がある

5. GROUP BY の順序
   頻繁にフィルタされるカラムを左に
   GROUP BY date, country (← dateの方が一般的なフィルタ)

6. インデックスを活用
   GROUP BY のベースとなるカラムにインデックスがあると高速化
*/


-- =====================================================================
-- まとめ：実務でよく使うテンプレート
-- =====================================================================

SELECT
    DATE(e.event_timestamp) AS event_date,
    u.country,
    e.device_type,
    e.event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.user_id) AS user_count,
    COUNT(DISTINCT e.session_id) AS session_count,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT e.user_id), 2) AS avg_events_per_user,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT e.session_id), 2) AS avg_events_per_session,
    MAX(e.event_timestamp) AS latest_event
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
WHERE e.event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY
    DATE(e.event_timestamp),
    u.country,
    e.device_type,
    e.event_type
HAVING COUNT(*) > 5  -- サンプルサイズ確保
ORDER BY event_date DESC, event_count DESC;

/*
このテンプレートは実務で頻繁に使用されます：
  1. WHERE：対象期間をフィルタ
  2. JOIN：属性情報を追加
  3. GROUP BY：複数ディメンションで分割
  4. HAVING：信頼できるセグメントのみ
  5. 集計関数：KPI計算

このパターンを習得すれば、データ分析の大部分に対応できます
*/
