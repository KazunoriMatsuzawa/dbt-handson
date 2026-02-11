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
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS EVENT_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP)
ORDER BY EVENT_DATE DESC;

/*
【GROUP BY の動作】
  DATE(EVENT_TIMESTAMP) でグループ化
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
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS TOTAL_EVENTS,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP)
ORDER BY EVENT_DATE DESC;

/*
このクエリから得られる情報：
  - TOTAL_EVENTS：その日のイベント総数
  - UNIQUE_USERS：その日のアクティブユーザー数
  - UNIQUE_SESSIONS：その日のセッション数

実務での応用：
  - 日別のサイトアクティビティ監視
  - トラフィック分析
*/


-- =====================================================================
-- 集計3：COUNT、SUM、AVG、MAX、MIN
-- =====================================================================

SELECT
    DEVICE_TYPE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT USER_ID), 2) AS AVG_EVENTS_PER_USER,
    MAX(EVENT_TIMESTAMP) AS LATEST_EVENT,
    MIN(EVENT_TIMESTAMP) AS EARLIEST_EVENT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DEVICE_TYPE
ORDER BY EVENT_COUNT DESC;

/*
【各集計関数の説明】
  - COUNT(*)：行数（全て）
  - COUNT(DISTINCT USER_ID)：ユニークユーザー数
  - COUNT(*)::FLOAT：型キャスト（分割用）
  - MAX(EVENT_TIMESTAMP)：最新のイベント時刻
  - MIN(EVENT_TIMESTAMP)：最古のイベント時刻

実務での応用：
  - デバイス別のユーザー行動分析
*/


-- =====================================================================
-- 集計4：SUM、AVG による数値集計
-- =====================================================================

SELECT
    DATE(s.SESSION_START) AS SESSION_DATE,
    COUNT(*) AS SESSION_COUNT,
    SUM(s.PAGE_VIEWS) AS TOTAL_PAGE_VIEWS,
    ROUND(AVG(s.PAGE_VIEWS), 2) AS AVG_PAGE_VIEWS,
    MAX(s.PAGE_VIEWS) AS MAX_PAGE_VIEWS,
    MIN(s.PAGE_VIEWS) AS MIN_PAGE_VIEWS
FROM DIESELPJ_TEST.DBT_HANDSON.SESSIONS s
GROUP BY DATE(s.SESSION_START)
ORDER BY SESSION_DATE DESC;

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
    USER_ID,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT SESSION_ID) AS SESSION_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY USER_ID
HAVING COUNT(*) > 100  -- 100イベント以上のユーザーのみ
ORDER BY EVENT_COUNT DESC;

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
    u.COUNTRY,
    u.PLAN_TYPE,
    COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
    COUNT(e.EVENT_ID) AS EVENT_COUNT,
    COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END) AS PURCHASE_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u ON e.USER_ID = u.USER_ID
WHERE e.EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_DATE())  -- 過去7日間
GROUP BY u.COUNTRY, u.PLAN_TYPE
HAVING COUNT(DISTINCT e.USER_ID) >= 10  -- 10ユーザー以上
ORDER BY EVENT_COUNT DESC;

/*
【実行順序】
  1. WHERE：e.EVENT_TIMESTAMP >= ... でフィルタ
  2. JOIN：テーブル結合
  3. GROUP BY：COUNTRY, PLAN_TYPE でグループ化
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
    DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
    e.EVENT_TYPE,
    e.DEVICE_TYPE,
    u.COUNTRY,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT e.USER_ID) AS UNIQUE_USERS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u ON e.USER_ID = u.USER_ID
GROUP BY
    DATE(e.EVENT_TIMESTAMP),
    e.EVENT_TYPE,
    e.DEVICE_TYPE,
    u.COUNTRY
ORDER BY EVENT_DATE DESC, EVENT_COUNT DESC
LIMIT 50;

/*
【複数キーでのグループ化】
  4つのディメンションでグループ化
  細粒度（granular）な分析が可能

注意：
  GROUP BY に指定したカラムは、
  SELECT で集計関数なしで指定可能

例：
  ✓ SELECT DATE(...), EVENT_TYPE, COUNT(*) FROM ... GROUP BY DATE(...), EVENT_TYPE
  ❌ SELECT DATE(...), EVENT_TYPE, DEVICE_TYPE, COUNT(*) FROM ... GROUP BY DATE(...), EVENT_TYPE
     （DEVICE_TYPE が GROUP BY にない）
*/


-- =====================================================================
-- 集計8：CASE文を使った条件付き集計
-- =====================================================================

SELECT
    u.COUNTRY,
    COUNT(*) AS TOTAL_EVENTS,
    COUNT(CASE WHEN e.EVENT_TYPE = 'purchase' THEN 1 END) AS PURCHASE_EVENTS,
    COUNT(CASE WHEN e.EVENT_TYPE = 'checkout' THEN 1 END) AS CHECKOUT_EVENTS,
    COUNT(CASE WHEN e.EVENT_TYPE = 'add_to_cart' THEN 1 END) AS CART_EVENTS,
    COUNT(CASE WHEN e.EVENT_TYPE = 'page_view' THEN 1 END) AS PAGEVIEW_EVENTS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u ON e.USER_ID = u.USER_ID
GROUP BY u.COUNTRY
ORDER BY TOTAL_EVENTS DESC;

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
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS DAILY_EVENTS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP)
ORDER BY EVENT_DATE;

-- ウィンドウ関数を使った集計（参考）
SELECT DISTINCT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) OVER (PARTITION BY DATE(EVENT_TIMESTAMP)) AS DAILY_EVENTS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
ORDER BY EVENT_DATE;

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
    COALESCE(DEVICE_TYPE, 'Unknown') AS DEVICE_TYPE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS USER_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DEVICE_TYPE
ORDER BY EVENT_COUNT DESC;

/*
【NULL値の処理】
  - COALESCE(DEVICE_TYPE, 'Unknown')：
    NULLの場合は'Unknown'に置き換え

実務での応用：
  - レポートでNULL値を明示的に表示
  - カテゴリ分析でUnknownカテゴリを認識
*/


-- =====================================================================
-- 集計11：集計後の四則演算
-- =====================================================================

SELECT
    u.PLAN_TYPE,
    COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
    COUNT(e.EVENT_ID) AS EVENT_COUNT,
    ROUND(COUNT(e.EVENT_ID)::FLOAT / COUNT(DISTINCT e.USER_ID), 2) AS AVG_EVENTS_PER_USER,
    COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END) AS PURCHASE_COUNT,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END)::FLOAT /
        COUNT(DISTINCT e.USER_ID),
        4
    ) AS PURCHASE_RATE,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END)::FLOAT /
        COUNT(e.EVENT_ID),
        4
    ) AS PURCHASE_EVENT_RATIO
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u ON e.USER_ID = u.USER_ID
GROUP BY u.PLAN_TYPE
ORDER BY USER_COUNT DESC;

/*
【集計後の計算】
  - AVG_EVENTS_PER_USER：ユーザー当たりイベント数
  - PURCHASE_RATE：購入ユーザー数 / 全ユーザー数
  - PURCHASE_EVENT_RATIO：購入イベント数 / 全イベント数

これらのKPI計算は実務で頻繁に行われます
*/


-- =====================================================================
-- 集計12：GROUP BY ALL（Snowflakeの拡張機能）
-- =====================================================================

SELECT
    DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
    e.EVENT_TYPE,
    e.DEVICE_TYPE,
    COUNT(*) AS EVENT_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
GROUP BY ALL  -- SELECT の全カラムで自動的にグループ化
ORDER BY EVENT_DATE DESC, EVENT_COUNT DESC;

/*
【GROUP BY ALL】
  Snowflakeの便利な機能で、SELECTの全カラムで自動グループ化

上記は以下と同等：
  GROUP BY DATE(e.EVENT_TIMESTAMP), e.EVENT_TYPE, e.DEVICE_TYPE
*/


-- =====================================================================
-- 集計13：パフォーマンス最適化のコツ
-- =====================================================================

/*
【GROUP BY のベストプラクティス】

1. 不要なカラムはSELECTに含めない
   ✓ SELECT COUNTRY, COUNT(*)
   ❌ SELECT COUNTRY, USER_ID, COUNT(*)  (USER_ID で更にグループ化される)

2. フィルタリングはWHEREで実施
   WHERE EVENT_TIMESTAMP >= '2025-12-01'  (グループ化前に行数削減)

3. 集計後のフィルタはHAVINGで実施
   HAVING COUNT(*) > 10

4. DISTINCT の多用を避ける
   COUNT(DISTINCT USER_ID) は処理が重い場合がある

5. GROUP BY の順序
   頻繁にフィルタされるカラムを左に
   GROUP BY date, COUNTRY (← dateの方が一般的なフィルタ)

6. インデックスを活用
   GROUP BY のベースとなるカラムにインデックスがあると高速化
*/


-- =====================================================================
-- まとめ：実務でよく使うテンプレート
-- =====================================================================

SELECT
    DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
    u.COUNTRY,
    e.DEVICE_TYPE,
    e.EVENT_TYPE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
    COUNT(DISTINCT e.SESSION_ID) AS SESSION_COUNT,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT e.USER_ID), 2) AS AVG_EVENTS_PER_USER,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT e.SESSION_ID), 2) AS AVG_EVENTS_PER_SESSION,
    MAX(e.EVENT_TIMESTAMP) AS LATEST_EVENT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u ON e.USER_ID = u.USER_ID
WHERE e.EVENT_TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY
    DATE(e.EVENT_TIMESTAMP),
    u.COUNTRY,
    e.DEVICE_TYPE,
    e.EVENT_TYPE
HAVING COUNT(*) > 5  -- サンプルサイズ確保
ORDER BY EVENT_DATE DESC, EVENT_COUNT DESC;

/*
このテンプレートは実務で頻繁に使用されます：
  1. WHERE：対象期間をフィルタ
  2. JOIN：属性情報を追加
  3. GROUP BY：複数ディメンションで分割
  4. HAVING：信頼できるセグメントのみ
  5. 集計関数：KPI計算

このパターンを習得すれば、データ分析の大部分に対応できます
*/
