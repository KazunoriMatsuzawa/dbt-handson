/*
================================================================================
Step A：SQL基礎ダイジェスト（12分）
================================================================================

【目的】
  SELECT / WHERE / JOIN / GROUP BY の基本を短時間で体験します。
  ここで学ぶSQLが、dbt モデルの中でもそのまま使われます。

【この後のStep B〜Eでは】
  SQLだけでは解決しにくい「5つの壁」を体験します。
*/


-- =====================================================================
-- 1. SELECT + WHERE + DISTINCT：データの確認と抽出（3分）
-- =====================================================================

-- まずデータの全体像を確認
SELECT * FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS LIMIT 10;

-- 必要なカラムだけ取得し、条件で絞り込む
SELECT
    EVENT_ID,
    USER_ID,
    EVENT_TYPE,
    EVENT_TIMESTAMP,
    COUNTRY
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
WHERE EVENT_TYPE = 'purchase'
  AND COUNTRY IN ('US', 'JP')
LIMIT 20;

-- どんなイベント種別があるか確認（重複排除）
SELECT DISTINCT EVENT_TYPE
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
ORDER BY EVENT_TYPE;

/*
ポイント：
  - SELECT *は避け、必要なカラムだけ指定する
  - WHERE句で条件を絞る（AND / IN で複数条件）
  - DISTINCTでユニーク値を確認 → データ品質チェックの第一歩
*/


-- =====================================================================
-- 2. JOIN：テーブル結合（4分）
-- =====================================================================

-- INNER JOIN：イベントログにユーザー属性を結合
SELECT
    E.EVENT_ID,
    E.USER_ID,
    E.EVENT_TYPE,
    E.EVENT_TIMESTAMP,
    U.COUNTRY,
    U.PLAN_TYPE
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS E
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS U
    ON E.USER_ID = U.USER_ID
WHERE E.EVENT_TYPE = 'purchase'
LIMIT 20;

/*
INNER JOIN：両テーブルに存在するレコードだけが結果に含まれる
  - RAW_EVENTS（イベントログ）に USERS（ユーザー属性）を追加
  - E, U はテーブルのエイリアス（短い別名）
*/

-- LEFT JOIN：マッチしないレコードも含めて確認
SELECT
    E.EVENT_ID,
    E.USER_ID,
    U.PLAN_TYPE
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS E
LEFT JOIN DIESELPJ_TEST.DBT_HANDSON.USERS U
    ON E.USER_ID = U.USER_ID
WHERE U.USER_ID IS NULL
LIMIT 20;

/*
LEFT JOIN + WHERE IS NULL：データ品質チェック
  USERSテーブルに存在しないUSER_IDのイベントを検出できる
*/


-- =====================================================================
-- 3. GROUP BY + 集計関数：データの集約（4分）
-- =====================================================================

-- 日別のイベント数を集計
SELECT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS TOTAL_EVENTS,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP)
ORDER BY EVENT_DATE DESC;

/*
GROUP BY：同じ値を持つ行をグループ化して集計
  - COUNT(*)：全行数
  - COUNT(DISTINCT ...)：ユニーク値の数
*/

-- CASE文で条件付き集計（ファネル分析の基礎）
SELECT
    U.COUNTRY,
    COUNT(*) AS TOTAL_EVENTS,
    COUNT(CASE WHEN E.EVENT_TYPE = 'page_view' THEN 1 END) AS PAGEVIEW_COUNT,
    COUNT(CASE WHEN E.EVENT_TYPE = 'purchase' THEN 1 END) AS PURCHASE_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS E
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS U ON E.USER_ID = U.USER_ID
GROUP BY U.COUNTRY
ORDER BY TOTAL_EVENTS DESC;

/*
CASE文による条件付き集計：
  1つのクエリで、イベント種別ごとの件数を横持ちで計算できる
  → ファネル分析（page_view → purchase）の基礎

これがStep Bで学ぶCTEや、dbtモデルの中核ロジックになります。
*/

-- HAVING句：集計後のフィルタリング
SELECT
    USER_ID,
    COUNT(*) AS EVENT_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY USER_ID
HAVING COUNT(*) > 100
ORDER BY EVENT_COUNT DESC;

/*
WHERE vs HAVING：
  - WHERE：GROUP BY の前（個別行のフィルタ）
  - HAVING：GROUP BY の後（集計結果のフィルタ）

=== SQL基礎ダイジェスト完了 ===

ここまでの知識で「日別サマリー」を作れます。
しかし、実務ではこのクエリを管理・自動化する必要があります。

次のStep B〜Eでは、管理・自動化しようとしたときに
SQLだけでは解決しにくい「5つの壁」を体験します。
*/
