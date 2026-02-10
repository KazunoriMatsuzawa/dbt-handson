/*
================================================================================
ステップ1：基本的なSELECT、WHERE、DISTINCT
================================================================================

【目的】
  データの抽出と重複排除の基本を学びます。
  Snowflakeでのデータ確認の第一歩となるスキルです。

【学習ポイント】
  - SELECT句でカラムを指定する
  - WHERE句でフィルタリングする
  - DISTINCTで重複を排除する
  - LIMITで取得行数を制限する

【実務での応用】
  - データの初期確認
  - 特定条件のレコード抽出
  - カテゴリ別のユニーク値確認
*/

-- =====================================================================
-- 基本1：全データの確認（最初の10行）
-- =====================================================================

SELECT * FROM raw_events LIMIT 10;

/*
期待される出力（例）：
+----------+---------+------------+-----------+------------------------+-------------------+-------------+---------+
| EVENT_ID | USER_ID | SESSION_ID | EVENT_TYPE| PAGE_URL               | EVENT_TIMESTAMP   | DEVICE_TYPE | COUNTRY |
+----------+---------+------------+-----------+------------------------+-------------------+-------------+---------+
|        1 |    4523 | session_a1 | page_view | /products              | 2025-01-15 08:23  | mobile      | US      |
|        2 |    4523 | session_a1 | click     | /products/1            | 2025-01-15 08:25  | mobile      | US      |
|        3 |    1287 | session_b2 | purchase  | /checkout              | 2025-01-15 09:01  | desktop     | JP      |
+----------+---------+------------+-----------+------------------------+-------------------+-------------+---------+
（10行表示されます。実際の値はダミーデータの生成結果により異なります）
*/


-- =====================================================================
-- 基本2：必要なカラムのみ抽出
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    device_type
FROM raw_events
LIMIT 10;

/*
【メリット】
  - 必要なカラムのみ取得することでネットワーク転送量を削減
  - クエリ意図が明確になり、可読性が向上
  - 不要なカラムのデータ型変換処理を回避

【デメリット】
  - SELECT *の方が簡潔に見えるかもしれない（実務では非推奨）

【アンチパターン】
  SELECT * FROM raw_events;
  ❌ 理由：不要なカラムまで取得し、パフォーマンスが悪化
  ✓ 改善：必要なカラムのみ指定する
*/


-- =====================================================================
-- フィルタリング1：WHERE句で特定のイベント種別を抽出
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    page_url,
    event_timestamp
FROM raw_events
WHERE event_type = 'purchase'
LIMIT 20;

/*
実行結果：
購入イベント（event_type = 'purchase'）のみ表示されます。
これで、購入に至ったユーザーの行動を分析できます。
*/


-- =====================================================================
-- フィルタリング2：複数条件でのフィルタリング（AND演算子）
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    device_type,
    country,
    event_timestamp
FROM raw_events
WHERE event_type = 'purchase'
  AND device_type = 'mobile'
  AND country = 'US'
LIMIT 20;

/*
【条件の解釈】
  - event_type = 'purchase' : 購入イベント
  - device_type = 'mobile' : モバイルデバイス
  - country = 'US' : アメリカのユーザー

実務での応用例：
  - US、モバイルユーザーの購入行動分析
  - セグメント別パフォーマンス比較
*/


-- =====================================================================
-- フィルタリング3：OR演算子を使った条件
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    device_type,
    event_timestamp
FROM raw_events
WHERE (event_type = 'purchase' OR event_type = 'checkout')
  AND (device_type = 'mobile' OR device_type = 'tablet')
LIMIT 20;

/*
【条件の解釈】
  - (purchase OR checkout) : 購入またはチェックアウト
  - (mobile OR tablet) : モバイルまたはタブレット

【ベストプラクティス】
  括弧を適切に使用して、条件の優先順位を明確にする
*/


-- =====================================================================
-- フィルタリング4：IN演算子による複数値の指定
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    country,
    event_timestamp
FROM raw_events
WHERE country IN ('US', 'JP', 'GB')
  AND event_type IN ('purchase', 'sign_up')
LIMIT 20;

/*
【IN演算子のメリット】
  OR演算子の繰り返しより簡潔で読みやすい

比較：
  ❌ WHERE country = 'US' OR country = 'JP' OR country = 'GB'
  ✓ WHERE country IN ('US', 'JP', 'GB')
*/


-- =====================================================================
-- フィルタリング5：NOT演算子（除外条件）
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    page_url,
    event_timestamp
FROM raw_events
WHERE event_type NOT IN ('page_view', 'click')
LIMIT 20;

/*
実行結果：
page_view と click 以外のイベントを取得します。
（購入、サインアップ、カート追加など）
*/


-- =====================================================================
-- DISTINCT1：ユニークなイベント種別を確認
-- =====================================================================

SELECT DISTINCT event_type
FROM raw_events
ORDER BY event_type;

/*
期待される出力：
+-------------+
| EVENT_TYPE  |
+-------------+
| add_to_cart |
| checkout    |
| click       |
| page_view   |
| purchase    |
| sign_up     |
+-------------+
（6行 - 定義済みの全イベント種別が表示されます）

実務での応用：
  - テーブル内のカテゴリ値を確認
  - データ品質チェック（予期しない値がないか）
*/


-- =====================================================================
-- DISTINCT2：ユニークなデバイス・国の組み合わせ
-- =====================================================================

SELECT DISTINCT device_type, country
FROM raw_events
ORDER BY country, device_type;

/*
実行結果：
データ内に存在する device_type と country の
全ての組み合わせが表示されます。

実務での応用：
  - セグメント分析：特定の国とデバイスの組み合わせ
  - データカバレッジの確認
*/


-- =====================================================================
-- DISTINCT3：ユニークなユーザー数（COUNT + DISTINCT）
-- =====================================================================

SELECT COUNT(DISTINCT user_id) AS unique_user_count
FROM raw_events;

/*
実行結果：
raw_eventsテーブルに登場する一意なユーザー数が表示されます。

【重要な注意】
  DISTINCT は大規模データではパフォーマンス劣化の原因になります

パフォーマンス比較：
  ❌ SELECT COUNT(DISTINCT user_id) FROM raw_events;
     （50万行全体をスキャンして重複排除を実施）

  ✓ SELECT COUNT(*) FROM (
      SELECT DISTINCT user_id FROM raw_events
    );
     （WITH句を使ってステップ化）
*/


-- =====================================================================
-- DISTINCT4：複数カラムのDISTINCT
-- =====================================================================

SELECT COUNT(DISTINCT user_id, device_type) AS unique_combinations
FROM raw_events;

/*
実行結果：
user_id と device_type の一意な組み合わせ数を取得します。

実務での応用：
  - ユーザーがどのデバイスから何回アクセスしたか
  - デバイス多様性の分析
*/


-- =====================================================================
-- NULL値の処理
-- =====================================================================

SELECT
    event_id,
    user_id,
    page_url,
    event_timestamp
FROM raw_events
WHERE page_url IS NOT NULL
LIMIT 10;

/*
【NULL値チェック】
  - IS NULL : NULLである
  - IS NOT NULL : NULLでない

注意：
  WHERE page_url = NULL は動作しません
  必ず IS NULL / IS NOT NULL を使用してください
*/


-- =====================================================================
-- LIKE演算子：パターンマッチ
-- =====================================================================

SELECT
    event_id,
    user_id,
    page_url,
    event_timestamp
FROM raw_events
WHERE page_url LIKE '/products%'
LIMIT 20;

/*
【LIKE演算子のパターン】
  %：任意の文字列にマッチ
  _：任意の1文字にマッチ

例：
  '/products%' : '/products'で始まるページ
  '%checkout%' : 'checkout'を含むページ
  '/products/_' : '/products/1'、'/products/2'など
*/


-- =====================================================================
-- 日付範囲でのフィルタリング
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp
FROM raw_events
WHERE DATE(event_timestamp) BETWEEN
    DATEADD(day, -30, CURRENT_DATE()) AND CURRENT_DATE()
LIMIT 20;

/*
実行結果：
過去30日間のイベントのみ取得します。

【DATE関数の使用】
  DATE(TIMESTAMP)：タイムスタンプから日付部分を抽出
  DATEADD(day, -30, CURRENT_DATE())：30日前の日付を動的に計算

【ベストプラクティス】
  固定日付（例：'2025-12-01'）ではなく、動的な日付関数を使うことで
  データの鮮度に依存しないクエリが書けます。

他のオプション：
  YEAR(event_timestamp) = YEAR(CURRENT_DATE())
  MONTH(event_timestamp) = MONTH(CURRENT_DATE())
*/


-- =====================================================================
-- パフォーマンス最適化のコツ
-- =====================================================================

/*
【ベストプラクティス】

1. 不要なカラムの取得を避ける
   SELECT event_id, user_id, event_type FROM raw_events;

2. フィルタリングは早期に実施
   WHERE句で行数を絞ってから集計

3. DISTINCTは必要な場合のみ
   パフォーマンスの低下を招く可能性がある

4. インデックスが効いているカラムでフィルタ
   WHERE event_timestamp > '2025-12-01'（推奨）
   WHERE YEAR(event_timestamp) = 2025（避けるべき）

5. LIKE検索は前方一致を使用
   WHERE url LIKE '/products%'（効率的）
   WHERE url LIKE '%products%'（全体スキャン）
*/


-- =====================================================================
-- まとめ：多段階フィルタリングの例
-- =====================================================================

SELECT
    event_id,
    user_id,
    event_type,
    device_type,
    country,
    event_timestamp
FROM raw_events
WHERE
    -- 条件1：対象期間（過去30日間）
    DATE(event_timestamp) >= DATEADD(day, -30, CURRENT_DATE())
    -- 条件2：特定の国
    AND country IN ('US', 'JP')
    -- 条件3：対象イベント
    AND event_type IN ('purchase', 'checkout')
    -- 条件4：デバイス
    AND device_type IN ('mobile', 'desktop')
    -- 条件5：NULL除外
    AND page_url IS NOT NULL
LIMIT 20;

/*
このクエリは実務でよく使う複合フィルタリングの例です。
各条件を段階的に指定することで、可読性を高めています。
*/
