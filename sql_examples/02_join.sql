/*
================================================================================
ステップ2：JOIN（結合）- 複数テーブルの結合
================================================================================

【目的】
  複数のテーブルを結合してデータを統合します。
  実務での分析は、ほとんどの場合、複数テーブルの結合から始まります。

【学習ポイント】
  - INNER JOIN：両テーブルに存在するレコードのみ
  - LEFT JOIN：左テーブルの全レコード + 右テーブルのマッチ結果
  - 結合キー（ON句）の指定
  - 結合後のフィルタリング

【実務での応用】
  - イベントログ（RAW_EVENTS）とユーザー情報（USERS）の結合
  - セッション情報（SESSIONS）とユーザー属性の組み合わせ
  - 多段階の結合（chained joins）
*/

-- =====================================================================
-- INNER JOIN1：RAW_EVENTS と USERS を結合
-- =====================================================================

SELECT
    e.EVENT_ID,
    e.USER_ID,
    e.EVENT_TYPE,
    e.EVENT_TIMESTAMP,
    u.SIGNUP_DATE,
    u.COUNTRY,
    u.PLAN_TYPE,
    u.IS_ACTIVE
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
LIMIT 20;

/*
【INNER JOIN の動作】
  RAW_EVENTS と USERS テーブルを USER_ID で結合
  両テーブルに存在するUSER_IDのレコードのみが結果に含まれます

実行結果：
  イベント情報 + ユーザー属性を1行で確認できます

【テーブルエイリアスの使用】
  FROM RAW_EVENTS e
  INNER JOIN USERS u

  エイリアス（e, u）を使うことで：
  - クエリが短く、読みやすくなる
  - タイプミス（全カラム名記述）を減らせる
  - テーブル名の変更時の影響を減らせる
*/


-- =====================================================================
-- INNER JOIN2：JOIN結果をWHEREでフィルタ
-- =====================================================================

SELECT
    e.EVENT_ID,
    e.USER_ID,
    e.EVENT_TYPE,
    e.EVENT_TIMESTAMP,
    u.PLAN_TYPE,
    u.COUNTRY
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
WHERE u.PLAN_TYPE = 'premium'
  AND e.EVENT_TYPE = 'purchase'
  AND e.COUNTRY = 'US'
ORDER BY e.EVENT_TIMESTAMP DESC
LIMIT 20;

/*
実務での応用：
  米国のプレミアム会員による購入イベントの分析

【JOIN + WHERE の順序】
  1. INNER JOIN：テーブルを結合
  2. WHERE：結合後のデータをフィルタ

パフォーマンスのコツ：
  可能な限り結合前にWHEREで行数を減らしましょう
*/


-- =====================================================================
-- LEFT JOIN1：RAW_EVENTS と USERS を左結合
-- =====================================================================

SELECT
    e.EVENT_ID,
    e.USER_ID,
    e.EVENT_TYPE,
    e.EVENT_TIMESTAMP,
    u.SIGNUP_DATE,
    u.PLAN_TYPE
FROM RAW_EVENTS e
LEFT JOIN USERS u
    ON e.USER_ID = u.USER_ID
LIMIT 20;

/*
【LEFT JOIN の動作】
  左テーブル（RAW_EVENTS）の全レコードが結果に含まれます
  右テーブル（USERS）でマッチしないレコードはNULLになります

実務での応用：
  - イベントログには存在するが、ユーザー情報には存在しないUSER_ID
  - データ品質チェック：不正なUSER_IDの検出
*/


-- =====================================================================
-- LEFT JOIN2：マッチしないレコード（NULL）の確認
-- =====================================================================

SELECT
    e.EVENT_ID,
    e.USER_ID,
    e.EVENT_TYPE,
    u.USER_ID AS USER_ID_MATCHED,
    u.PLAN_TYPE,
    u.IS_ACTIVE
FROM RAW_EVENTS e
LEFT JOIN USERS u
    ON e.USER_ID = u.USER_ID
WHERE u.USER_ID IS NULL
LIMIT 20;

/*
実行結果：
USERS テーブルに存在しないUSER_IDを検出できます

実務での応用：
  - データ品質チェック：孤立レコード（orphaned records）の検出
  - 不正なカウント：存在しないユーザーのイベント
*/


-- =====================================================================
-- INNER JOIN vs LEFT JOIN：違いの実験
-- =====================================================================

-- パターン1：INNER JOINの件数
SELECT COUNT(*) AS INNER_JOIN_COUNT
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID;

-- パターン2：LEFT JOINの件数
SELECT COUNT(*) AS LEFT_JOIN_COUNT
FROM RAW_EVENTS e
LEFT JOIN USERS u
    ON e.USER_ID = u.USER_ID;

/*
結果の解釈：
  INNER_JOIN_COUNT < LEFT_JOIN_COUNT の場合、
  RAW_EVENTSに存在するが、USERSに存在しないUSER_IDが存在

実務での応用：
  - データ検証：テーブル間の整合性確認
  - 参照整合性の確認（外部キー制約が設定されていない場合）
*/


-- =====================================================================
-- 複数テーブルの結合：RAW_EVENTS + SESSIONS + USERS
-- =====================================================================

SELECT
    e.EVENT_ID,
    e.USER_ID,
    e.SESSION_ID,
    e.EVENT_TYPE,
    e.EVENT_TIMESTAMP,
    s.SESSION_START,
    s.SESSION_END,
    s.PAGE_VIEWS,
    u.SIGNUP_DATE,
    u.PLAN_TYPE
FROM RAW_EVENTS e
INNER JOIN SESSIONS s
    ON e.SESSION_ID = s.SESSION_ID
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
LIMIT 20;

/*
【複数JOIN の実行順序】
  1. RAW_EVENTS と SESSIONS を SESSION_ID で結合
  2. 結果 と USERS を USER_ID で結合

【注意点】
  結合順序はパフォーマンスに影響
  統計情報が有効なら、Snowflakeが最適な順序を選択
*/


-- =====================================================================
-- 結合キーの一致チェック（ON句のベストプラクティス）
-- =====================================================================

-- ❌ アンチパターン：結合条件が不足
/*
SELECT *
FROM RAW_EVENTS e
JOIN USERS u ON e.USER_ID = u.USER_ID
JOIN SESSIONS s ON e.USER_ID = s.USER_ID
  -- ❌ 問題：e.SESSION_ID と s.SESSION_ID の関連性を確認していない

このクエリは論理的には正しいですが、
セッションIDとユーザーIDが一対一でない場合、
不正なマッチが発生する可能性があります
*/

-- ✓ 正しい方法
SELECT *
FROM RAW_EVENTS e
INNER JOIN SESSIONS s
    ON e.SESSION_ID = s.SESSION_ID
    AND e.USER_ID = s.USER_ID  -- 複合キー
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
LIMIT 20;

/*
【複合キーでの結合】
  複数のカラムで結合条件を指定すると、
  より正確なマッチングが可能になります
*/


-- =====================================================================
-- 国別のイベント数を集計（結合 + 集計）
-- =====================================================================

SELECT
    u.COUNTRY,
    COUNT(e.EVENT_ID) AS TOTAL_EVENTS,
    COUNT(DISTINCT e.USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT e.SESSION_ID) AS UNIQUE_SESSIONS
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
GROUP BY u.COUNTRY
ORDER BY TOTAL_EVENTS DESC;

/*
実行結果：
  国別のイベント集計が表示されます

このクエリは以下の情報を提供：
  - 国別の総イベント数
  - 国別のアクティブユーザー数
  - 国別のセッション数

実務での応用：
  - 地域別パフォーマンス分析
  - 地域別のユーザー規模把握
*/


-- =====================================================================
-- プランタイプ別のイベント分析
-- =====================================================================

SELECT
    u.PLAN_TYPE,
    COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
    COUNT(e.EVENT_ID) AS EVENT_COUNT,
    ROUND(COUNT(e.EVENT_ID)::FLOAT / COUNT(DISTINCT e.USER_ID), 2) AS AVG_EVENTS_PER_USER,
    ROUND(COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END)::FLOAT /
          COUNT(DISTINCT e.USER_ID), 4) AS PURCHASE_CONVERSION_RATE
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
GROUP BY u.PLAN_TYPE
ORDER BY EVENT_COUNT DESC;

/*
このクエリは以下の分析を実施：
  - プランタイプ別のユーザー数
  - プランタイプ別のイベント数
  - ユーザーあたりの平均イベント数
  - 購入コンバージョン率（購入イベント / ユーザー数）

実務での応用：
  - 有料会員 vs 無料会員の行動比較
  - プラン別のビジネスメトリクス分析
*/


-- =====================================================================
-- RIGHT JOIN（参考：Snowflakeでも使用可能）
-- =====================================================================

SELECT
    e.EVENT_ID,
    e.USER_ID,
    u.USER_ID AS USER_ID_MATCHED,
    u.PLAN_TYPE
FROM RAW_EVENTS e
RIGHT JOIN USERS u
    ON e.USER_ID = u.USER_ID
WHERE e.EVENT_ID IS NULL
LIMIT 20;

/*
【RIGHT JOINの説明】
  右テーブル（USERS）の全レコードが結果に含まれます
  左テーブル（RAW_EVENTS）でマッチしないレコードはNULLになります

実務での応用：
  - ユーザーマスタに存在するが、イベントログにない（非アクティブ）ユーザーの検出

ただし、実務ではLEFT JOINで十分な場合がほとんどです
*/


-- =====================================================================
-- FULL OUTER JOIN（Snowflakeでは右結合で実装）
-- =====================================================================

-- 注：Snowflakeでは FULL OUTER JOIN が直接サポートされています
SELECT
    e.EVENT_ID,
    e.USER_ID AS EVENT_USER_ID,
    u.USER_ID AS USER_TABLE_USER_ID,
    CASE
        WHEN e.USER_ID IS NULL THEN 'Users only'
        WHEN u.USER_ID IS NULL THEN 'Events only'
        ELSE 'Both tables'
    END AS MATCH_STATUS
FROM RAW_EVENTS e
FULL OUTER JOIN USERS u
    ON e.USER_ID = u.USER_ID
LIMIT 20;

/*
【FULL OUTER JOINの説明】
  両テーブルの全レコードが結果に含まれます
  マッチしない側のカラムはNULLになります

実務での応用：
  - 両テーブルのデータの完全な照合
  - データ同期の確認
  ただし、実務ではほとんど使用しません
*/


-- =====================================================================
-- CROSS JOIN（直積）- 注意が必要
-- =====================================================================

/*
注意：CROSS JOINは実務ではほぼ使用しません

SELECT
    u.USER_ID,
    e.EVENT_TYPE
FROM USERS u
CROSS JOIN (SELECT DISTINCT EVENT_TYPE FROM RAW_EVENTS) e
LIMIT 20;

このクエリは：
  USERS（1万件）× 6イベント種別 = 6万件のレコードを生成

実務での応用（稀）：
  - すべてのユーザーとすべてのイベント種別の組み合わせ
  - 補助テーブルとの組み合わせ（日付マスタなど）
*/


-- =====================================================================
-- セルフジョイン：同じテーブル同士の結合
-- =====================================================================

SELECT
    e1.USER_ID,
    e1.EVENT_TIMESTAMP AS FIRST_EVENT,
    e2.EVENT_TIMESTAMP AS SECOND_EVENT,
    DATEDIFF(second, e1.EVENT_TIMESTAMP, e2.EVENT_TIMESTAMP) AS SECONDS_BETWEEN
FROM RAW_EVENTS e1
INNER JOIN RAW_EVENTS e2
    ON e1.USER_ID = e2.USER_ID
    AND e1.EVENT_ID < e2.EVENT_ID  -- 自分より前のイベントのみ
WHERE e1.EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_DATE())
LIMIT 20;

/*
【セルフジョイン】
同じテーブルを2つのエイリアスで参照し、結合します

実務での応用：
  - ユーザーの連続イベント分析
  - 時系列でのパターン検出（e1.EVENT_TIMESTAMP < e2.EVENT_TIMESTAMP）
*/


-- =====================================================================
-- JOIN のベストプラクティス
-- =====================================================================

/*
【推奨】
1. テーブルエイリアスを常に使用
   FROM RAW_EVENTS e INNER JOIN USERS u ON e.USER_ID = u.USER_ID

2. ON句は結合前の行数削減に活用
   ON e.USER_ID = u.USER_ID AND u.IS_ACTIVE = TRUE

3. WHERE句は結合後のフィルタに使用
   WHERE e.EVENT_TIMESTAMP >= '2025-12-01'

4. 複合キーでの結合を慎重に設計
   確認：各テーブルの主キー、外部キー制約

5. 結合するテーブルの統計情報を保持
   ANALYZE TABLE でOptimizer用の統計情報を更新

6. パフォーマンスモニタリング
   EXPLAIN で実行計画を確認
*/


-- =====================================================================
-- まとめ：実務で最もよく使うパターン
-- =====================================================================

SELECT
    DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
    u.COUNTRY,
    u.PLAN_TYPE,
    COUNT(DISTINCT e.USER_ID) AS ACTIVE_USERS,
    COUNT(e.EVENT_ID) AS EVENT_COUNT,
    COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END) AS PURCHASES
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID
WHERE e.EVENT_TIMESTAMP >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY DATE(e.EVENT_TIMESTAMP), u.COUNTRY, u.PLAN_TYPE
ORDER BY EVENT_DATE DESC, EVENT_COUNT DESC;

/*
このクエリは実務で頻繁に使用される形式です：
  - INNER JOIN：両テーブルに存在するレコードのみ
  - WHERE：対象期間でフィルタ
  - GROUP BY：ディメンション別に集計
  - CASE文：条件付き集計
  - ORDER BY：結果をソート

このパターンをマスターすれば、実務の大部分のクエリに対応できます
*/
