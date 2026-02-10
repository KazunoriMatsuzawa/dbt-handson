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
  - イベントログ（raw_events）とユーザー情報（users）の結合
  - セッション情報（sessions）とユーザー属性の組み合わせ
  - 多段階の結合（chained joins）
*/

-- =====================================================================
-- INNER JOIN1：raw_events と users を結合
-- =====================================================================

SELECT
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp,
    u.signup_date,
    u.country,
    u.plan_type,
    u.is_active
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id
LIMIT 20;

/*
【INNER JOIN の動作】
  raw_events と users テーブルを user_id で結合
  両テーブルに存在するuser_idのレコードのみが結果に含まれます

実行結果：
  イベント情報 + ユーザー属性を1行で確認できます

【テーブルエイリアスの使用】
  FROM raw_events e
  INNER JOIN users u

  エイリアス（e, u）を使うことで：
  - クエリが短く、読みやすくなる
  - タイプミス（全カラム名記述）を減らせる
  - テーブル名の変更時の影響を減らせる
*/


-- =====================================================================
-- INNER JOIN2：JOIN結果をWHEREでフィルタ
-- =====================================================================

SELECT
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp,
    u.plan_type,
    u.country
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id
WHERE u.plan_type = 'premium'
  AND e.event_type = 'purchase'
  AND e.country = 'US'
ORDER BY e.event_timestamp DESC
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
-- LEFT JOIN1：raw_events と users を左結合
-- =====================================================================

SELECT
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp,
    u.signup_date,
    u.plan_type
FROM raw_events e
LEFT JOIN users u
    ON e.user_id = u.user_id
LIMIT 20;

/*
【LEFT JOIN の動作】
  左テーブル（raw_events）の全レコードが結果に含まれます
  右テーブル（users）でマッチしないレコードはNULLになります

実務での応用：
  - イベントログには存在するが、ユーザー情報には存在しないuser_id
  - データ品質チェック：不正なuser_idの検出
*/


-- =====================================================================
-- LEFT JOIN2：マッチしないレコード（NULL）の確認
-- =====================================================================

SELECT
    e.event_id,
    e.user_id,
    e.event_type,
    u.user_id AS user_id_matched,
    u.plan_type,
    u.is_active
FROM raw_events e
LEFT JOIN users u
    ON e.user_id = u.user_id
WHERE u.user_id IS NULL
LIMIT 20;

/*
実行結果：
users テーブルに存在しないuser_idを検出できます

実務での応用：
  - データ品質チェック：孤立レコード（orphaned records）の検出
  - 不正なカウント：存在しないユーザーのイベント
*/


-- =====================================================================
-- INNER JOIN vs LEFT JOIN：違いの実験
-- =====================================================================

-- パターン1：INNER JOINの件数
SELECT COUNT(*) AS inner_join_count
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id;

-- パターン2：LEFT JOINの件数
SELECT COUNT(*) AS left_join_count
FROM raw_events e
LEFT JOIN users u
    ON e.user_id = u.user_id;

/*
結果の解釈：
  inner_join_count < left_join_count の場合、
  raw_eventsに存在するが、usersに存在しないuser_idが存在

実務での応用：
  - データ検証：テーブル間の整合性確認
  - 参照整合性の確認（外部キー制約が設定されていない場合）
*/


-- =====================================================================
-- 複数テーブルの結合：raw_events + sessions + users
-- =====================================================================

SELECT
    e.event_id,
    e.user_id,
    e.session_id,
    e.event_type,
    e.event_timestamp,
    s.session_start,
    s.session_end,
    s.page_views,
    u.signup_date,
    u.plan_type
FROM raw_events e
INNER JOIN sessions s
    ON e.session_id = s.session_id
INNER JOIN users u
    ON e.user_id = u.user_id
LIMIT 20;

/*
【複数JOIN の実行順序】
  1. raw_events と sessions を session_id で結合
  2. 結果 と users を user_id で結合

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
FROM raw_events e
JOIN users u ON e.user_id = u.user_id
JOIN sessions s ON e.user_id = s.user_id
  -- ❌ 問題：e.session_id と s.session_id の関連性を確認していない

このクエリは論理的には正しいですが、
セッションIDとユーザーIDが一対一でない場合、
不正なマッチが発生する可能性があります
*/

-- ✓ 正しい方法
SELECT *
FROM raw_events e
INNER JOIN sessions s
    ON e.session_id = s.session_id
    AND e.user_id = s.user_id  -- 複合キー
INNER JOIN users u
    ON e.user_id = u.user_id
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
    u.country,
    COUNT(e.event_id) AS total_events,
    COUNT(DISTINCT e.user_id) AS unique_users,
    COUNT(DISTINCT e.session_id) AS unique_sessions
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id
GROUP BY u.country
ORDER BY total_events DESC;

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
    u.plan_type,
    COUNT(DISTINCT e.user_id) AS user_count,
    COUNT(e.event_id) AS event_count,
    ROUND(COUNT(e.event_id)::FLOAT / COUNT(DISTINCT e.user_id), 2) AS avg_events_per_user,
    ROUND(COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END)::FLOAT /
          COUNT(DISTINCT e.user_id), 4) AS purchase_conversion_rate
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id
GROUP BY u.plan_type
ORDER BY event_count DESC;

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
    e.event_id,
    e.user_id,
    u.user_id AS user_id_matched,
    u.plan_type
FROM raw_events e
RIGHT JOIN users u
    ON e.user_id = u.user_id
WHERE e.event_id IS NULL
LIMIT 20;

/*
【RIGHT JOINの説明】
  右テーブル（users）の全レコードが結果に含まれます
  左テーブル（raw_events）でマッチしないレコードはNULLになります

実務での応用：
  - ユーザーマスタに存在するが、イベントログにない（非アクティブ）ユーザーの検出

ただし、実務ではLEFT JOINで十分な場合がほとんどです
*/


-- =====================================================================
-- FULL OUTER JOIN（Snowflakeでは右結合で実装）
-- =====================================================================

-- 注：Snowflakeでは FULL OUTER JOIN が直接サポートされています
SELECT
    e.event_id,
    e.user_id AS event_user_id,
    u.user_id AS user_table_user_id,
    CASE
        WHEN e.user_id IS NULL THEN 'Users only'
        WHEN u.user_id IS NULL THEN 'Events only'
        ELSE 'Both tables'
    END AS match_status
FROM raw_events e
FULL OUTER JOIN users u
    ON e.user_id = u.user_id
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
    u.user_id,
    e.event_type
FROM users u
CROSS JOIN (SELECT DISTINCT event_type FROM raw_events) e
LIMIT 20;

このクエリは：
  users（1万件）× 6イベント種別 = 6万件のレコードを生成

実務での応用（稀）：
  - すべてのユーザーとすべてのイベント種別の組み合わせ
  - 補助テーブルとの組み合わせ（日付マスタなど）
*/


-- =====================================================================
-- セルフジョイン：同じテーブル同士の結合
-- =====================================================================

SELECT
    e1.user_id,
    e1.event_timestamp AS first_event,
    e2.event_timestamp AS second_event,
    DATEDIFF(second, e1.event_timestamp, e2.event_timestamp) AS seconds_between
FROM raw_events e1
INNER JOIN raw_events e2
    ON e1.user_id = e2.user_id
    AND e1.event_id < e2.event_id  -- 自分より前のイベントのみ
WHERE e1.event_timestamp >= DATEADD(day, -7, CURRENT_DATE())
LIMIT 20;

/*
【セルフジョイン】
同じテーブルを2つのエイリアスで参照し、結合します

実務での応用：
  - ユーザーの連続イベント分析
  - 時系列でのパターン検出（e1.event_timestamp < e2.event_timestamp）
*/


-- =====================================================================
-- JOIN のベストプラクティス
-- =====================================================================

/*
【推奨】
1. テーブルエイリアスを常に使用
   FROM raw_events e INNER JOIN users u ON e.user_id = u.user_id

2. ON句は結合前の行数削減に活用
   ON e.user_id = u.user_id AND u.is_active = TRUE

3. WHERE句は結合後のフィルタに使用
   WHERE e.event_timestamp >= '2025-12-01'

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
    DATE(e.event_timestamp) AS event_date,
    u.country,
    u.plan_type,
    COUNT(DISTINCT e.user_id) AS active_users,
    COUNT(e.event_id) AS event_count,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchases
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id
WHERE e.event_timestamp >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY DATE(e.event_timestamp), u.country, u.plan_type
ORDER BY event_date DESC, event_count DESC;

/*
このクエリは実務で頻繁に使用される形式です：
  - INNER JOIN：両テーブルに存在するレコードのみ
  - WHERE：対象期間でフィルタ
  - GROUP BY：ディメンション別に集計
  - CASE文：条件付き集計
  - ORDER BY：結果をソート

このパターンをマスターすれば、実務の大部分のクエリに対応できます
*/
