/*
================================================================================
ステップ5：ビュー、マテリアライズドビュー
================================================================================

【目的】
  よく使うクエリを再利用可能な「仮想テーブル」として保存します。
  複数のアナリストが同じロジックを共有できます。

【学習ポイント】
  - VIEW（ビュー）：リアルタイムデータ参照
  - MATERIALIZED VIEW（マテリアライズドビュー）：事前計算済みデータ
  - ビューの利点・制限事項
  - いつどちらを使うか

【実務での応用】
  - 複雑なクエリ定義を再利用
  - データアクセス権限の制御
  - パフォーマンス最適化（マテビュー）
  - 標準レポート定義
*/

-- =====================================================================
-- VIEW1：シンプルなビューの作成
-- =====================================================================

CREATE OR REPLACE VIEW v_daily_events AS
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions
FROM raw_events
GROUP BY DATE(event_timestamp);

-- ビューの確認
SELECT * FROM v_daily_events
ORDER BY event_date DESC;

/*
【VIEW の特性】
  1. 実データを保持しない（仮想テーブル）
  2. クエリ実行時にベースとなるテーブルをスキャン
  3. 常に最新データを参照
  4. ストレージ不要

【用途】
  - 複雑なクエリをシンプルなテーブル参照に変換
  - 複数ユーザーが同じロジックを共有
  - テーブル設計を隠蔽（セキュリティ）
*/


-- =====================================================================
-- VIEW2：JOINを含むビュー
-- =====================================================================

CREATE OR REPLACE VIEW v_events_with_user_info AS
SELECT
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp,
    e.device_type,
    u.country,
    u.plan_type,
    u.is_active,
    CASE
        WHEN u.plan_type = 'premium' THEN 'Premium User'
        ELSE 'Free User'
    END AS user_segment
FROM raw_events e
INNER JOIN users u
    ON e.user_id = u.user_id;

-- ビューの使用
SELECT * FROM v_events_with_user_info
WHERE event_type = 'purchase'
LIMIT 20;

/*
【複雑なビューのメリット】
  - ユーザーはシンプルなテーブル参照でよい
  - JOIN、カラム計算は自動で適用される
  - ベースのテーブル構造が変わっても、ビュー定義で吸収可能

注意：
  VIEWは毎回クエリ実行時にベーステーブルをスキャンするため、
  大規模データでのパフォーマンスが懸念される場合はMATERIALIZED VIEWを検討
*/


-- =====================================================================
-- VIEW3：複数VIEWを組み合わせる
-- =====================================================================

CREATE OR REPLACE VIEW v_country_daily_summary AS
SELECT
    DATE(e.event_timestamp) AS event_date,
    u.country,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.user_id) AS user_count,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchase_count
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
GROUP BY DATE(e.event_timestamp), u.country;

-- VIEWからのクエリ
SELECT
    event_date,
    country,
    event_count,
    user_count,
    purchase_count,
    ROUND(purchase_count::FLOAT / user_count, 4) AS purchase_rate
FROM v_country_daily_summary
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY event_date DESC, purchase_count DESC;

/*
VIEWの重要な役割：
  - ビジネスロジック（country別, 日別集計）をカプセル化
  - アナリストは purchase_rate の計算に集中できる
  - データ更新後も自動的に最新を反映
*/


-- =====================================================================
-- VIEW のアンチパターン
-- =====================================================================

/*
【避けるべきパターン】

1. 多段階のビューネスティング（View on View on View）
   ❌ v_base → v_middle → v_final → query
      各レイヤーでテーブルスキャンが発生
   ✓ 2段階までに留める

2. 不要に広い列集合
   ❌ CREATE VIEW v_all AS SELECT * FROM raw_events;
   ✓ 必要なカラムのみ

3. フィルタを含まないVIEW（スキャン範囲が大きい）
   ❌ CREATE VIEW v_all_events AS SELECT * FROM raw_events;
   ✓ 日付範囲等でフィルタを含める

4. VIEWのパフォーマンス最適化なし
   - クエリが遅い場合は MATERIALIZED VIEW を検討
*/


-- =====================================================================
-- MATERIALIZED VIEW（マテリアライズドビュー）
-- =====================================================================

CREATE OR REPLACE MATERIALIZED VIEW mv_daily_summary AS
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_id END) AS purchase_count
FROM raw_events
GROUP BY DATE(event_timestamp);

-- マテビューの確認
SELECT * FROM mv_daily_summary
ORDER BY event_date DESC;

/*
【MATERIALIZED VIEW の特性】
  1. 実データを物理的に保持（ディスク領域を消費）
  2. クエリ結果を事前計算・保存
  3. 参照時は計算済みデータを即座に返す
  4. 手動更新が必要（リアルタイムではない）

【用途】
  - 複雑な集計をする場合（パフォーマンス改善）
  - よくアクセスされるレポート
  - リアルタイム性より処理速度優先の場合
*/


-- =====================================================================
-- MATERIALIZED VIEW の更新
-- =====================================================================

-- マテビューの更新（手動トリガー）
ALTER MATERIALIZED VIEW mv_daily_summary REFRESH;

-- 更新後の確認
SELECT * FROM mv_daily_summary
ORDER BY event_date DESC;

/*
【マテビュー更新の方法】
  1. 手動更新：ALTER MATERIALIZED VIEW ... REFRESH;
  2. スケジュール更新：タスク（ステップ7で詳しく解説）
  3. 自動更新：Snowflakeの Dynamic Tables（参考資料参照）

重要：
  マテビューの更新をスキップすると、古いデータが参照される
  ビジネス要件に応じた更新スケジュール設計が必須
*/


-- =====================================================================
-- VIEW vs MATERIALIZED VIEW の比較
-- =====================================================================

/*
【VIEW】
  メリット：
    - ストレージ不要
    - 常に最新データ
    - 定義が簡単
  デメリット：
    - クエリ実行が遅い（毎回スキャン）
    - 複雑な集計に不向き

  推奨：
    - リアルタイムデータが必須
    - 小～中規模データセット
    - 参照頻度が低い

【MATERIALIZED VIEW】
  メリット：
    - クエリが高速（事前計算）
    - ディスク効率が良い（集計済み）
    - パフォーマンス安定
  デメリット：
    - ストレージ消費
    - データ鮮度に遅延
    - 更新管理が必要

  推奨：
    - 複雑な集計を頻繁に参照
    - 大規模データセット
    - 鮮度の遅延が許容可能
*/


-- =====================================================================
-- ビューの権限管理
-- =====================================================================

/*
【ビューを使ったセキュリティ】

例：
  raw_events テーブルには全カラムがあるが、
  v_events_with_user_info ビューを通じて参照させる場合

CREATE VIEW v_events_with_user_info AS
SELECT
    event_id,      -- 公開OK
    user_id,       -- 公開OK
    event_type,    -- 公開OK
    event_timestamp, -- 公開OK
    device_type,   -- 公開OK
    u.country,     -- 公開OK
    u.plan_type    -- 公開OK
    -- page_url は非公開（セキュリティ上の理由で除外）
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id;

このようにしてから：
  - 直接 raw_events へのアクセス権を制限
  - v_events_with_user_info へのアクセスのみ許可

結果：セキュアなデータアクセスが実現
*/


-- =====================================================================
-- VIEWの定義確認・削除
-- =====================================================================

-- ビューの一覧確認
SHOW VIEWS;

-- 特定ビューの定義確認
DESCRIBE VIEW v_daily_events;

-- ビューのコード確認（Snowflake独自コマンド）
SELECT GET_DDL('VIEW', 'v_daily_events');

-- ビュー削除（必要に応じて）
-- DROP VIEW v_daily_events;

-- マテビュー削除（必要に応じて）
-- DROP MATERIALIZED VIEW mv_daily_summary;


-- =====================================================================
-- Dynamic Tables（Snowflakeの最新機能・参考情報）
-- =====================================================================

/*
【Dynamic Tables】
  Snowflake の最新機能で、MATERIALIZED VIEW の進化版です

特徴：
  - 自動更新スケジュール設定可能
  - MATERIALIZED VIEW より自動化が充実
  - 依存関係の自動管理

構文（参考）：
CREATE OR REPLACE DYNAMIC TABLE dt_daily_summary
LAG = '1 day'  -- 更新頻度：1日ごと
WAREHOUSE = compute_wh
AS
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM raw_events
GROUP BY DATE(event_timestamp);

注：
  本ハンズオンでは実装しませんが、
  Snowflake のベストプラクティスとして Dynamic Tables の検討を推奨
*/


-- =====================================================================
-- VIEWのベストプラクティス
-- =====================================================================

/*
【推奨】

1. ビュー名は明確で説明的に
   ✓ v_daily_user_events (何の日別集計か明確)
   ✓ mv_country_summary (マテビューであることが分かる)
   ❌ v_x (意味不明)
   ❌ summary (ビューか元テーブルか不明)

2. プレフィックスの使い分け
   - v_*：通常のVIEW
   - mv_*：MATERIALIZED VIEW
   - vw_*：ビューであることを明示（オプション）

3. ビューの更新ポリシーを文書化
   VIEWはリアルタイム
   MATERIALIZED VIEWは更新スケジュールを明記

4. 複雑なビューは分割
   1つのビューが複数の責務を持たないように

5. パフォーマンス監視
   SELECT * FROM view_name の実行時間を定期確認
   遅い場合は MATERIALIZED VIEW への移行検討

6. ビュー定義のバージョン管理
   Git等でビュー定義を保存
   dbt と連携する場合は dbt の models/ に含める
*/


-- =====================================================================
-- まとめ：VIEW と MATERIALIZED VIEW の使い分け
-- =====================================================================

-- 【ケース1】リアルタイムで最新イベントを確認したい
CREATE OR REPLACE VIEW v_latest_events AS
SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp
FROM raw_events
WHERE event_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY event_timestamp DESC;

-- 参照：毎回最新のデータを取得
SELECT * FROM v_latest_events LIMIT 20;


-- 【ケース2】複雑な日別集計をよく参照する（パフォーマンス重視）
CREATE OR REPLACE MATERIALIZED VIEW mv_daily_performance AS
SELECT
    DATE(e.event_timestamp) AS event_date,
    u.country,
    u.plan_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT e.user_id) AS user_count,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchase_count,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT e.user_id), 2) AS avg_events_per_user,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END)::FLOAT /
        COUNT(DISTINCT e.user_id),
        4
    ) AS purchase_rate
FROM raw_events e
INNER JOIN users u ON e.user_id = u.user_id
GROUP BY DATE(e.event_timestamp), u.country, u.plan_type;

-- 参照：事前計算されたデータを即座に返す
SELECT * FROM mv_daily_performance
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY event_date DESC, purchase_count DESC;

-- 更新（定期的に実行：タスクで自動化）
ALTER MATERIALIZED VIEW mv_daily_performance REFRESH;

/*
このセクションで学んだポイント：
  1. VIEW：リアルタイムデータ参照用
  2. MATERIALIZED VIEW：複雑集計をパフォーマンス重視で利用
  3. 使い分けはビジネス要件（鮮度 vs パフォーマンス）による
  4. dbt に移行する際も、このビュー設計の概念が応用される
*/
