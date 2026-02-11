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

CREATE OR REPLACE VIEW V_DAILY_EVENTS AS
SELECT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS
FROM RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP);

-- ビューの確認
SELECT * FROM V_DAILY_EVENTS
ORDER BY EVENT_DATE DESC;

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

CREATE OR REPLACE VIEW V_EVENTS_WITH_USER_INFO AS
SELECT
    e.EVENT_ID,
    e.USER_ID,
    e.EVENT_TYPE,
    e.EVENT_TIMESTAMP,
    e.DEVICE_TYPE,
    u.COUNTRY,
    u.PLAN_TYPE,
    u.IS_ACTIVE,
    CASE
        WHEN u.PLAN_TYPE = 'premium' THEN 'Premium User'
        ELSE 'Free User'
    END AS USER_SEGMENT
FROM RAW_EVENTS e
INNER JOIN USERS u
    ON e.USER_ID = u.USER_ID;

-- ビューの使用
SELECT * FROM V_EVENTS_WITH_USER_INFO
WHERE EVENT_TYPE = 'purchase'
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

CREATE OR REPLACE VIEW V_COUNTRY_DAILY_SUMMARY AS
SELECT
    DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
    u.COUNTRY,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
    COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END) AS PURCHASE_COUNT
FROM RAW_EVENTS e
INNER JOIN USERS u ON e.USER_ID = u.USER_ID
GROUP BY DATE(e.EVENT_TIMESTAMP), u.COUNTRY;

-- VIEWからのクエリ
SELECT
    EVENT_DATE,
    COUNTRY,
    EVENT_COUNT,
    USER_COUNT,
    PURCHASE_COUNT,
    ROUND(PURCHASE_COUNT::FLOAT / USER_COUNT, 4) AS PURCHASE_RATE
FROM V_COUNTRY_DAILY_SUMMARY
WHERE EVENT_DATE >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, PURCHASE_COUNT DESC;

/*
VIEWの重要な役割：
  - ビジネスロジック（COUNTRY別, 日別集計）をカプセル化
  - アナリストは PURCHASE_RATE の計算に集中できる
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
   ❌ CREATE VIEW v_all AS SELECT * FROM RAW_EVENTS;
   ✓ 必要なカラムのみ

3. フィルタを含まないVIEW（スキャン範囲が大きい）
   ❌ CREATE VIEW v_all_events AS SELECT * FROM RAW_EVENTS;
   ✓ 日付範囲等でフィルタを含める

4. VIEWのパフォーマンス最適化なし
   - クエリが遅い場合は MATERIALIZED VIEW を検討
*/


-- =====================================================================
-- MATERIALIZED VIEW（マテリアライズドビュー）
-- =====================================================================

CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_SUMMARY AS
SELECT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS,
    COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'purchase' THEN EVENT_ID END) AS PURCHASE_COUNT
FROM RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP);

-- マテビューの確認
SELECT * FROM MV_DAILY_SUMMARY
ORDER BY EVENT_DATE DESC;

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
ALTER MATERIALIZED VIEW MV_DAILY_SUMMARY REFRESH;

-- 更新後の確認
SELECT * FROM MV_DAILY_SUMMARY
ORDER BY EVENT_DATE DESC;

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
  RAW_EVENTS テーブルには全カラムがあるが、
  V_EVENTS_WITH_USER_INFO ビューを通じて参照させる場合

CREATE VIEW V_EVENTS_WITH_USER_INFO AS
SELECT
    EVENT_ID,      -- 公開OK
    USER_ID,       -- 公開OK
    EVENT_TYPE,    -- 公開OK
    EVENT_TIMESTAMP, -- 公開OK
    DEVICE_TYPE,   -- 公開OK
    u.COUNTRY,     -- 公開OK
    u.PLAN_TYPE    -- 公開OK
    -- PAGE_URL は非公開（セキュリティ上の理由で除外）
FROM RAW_EVENTS e
INNER JOIN USERS u ON e.USER_ID = u.USER_ID;

このようにしてから：
  - 直接 RAW_EVENTS へのアクセス権を制限
  - V_EVENTS_WITH_USER_INFO へのアクセスのみ許可

結果：セキュアなデータアクセスが実現
*/


-- =====================================================================
-- VIEWの定義確認・削除
-- =====================================================================

-- ビューの一覧確認
SHOW VIEWS;

-- 特定ビューの定義確認
DESCRIBE VIEW V_DAILY_EVENTS;

-- ビューのコード確認（Snowflake独自コマンド）
SELECT GET_DDL('VIEW', 'V_DAILY_EVENTS');

-- ビュー削除（必要に応じて）
-- DROP VIEW V_DAILY_EVENTS;

-- マテビュー削除（必要に応じて）
-- DROP MATERIALIZED VIEW MV_DAILY_SUMMARY;


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
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_SUMMARY
LAG = '1 day'  -- 更新頻度：1日ごと
WAREHOUSE = compute_wh
AS
SELECT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS
FROM RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP);

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
   ✓ V_DAILY_USER_EVENTS (何の日別集計か明確)
   ✓ MV_COUNTRY_SUMMARY (マテビューであることが分かる)
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
CREATE OR REPLACE VIEW V_LATEST_EVENTS AS
SELECT
    EVENT_ID,
    USER_ID,
    EVENT_TYPE,
    EVENT_TIMESTAMP
FROM RAW_EVENTS
WHERE EVENT_TIMESTAMP >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY EVENT_TIMESTAMP DESC;

-- 参照：毎回最新のデータを取得
SELECT * FROM V_LATEST_EVENTS LIMIT 20;


-- 【ケース2】複雑な日別集計をよく参照する（パフォーマンス重視）
CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_PERFORMANCE AS
SELECT
    DATE(e.EVENT_TIMESTAMP) AS EVENT_DATE,
    u.COUNTRY,
    u.PLAN_TYPE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT e.USER_ID) AS USER_COUNT,
    COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END) AS PURCHASE_COUNT,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT e.USER_ID), 2) AS AVG_EVENTS_PER_USER,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.EVENT_TYPE = 'purchase' THEN e.EVENT_ID END)::FLOAT /
        COUNT(DISTINCT e.USER_ID),
        4
    ) AS PURCHASE_RATE
FROM RAW_EVENTS e
INNER JOIN USERS u ON e.USER_ID = u.USER_ID
GROUP BY DATE(e.EVENT_TIMESTAMP), u.COUNTRY, u.PLAN_TYPE;

-- 参照：事前計算されたデータを即座に返す
SELECT * FROM MV_DAILY_PERFORMANCE
WHERE EVENT_DATE >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, PURCHASE_COUNT DESC;

-- 更新（定期的に実行：タスクで自動化）
ALTER MATERIALIZED VIEW MV_DAILY_PERFORMANCE REFRESH;

/*
このセクションで学んだポイント：
  1. VIEW：リアルタイムデータ参照用
  2. MATERIALIZED VIEW：複雑集計をパフォーマンス重視で利用
  3. 使い分けはビジネス要件（鮮度 vs パフォーマンス）による
  4. dbt に移行する際も、このビュー設計の概念が応用される
*/
