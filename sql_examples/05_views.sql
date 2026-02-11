/*
================================================================================
ステップ5：ビュー、ダイナミックテーブル
================================================================================

【目的】
  よく使うクエリを再利用可能な「仮想テーブル」として保存します。
  複数のアナリストが同じロジックを共有できます。

【学習ポイント】
  - VIEW（ビュー）：リアルタイムデータ参照
  - DYNAMIC TABLE（ダイナミックテーブル）：自動更新される事前計算済みデータ
  - ビューの利点・制限事項
  - いつどちらを使うか

【実務での応用】
  - 複雑なクエリ定義を再利用
  - データアクセス権限の制御
  - パフォーマンス最適化（ダイナミックテーブル）
  - 標準レポート定義

【補足】
  MATERIALIZED VIEW は Enterprise Edition 以上が必要です。
  本ハンズオンでは Standard Edition でも利用可能な DYNAMIC TABLE を使用します。
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
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
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
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u
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
  大規模データでのパフォーマンスが懸念される場合は DYNAMIC TABLE を検討
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
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS e
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS u ON e.USER_ID = u.USER_ID
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
   - クエリが遅い場合は DYNAMIC TABLE を検討
*/


-- =====================================================================
-- DYNAMIC TABLE（ダイナミックテーブル）
-- =====================================================================

/*
【DYNAMIC TABLE とは】
  Snowflake が提供する自動更新型のテーブルです。
  MATERIALIZED VIEW の進化版で、Standard Edition でも利用可能です。
  TARGET_LAG を指定すると、Snowflake が自動的にデータを更新します。
*/

CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_SUMMARY
  TARGET_LAG = '1 day'       -- データ鮮度：最大1日遅延で自動更新
  WAREHOUSE = COMPUTE_WH     -- 更新処理に使用するウェアハウス
AS
SELECT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS,
    COUNT(DISTINCT CASE WHEN EVENT_TYPE = 'purchase' THEN EVENT_ID END) AS PURCHASE_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP);

-- ダイナミックテーブルの確認
SELECT * FROM DT_DAILY_SUMMARY
ORDER BY EVENT_DATE DESC;

/*
【DYNAMIC TABLE の特性】
  1. 実データを物理的に保持（ディスク領域を消費）
  2. クエリ結果を事前計算・保存
  3. TARGET_LAG に基づいて Snowflake が自動更新
  4. 手動更新も可能（ALTER DYNAMIC TABLE ... REFRESH）

【用途】
  - 複雑な集計をする場合（パフォーマンス改善）
  - よくアクセスされるレポート
  - リアルタイム性より処理速度優先の場合
  - データパイプラインの構築（DT同士の依存関係を自動管理）
*/


-- =====================================================================
-- DYNAMIC TABLE の更新管理
-- =====================================================================

-- 手動で即座に更新したい場合
ALTER DYNAMIC TABLE DT_DAILY_SUMMARY REFRESH;

-- 更新後の確認
SELECT * FROM DT_DAILY_SUMMARY
ORDER BY EVENT_DATE DESC;

-- ダイナミックテーブルの更新状況を確認
SELECT *
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;

/*
【ダイナミックテーブルの更新方法】
  1. 自動更新：TARGET_LAG の設定に基づき Snowflake が自動実行（推奨）
  2. 手動更新：ALTER DYNAMIC TABLE ... REFRESH;（緊急時など）

  TARGET_LAG の設定例：
    '1 minute'  -- 1分ごと（ニアリアルタイム）
    '1 hour'    -- 1時間ごと
    '1 day'     -- 1日ごと（日次バッチ向け）
    DOWNSTREAM  -- 下流のDTが更新されるときに連動

重要：
  TARGET_LAG が短いほどデータは新鮮だが、ウェアハウスのコストが増加
  ビジネス要件に応じた TARGET_LAG 設計が必須
*/


-- =====================================================================
-- VIEW vs DYNAMIC TABLE の比較
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

【DYNAMIC TABLE】
  メリット：
    - クエリが高速（事前計算）
    - 自動更新（TARGET_LAG による管理）
    - 依存関係の自動管理（DT同士のパイプライン構築）
    - Standard Edition で利用可能
  デメリット：
    - ストレージ消費
    - データ鮮度に遅延（TARGET_LAG による）
    - ウェアハウスコストが発生

  推奨：
    - 複雑な集計を頻繁に参照
    - 大規模データセット
    - 鮮度の遅延が許容可能
    - データパイプラインを構築したい
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
-- VIEWとダイナミックテーブルの定義確認・削除
-- =====================================================================

-- ビューの一覧確認
SHOW VIEWS;

-- ダイナミックテーブルの一覧確認
SHOW DYNAMIC TABLES;

-- 特定ビューの定義確認
DESCRIBE VIEW V_DAILY_EVENTS;

-- ビューのコード確認（Snowflake独自コマンド）
SELECT GET_DDL('VIEW', 'V_DAILY_EVENTS');

-- ダイナミックテーブルの定義確認
SELECT GET_DDL('DYNAMIC_TABLE', 'DT_DAILY_SUMMARY');

-- ビュー削除（必要に応じて）
-- DROP VIEW V_DAILY_EVENTS;

-- ダイナミックテーブル削除（必要に応じて）
DROP DYNAMIC TABLE DT_DAILY_SUMMARY;


-- =====================================================================
-- DYNAMIC TABLE と MATERIALIZED VIEW の違い（参考情報）
-- =====================================================================

/*
【MATERIALIZED VIEW との比較】
  MATERIALIZED VIEW は Enterprise Edition 以上で利用可能な機能です。
  Standard Edition では DYNAMIC TABLE を代替として利用します。

  MATERIALIZED VIEW:
    - Enterprise Edition 以上が必要
    - 手動更新（ALTER MATERIALIZED VIEW ... REFRESH）
    - 単純な集計向き

  DYNAMIC TABLE:
    - Standard Edition から利用可能
    - TARGET_LAG による自動更新
    - 複雑なクエリ・JOIN・複数段パイプラインに対応
    - DT同士の依存関係を自動管理
*/


-- =====================================================================
-- VIEWのベストプラクティス
-- =====================================================================

/*
【推奨】

1. ビュー名・DT名は明確で説明的に
   ✓ V_DAILY_USER_EVENTS (何の日別集計か明確)
   ✓ DT_COUNTRY_SUMMARY (ダイナミックテーブルであることが分かる)
   ❌ v_x (意味不明)
   ❌ summary (ビューか元テーブルか不明)

2. プレフィックスの使い分け
   - V_*：通常のVIEW
   - DT_*：DYNAMIC TABLE
   - VW_*：ビューであることを明示（オプション）

3. 更新ポリシーを文書化
   VIEWはリアルタイム
   DYNAMIC TABLE は TARGET_LAG の設定値を明記

4. 複雑なビューは分割
   1つのビューが複数の責務を持たないように

5. パフォーマンス監視
   SELECT * FROM view_name の実行時間を定期確認
   遅い場合は DYNAMIC TABLE への移行検討

6. ビュー・DT定義のバージョン管理
   Git等で定義を保存
   dbt と連携する場合は dbt の models/ に含める
*/


-- =====================================================================
-- まとめ：VIEW と DYNAMIC TABLE の使い分け
-- =====================================================================

-- 【ケース1】リアルタイムで最新イベントを確認したい → VIEW
CREATE OR REPLACE VIEW V_LATEST_EVENTS AS
SELECT
    EVENT_ID,
    USER_ID,
    EVENT_TYPE,
    EVENT_TIMESTAMP
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
WHERE EVENT_TIMESTAMP >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY EVENT_TIMESTAMP DESC;

-- 参照：毎回最新のデータを取得
SELECT * FROM V_LATEST_EVENTS LIMIT 20;


-- 【ケース2】複雑な日別集計をよく参照する（パフォーマンス重視）→ DYNAMIC TABLE
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_PERFORMANCE
  TARGET_LAG = '1 day'
  WAREHOUSE = COMPUTE_WH
AS
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
SELECT * FROM DT_DAILY_PERFORMANCE
WHERE EVENT_DATE >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, PURCHASE_COUNT DESC;

-- 手動更新（緊急時のみ。通常は TARGET_LAG で自動更新される）
ALTER DYNAMIC TABLE DT_DAILY_PERFORMANCE REFRESH;



/*
このセクションで学んだポイント：
  1. VIEW：リアルタイムデータ参照用
  2. DYNAMIC TABLE：複雑集計をパフォーマンス重視で利用（自動更新付き）
  3. 使い分けはビジネス要件（鮮度 vs パフォーマンス）による
  4. TARGET_LAG でデータ鮮度とコストのバランスを調整
  5. dbt に移行する際も、このビュー / DT 設計の概念が応用される
*/
