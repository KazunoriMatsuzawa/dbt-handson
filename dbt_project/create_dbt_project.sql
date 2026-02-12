/*
================================================================================
dbt on Snowflake - 実環境セットアップ
================================================================================

【説明】
  既存のSnowflake環境にdbt用のスキーマを作成するスクリプトです。
  ウェアハウス、データベース、権限は既存のものを使用します。

【前提条件】
  - データベース: DIESELPJ_TEST（既存）
  - ウェアハウス: COMPUTE_WH（既存）
  - スキーマ: DBT_HANDSON（既存 - ソースデータ格納済み）
  - ロール: SANDSHREW_PUBLIC（既存）

【実行方法】
  SANDSHREW_PUBLIC ロールで Snowflake Web UI から実行してください。
*/


-- =====================================================================
-- ステップ1：既存環境の確認
-- =====================================================================

USE ROLE SANDSHREW_PUBLIC;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DIESELPJ_TEST;

-- ウェアハウスの確認
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

-- 既存スキーマの確認（ソースデータ）
SHOW SCHEMAS LIKE 'DBT_HANDSON' IN DATABASE DIESELPJ_TEST;

-- ソーステーブルの確認（RAW_EVENTS, USERS, SESSIONS）
SHOW TABLES IN DIESELPJ_TEST.DBT_HANDSON;


-- =====================================================================
-- ステップ2：dbt出力用スキーマの作成
-- =====================================================================
--
-- ソースデータ（DBT_HANDSON）と dbt の出力を分離します。
-- dbt のレイヤー構造に対応した3つのスキーマを作成します。
--
-- 構成図：
--   DBT_HANDSON          → ソースデータ（RAW_EVENTS, USERS 等）
--   DBT_HANDSON_STAGING  → staging層（VIEW） - ソースの標準化
--   DBT_HANDSON_INTERMEDIATE → intermediate層（VIEW） - 結合・加工
--   DBT_HANDSON_MARTS    → marts層（TABLE） - ビジネス向け最終データ

-- staging層：生データの標準化（VIEW として作成される）
CREATE SCHEMA IF NOT EXISTS DIESELPJ_TEST.DBT_HANDSON_STAGING
COMMENT = 'dbt staging層 - ソースデータの標準化（VIEW）';

-- intermediate層：中間加工（VIEW として作成される）
CREATE SCHEMA IF NOT EXISTS DIESELPJ_TEST.DBT_HANDSON_INTERMEDIATE
COMMENT = 'dbt intermediate層 - 複数テーブルの結合・加工（VIEW）';

-- marts層：最終ビジネスデータ（TABLE として作成される）
CREATE SCHEMA IF NOT EXISTS DIESELPJ_TEST.DBT_HANDSON_MARTS
COMMENT = 'dbt marts層 - ビジネス向け最終データマート（TABLE）';

-- 作成確認
SHOW SCHEMAS LIKE 'DBT_HANDSON%' IN DATABASE DIESELPJ_TEST;


-- =====================================================================
-- ステップ3：dbt on Snowflake プロジェクト設定
-- =====================================================================

/*
【Snowflake Web UI でのプロジェクト作成手順】

1. Snowflake Web UI にログイン
2. Projects → "Create New Project" をクリック
3. "Develop in Git" を選択
4. Git リポジトリ URL を入力（このリポジトリ）
5. 認証を設定
6. 以下のパラメータを設定：
   - Database: DIESELPJ_TEST
   - Schema: DBT_HANDSON（デフォルトスキーマ）
   - Warehouse: COMPUTE_WH
   - Repository Path: /dbt_project
7. "Create" をクリック

【dbt が使用するスキーマの対応】
  dbt_project.yml の設定により、以下のスキーマにオブジェクトが作成されます：
  - staging モデル  → DIESELPJ_TEST.DBT_HANDSON_STAGING
  - intermediate モデル → DIESELPJ_TEST.DBT_HANDSON_INTERMEDIATE
  - marts モデル    → DIESELPJ_TEST.DBT_HANDSON_MARTS

  ※ dbt のデフォルト動作：<target_schema>_<custom_schema> の形式で
    スキーマ名が生成されます。
*/


-- =====================================================================
-- ステップ4：セットアップ完了後の確認
-- =====================================================================

-- 全体構成の確認クエリ
SELECT 'Warehouse' AS RESOURCE_TYPE, 'COMPUTE_WH' AS RESOURCE_NAME
UNION ALL
SELECT 'Database', 'DIESELPJ_TEST'
UNION ALL
SELECT 'Source Schema', 'DBT_HANDSON'
UNION ALL
SELECT 'Staging Schema', 'DBT_HANDSON_STAGING'
UNION ALL
SELECT 'Intermediate Schema', 'DBT_HANDSON_INTERMEDIATE'
UNION ALL
SELECT 'Marts Schema', 'DBT_HANDSON_MARTS';


-- =====================================================================
-- ステップ5：dbt on Snowflake 初回実行
-- =====================================================================

/*
【Snowflake Web UI での実行手順】

  Projects → [プロジェクト名] → Terminal で以下を順番に実行：

  1. dbt deps          # 依存パッケージのインストール
  2. dbt debug         # 接続確認
  3. dbt run           # モデルの実行
  4. dbt test          # テストの実行
  5. dbt docs generate # ドキュメント生成

【ビギナーコース（v2）のみ実行する場合】

  dbt run --select tag:beginner     # beginnerタグのモデルのみ実行
  dbt test --select tag:beginner    # beginnerタグのテストのみ実行

【確認】

  -- staging VIEWの確認
  SELECT * FROM DIESELPJ_TEST.DBT_HANDSON_STAGING.STG_EVENTS_V2 LIMIT 10;
  SELECT * FROM DIESELPJ_TEST.DBT_HANDSON_STAGING.STG_USERS_V2 LIMIT 10;

  -- marts TABLEの確認
  SELECT * FROM DIESELPJ_TEST.DBT_HANDSON_MARTS.DAILY_SUMMARY_V2 LIMIT 10;
*/


-- =====================================================================
-- トラブルシューティング
-- =====================================================================

/*
【よくあるエラーと解決方法】

1. "Schema does not exist"
   → ステップ2 のスキーマ作成を実行してください

2. "Object does not exist: RAW_EVENTS"
   → DBT_HANDSON スキーマにソーステーブルが存在するか確認
   → SHOW TABLES IN DIESELPJ_TEST.DBT_HANDSON;

3. "Insufficient privileges"
   → SANDSHREW_PUBLIC ロールで実行しているか確認
   → USE ROLE SANDSHREW_PUBLIC;

4. "Warehouse does not exist"
   → COMPUTE_WH が存在するか確認
   → SHOW WAREHOUSES LIKE 'COMPUTE_WH';

5. "Connection timeout"
   → ウェアハウスが一時停止している可能性
   → ALTER WAREHOUSE COMPUTE_WH RESUME;

【ログ確認】
  Snowflake UI → Admin → Query History で実行ログを確認
*/


SELECT '✓ dbt on Snowflake セットアップ完了' AS MESSAGE;
-- 次のステップ：
--   1. Snowflake UI で DBT PROJECT を作成
--   2. dbt deps でパッケージをインストール
--   3. dbt run でモデルを実行
--   4. dbt test でテストを実行
