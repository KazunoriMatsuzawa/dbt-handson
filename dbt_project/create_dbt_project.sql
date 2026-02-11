/*
================================================================================
dbt on Snowflake - プロジェクト作成コマンド
================================================================================

【説明】
  このスクリプトは、Snowflake内にdbt on Snowflakeのプロジェクトオブジェクトを
  作成するためのコマンド例を含みます。

【実行環境】
  - Snowflake エンタープライズ版以上
  - dbt on Snowflake が有効化されていること
  - 管理者権限が必要

【前提条件】
  1. このリポジトリが Git に接続されていること
  2. GitHub/GitLab等の Git プロバイダーが設定されていること
  3. 必要なウェアハウスが作成されていること
*/

-- =====================================================================
-- ステップ1：dbtプロジェクト用ウェアハウスの確認・作成
-- =====================================================================

-- dbt実行用ウェアハウスを作成
CREATE OR REPLACE WAREHOUSE DBT_WH
WITH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    SCALING_POLICY = 'STANDARD';

-- ウェアハウスの確認
SHOW WAREHOUSES LIKE 'DBT_WH';


-- =====================================================================
-- ステップ2：dbt実行用データベース・スキーマの準備
-- =====================================================================

-- データベース確認・作成
CREATE OR REPLACE DATABASE ANALYTICS;

-- スキーマ作成（層別）
CREATE OR REPLACE SCHEMA ANALYTICS.STAGING;
CREATE OR REPLACE SCHEMA ANALYTICS.INTERMEDIATE;
CREATE OR REPLACE SCHEMA ANALYTICS.MARTS;

-- スキーマ権限設定
GRANT USAGE ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.STAGING TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.MARTS TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA ANALYTICS.STAGING TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA ANALYTICS.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA ANALYTICS.MARTS TO ROLE TRANSFORMER;


-- =====================================================================
-- ステップ3：DBT PROJECT オブジェクト作成（Snowflake Native）
-- =====================================================================

/*
【注意】
  このコマンドは Snowflake UI で実行してください
  コマンドラインの SnowSQL では実行できない可能性があります

【実行場所】
  Snowflake Web UI → Projects → Create DBT Project
*/

-- 例：Git from GitHub の場合のコマンド構成
-- （実際の実行は Snowflake UI から）

/*
CREATE DBT PROJECT IF NOT EXISTS my_analytics_project
  AUTO_EXECUTE = FALSE
  EXECUTE_ON_PUSH = 'main'
  GIT_REPOSITORY = 'https://github.com/your-org/your-repo.git'
  API_INTEGRATION = 'github_api'
  REPOSITORY_PATH = '/dbt_project'
  DEFAULT_COMPUTE_POOL = 'compute_pool_default'
  DEFAULT_PACKAGE_PATH = 'build'
;
*/


-- =====================================================================
-- ステップ4：Git 統合の設定（GitHub の例）
-- =====================================================================

/*
【前提】
  GitHub の OAuth App が設定されていること
  Snowflake で API Integration が作成されていること

【実行手順】
  1. Snowflake Admin が GitHub API Integration を作成
  2. GitHub の OAuth App の認証情報を設定
  3. DBT PROJECT オブジェクトで GIT_REPOSITORY を指定
*/

-- Git Repository 状態確認（DBT PROJECT 作成後）
-- (Snowflake UI から実行)


-- =====================================================================
-- ステップ5：dbt role と権限設定
-- =====================================================================

-- dbt 実行用ロールの作成
CREATE OR REPLACE ROLE TRANSFORMER;

-- ウェアハウス権限
GRANT USAGE ON WAREHOUSE DBT_WH TO ROLE TRANSFORMER;
GRANT OPERATE ON WAREHOUSE DBT_WH TO ROLE TRANSFORMER;

-- データベース・スキーマ権限
GRANT USAGE ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.PUBLIC TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.STAGING TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.MARTS TO ROLE TRANSFORMER;

-- テーブル作成権限
GRANT CREATE TABLE ON SCHEMA ANALYTICS.STAGING TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA ANALYTICS.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA ANALYTICS.MARTS TO ROLE TRANSFORMER;

-- ビュー作成権限
GRANT CREATE VIEW ON SCHEMA ANALYTICS.STAGING TO ROLE TRANSFORMER;
GRANT CREATE VIEW ON SCHEMA ANALYTICS.INTERMEDIATE TO ROLE TRANSFORMER;

-- ユーザーにロールを付与
-- (適切なユーザー名に置き換え)
-- GRANT ROLE TRANSFORMER TO USER dbt_user;


-- =====================================================================
-- ステップ6：dbt メタデータスキーマの準備
-- =====================================================================

-- dbt メタデータ用スキーマ（オプション）
CREATE OR REPLACE SCHEMA ANALYTICS.DBT_METADATA;

-- テスト結果テーブル
CREATE OR REPLACE TABLE ANALYTICS.DBT_METADATA.DBT_TEST_RESULTS (
    TEST_NAME VARCHAR,
    MODEL_NAME VARCHAR,
    TEST_STATUS VARCHAR,
    TEST_TIMESTAMP TIMESTAMP,
    ERROR_MESSAGE VARCHAR
);

-- 実行履歴テーブル
CREATE OR REPLACE TABLE ANALYTICS.DBT_METADATA.DBT_EXECUTION_LOG (
    PROJECT_NAME VARCHAR,
    EXECUTION_ID VARCHAR,
    COMMAND VARCHAR,
    START_TIME TIMESTAMP,
    END_TIME TIMESTAMP,
    STATUS VARCHAR,
    ROWS_AFFECTED INTEGER
);


-- =====================================================================
-- ステップ7：dbt on Snowflake 実行前の確認
-- =====================================================================

-- 全体構成の確認クエリ
SELECT
    'Database' AS RESOURCE_TYPE,
    DATABASE_NAME AS RESOURCE_NAME,
    'ANALYTICS' AS VALUE
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = 'ANALYTICS'

UNION ALL

SELECT
    'Warehouse' AS RESOURCE_TYPE,
    WAREHOUSE_NAME AS RESOURCE_NAME,
    'DBT_WH' AS VALUE
FROM INFORMATION_SCHEMA.WAREHOUSES
WHERE WAREHOUSE_NAME = 'DBT_WH'

UNION ALL

SELECT
    'Role' AS RESOURCE_TYPE,
    ROLE_NAME AS RESOURCE_NAME,
    'TRANSFORMER' AS VALUE
FROM INFORMATION_SCHEMA.APPLICABLE_ROLES
WHERE ROLE_NAME = 'TRANSFORMER';


-- =====================================================================
-- ステップ8：Snowflake UI での DBT PROJECT 実行
-- =====================================================================

/*
【実行手順（Snowflake Web UI）】

1. Projects メニューを開く
2. "Create New Project" をクリック
3. "Develop in Git" を選択
4. Git リポジトリ URL を入力（このリポジトリ）
5. 認証を設定
6. Default Warehouse を DBT_WH に設定
7. "Create" をクリック

8. プロジェクト内で以下を実行：
   - dbt deps（依存関係インストール）
   - dbt debug（接続確認）
   - dbt run（モデル実行）
   - dbt test（テスト実行）
   - dbt docs generate（ドキュメント生成）

【UI から実行するコマンド】
  dbt deps
  dbt run
  dbt test
  dbt docs generate
*/


-- =====================================================================
-- ステップ9：スケジュール実行用タスク作成（オプション）
-- =====================================================================

/*
【Snowflake Task との統合】

DBT PROJECT の実行を Snowflake Task でスケジュール化：

CREATE OR REPLACE TASK ANALYTICS.RUN_DBT_DAILY
WAREHOUSE = DBT_WH
SCHEDULE = 'USING CRON 0 1 * * * UTC'
AS
EXECUTE DBT PROJECT ANALYTICS.MY_ANALYTICS_PROJECT
COMMAND = 'dbt run'
;

ALTER TASK ANALYTICS.RUN_DBT_DAILY RESUME;

【テスト実行タスク】

CREATE OR REPLACE TASK ANALYTICS.RUN_DBT_TESTS
WAREHOUSE = DBT_WH
AFTER ANALYTICS.RUN_DBT_DAILY
AS
EXECUTE DBT PROJECT ANALYTICS.MY_ANALYTICS_PROJECT
COMMAND = 'dbt test'
;

ALTER TASK ANALYTICS.RUN_DBT_TESTS RESUME;
*/


-- =====================================================================
-- ステップ10：Snowflake UI でのプロジェクト確認
-- =====================================================================

/*
【dbt on Snowflake UIで確認できる内容】

1. Project Dashboard
   - 最新の実行状況
   - 実行時間、成功/失敗数

2. DAG (Directed Acyclic Graph)
   - モデル間の依存関係を可視化

3. Lineage (系統図)
   - データの流れを可視化

4. Documentation
   - モデルのスキーマ情報
   - テストの定義

5. Execution History
   - 過去の実行履歴
   - エラーログ

【アクセス】
  Snowflake Web UI → Projects → 対象プロジェクト
*/


-- =====================================================================
-- トラブルシューティング
-- =====================================================================

/*
【よくあるエラーと解決方法】

1. "Database does not exist"
   → ANALYTICS データベースが作成されていることを確認

2. "Insufficient privileges"
   → TRANSFORMER ロールに必要な権限があることを確認

3. "Warehouse does not exist"
   → DBT_WH ウェアハウスが作成されていることを確認

4. "Git authentication failed"
   → GitHub API Integration の設定を確認

5. "Connection timeout"
   → ウェアハウスが一時停止している可能性
   → ALTER WAREHOUSE DBT_WH RESUME; で再開

【ログ確認】
  Snowflake UI → Admin → Query History で実行ログを確認
*/


-- =====================================================================
-- 最終確認
-- =====================================================================

SELECT '✓ dbt on Snowflake セットアップ完了' AS MESSAGE;
-- 次のステップ：
--   1. Snowflake UI で DBT PROJECT を作成
--   2. dbt deps でパッケージをインストール
--   3. dbt run でモデルを実行
--   4. dbt test でテストを実行
