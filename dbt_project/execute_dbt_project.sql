/*
================================================================================
dbt on Snowflake - プロジェクト実行コマンド集
================================================================================

【説明】
  dbt on Snowflake プロジェクトの実行、監視、トラブルシューティング用の
  コマンド集です。

【実行環境】
  - Snowflake Web UI の Projects セクション
  - または SnowSQL + Snowflake CLI (snowsql)

【最初に実行すべきコマンド順序】
  1. dbt deps        # 依存パッケージのインストール
  2. dbt debug       # 接続確認
  3. dbt run         # モデルの実行
  4. dbt test        # テストの実行
  5. dbt docs generate  # ドキュメント生成
*/

-- =====================================================================
-- dbt コマンド実行例
-- =====================================================================

/*
【Snowflake Web UI での実行方法】

  Projects → [プロジェクト名] → Terminal
  上記コマンドを入力して実行

【SnowSQL での実行方法】

  snowsql -a account_identifier -u username -d database
  > EXECUTE DBT PROJECT project_name COMMAND = 'dbt run';

【Snowflake Task での実行方法】

  CREATE TASK task_name
  WAREHOUSE = DBT_WH
  SCHEDULE = '...'
  AS
  EXECUTE DBT PROJECT project_name COMMAND = 'dbt run';
*/


-- =====================================================================
-- 1. 依存パッケージのインストール
-- =====================================================================

/*
【コマンド】
dbt deps

【説明】
  dbt_packages.yml または packages.yml に定義されたパッケージをインストール

【出力例】
  Installing package dependencies
  Updating packages.yml...
  Successfully installed 0 packages in ...

【実行頻度】
  - プロジェクト初回実行時
  - packages.yml を更新した時
*/


-- =====================================================================
-- 2. 接続テスト
-- =====================================================================

/*
【コマンド】
dbt debug

【説明】
  dbt と Snowflake の接続、権限、設定をテスト

【出力例】
  dbt version: X.X.X
  snowflake version: X.X.X
  Configuration:
    profiles.yml file [OK]
    dbt_project.yml file [OK]
    Connection test [OK]

【トラブルシューティング】
  - [ERROR] が表示された場合は、設定を確認
  - profiles.yml の認証情報をチェック
  - Snowflake ユーザーの権限を確認
*/


-- =====================================================================
-- 3. モデルの解析（実行なし）
-- =====================================================================

/*
【コマンド】
dbt parse

【説明】
  モデルの構文チェック、DAG の構築（実行はしない）

【使用例】
  - CI/CD パイプラインでのテスト
  - 構文エラーの事前確認
  - ドキュメント生成前の検証
*/


-- =====================================================================
-- 4. モデルの実行（基本）
-- =====================================================================

/*
【コマンド】
dbt run

【説明】
  dbt_project.yml で定義されたすべてのモデルを順序付けで実行
  - staging/ モデル → VIEW として作成
  - intermediate/ モデル → VIEW として作成
  - marts/ モデル → TABLE として作成

【実行順序】
  1. staging レイヤー
     stg_events (VIEW)
     stg_users (VIEW)

  2. intermediate レイヤー
     int_daily_events (VIEW)

  3. marts レイヤー
     DAILY_SUMMARY (TABLE)
     WEEKLY_SUMMARY (TABLE)

【出力例】
  Running with dbt version X.X.X
  Found 5 models ...

  Completed successfully
  Done. PASS=5 WARN=0 ERROR=0

【推奨】
  初回実行時は --full-refresh で全モデルを再構築
  dbt run --full-refresh
*/


-- =====================================================================
-- 5. モデル実行（セレクティブ実行）
-- =====================================================================

/*
【特定モデルのみ実行】
dbt run -s stg_events

【説明】
  指定したモデルとその依存関係のみ実行

【使用例】
  dbt run -s staging  # staging レイヤーのみ
  dbt run -s daily_summary  # DAILY_SUMMARY とその依存モデル
  dbt run -s +daily_summary  # DAILY_SUMMARY と上流依存（依存元）

【演算子】
  stg_events           ← 単一モデル
  staging              ← タグでセレクト
  +daily_summary       ← upstream + 指定モデル
  daily_summary+       ← 指定モデル + downstream
  +daily_summary+      ← upstream + 指定 + downstream
*/


-- =====================================================================
-- 6. 完全リセット + 再実行
-- =====================================================================

/*
【コマンド】
dbt run --full-refresh

【説明】
  既存のモデル（テーブル）をドロップして、ゼロから再構築

【使用例】
  - スキーマが壊れた場合
  - データをリセットしたい場合
  - 本番環境のデータリセット（注意：本番では実行しないこと推奨）

【警告】
  既存のテーブルが削除されます。本番環境では慎重に実行してください。
*/


-- =====================================================================
-- 7. テストの実行
-- =====================================================================

/*
【すべてのテスト実行】
dbt test

【説明】
  schema.yml で定義されたすべてのテストを実行
  - unique テスト
  - not_null テスト
  - accepted_values テスト
  - relationships テスト（外部キー検証）

【テスト内容（本プロジェクト）】
  1. ソースデータテスト
     - RAW_EVENTS, USERS の主キー一意性
     - 必須フィールドの NULL チェック
     - イベント種別の値チェック

  2. ステージングモデルテスト
     - stg_events の EVENT_ID 一意性
     - stg_users の USER_ID 一意性
     - ファネルステージの値チェック

  3. マートモデルテスト
     - コンバージョンレート（0-1範囲）
     - データ鮮度チェック

【出力例】
  Executing test unique_stg_events_EVENT_ID
  PASS
  Executing test not_null_RAW_EVENTS_USER_ID
  PASS
  ...

  Completed successfully
  Done. PASS=15 WARN=0 ERROR=0

【個別テスト実行】
dbt test -s stg_events  # stg_events 関連のテストのみ
*/


-- =====================================================================
-- 8. ドキュメント生成
-- =====================================================================

/*
【コマンド】
dbt docs generate

【説明】
  プロジェクトのドキュメント HTML を生成
  - モデルスキーマ
  - テーブル・カラムの説明
  - lineage（系統図）

【出力】
  target/index.html
  Snowflake UI の "Docs" タブで表示可能

【ドキュメント内容】
  1. Project Overview
     - プロジェクト名、バージョン

  2. Models
     - stg_events, stg_users, ...
     - 各モデルのカラム説明
     - テスト定義

  3. Lineage DAG
     - モデル間の依存関係図

  4. Sources
     - RAW_EVENTS, USERS, SESSIONS
*/


-- =====================================================================
-- 9. Snapshot（変化データキャプチャ）
-- =====================================================================

/*
【コマンド】
dbt snapshot

【説明】
  時点データの履歴管理（変化分追跡）

【使用例】
  - ユーザーのプラン変更履歴
  - イベント分類の変更記録
  - コホート定義の履歴

【本プロジェクトでのスナップショット例】
  snapshots/users_snapshot.sql:

  {% snapshot users_snapshot %}
    SELECT *
    FROM {{ source('analytics', 'USERS') }}
  {% endsnapshot %}

  実行：dbt snapshot
  → users_snapshot テーブルが作成され、差分が記録される
*/


-- =====================================================================
-- 10. Freshness Check（ソースデータの鮮度チェック）
-- =====================================================================

/*
【コマンド】
dbt source freshness

【説明】
  ソーステーブルが期待された時間内に更新されているか確認

【schema.yml での設定例】
  sources:
    - name: analytics
      tables:
        - name: RAW_EVENTS
          freshness:
            warn_after: {count: 12, period: hour}
            error_after: {count: 24, period: hour}
          loaded_at_field: CREATED_AT

【実行結果例】
  Executing freshness check for database.schema.RAW_EVENTS
  WARNING: The freshness check for table "RAW_EVENTS" has failed.

【利用例】
  - ソースデータの定期更新確認
  - ETL パイプラインの監視
  - アラート通知
*/


-- =====================================================================
-- 11. 複合実行：Run + Test
-- =====================================================================

/*
【コマンド】
dbt build

【説明】
  モデル実行 → テスト実行 をまとめて実行

【実行フロー】
  1. dbt parse
  2. dbt run
  3. dbt test
  4. ドキュメント生成（オプション）

【推奨】
  - 本番環境への反映前に dbt build で検証
  - CI/CD パイプラインで dbt build を実行

【オプション】
  dbt build --select staging  # staging レイヤーのビルド
  dbt build --full-refresh     # 完全リセット + ビルド
*/


-- =====================================================================
-- 12. エラー時のトラブルシューティング
-- =====================================================================

/*
【エラー：Relation does not exist】
  原因：依存するモデルが実行されていない
  解決：dbt run -s +model_name で依存関係を再実行

【エラー：Insufficient privileges】
  原因：Snowflake ユーザーの権限不足
  解決：GRANT文で権限を付与（create_dbt_project.sql 参照）

【エラー：Warehouse suspended】
  原因：ウェアハウスが一時停止している
  解決：ALTER WAREHOUSE DBT_WH RESUME;

【エラー：Git authentication failed】
  原因：Git リポジトリの認証情報が正しくない
  解決：API Integration の設定を確認

【デバッグモード】
  dbt --debug run  # 詳細ログ出力
  dbt run --fail-fast  # 最初のエラーで停止
*/


-- =====================================================================
-- 13. 実行パフォーマンス計測
-- =====================================================================

/*
【実行時間の確認】
  Snowflake Web UI → Query History で dbt が発行した SQL を確認

【メモリ使用量の確認】
  SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE DATABASE_NAME = 'ANALYTICS'
  AND QUERY_START_TIME > CURRENT_DATE() - 7
  ORDER BY START_TIME DESC;

【ウェアハウス利用料の確認】
  SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
  WHERE WAREHOUSE_NAME = 'DBT_WH'
  ORDER BY START_TIME DESC;

【最適化提案】
  - ウェアハウスサイズを XSMALL から SMALL に変更（パフォーマンス改善）
  - モデルのマテリアライズ方法を調整
  - 不要なカラムを削除（データスキャン量削減）
*/


-- =====================================================================
-- 14. スケジュール実行設定
-- =====================================================================

/*
【毎日実行】
CREATE OR REPLACE TASK DBT_DAILY_RUN
WAREHOUSE = DBT_WH
SCHEDULE = 'USING CRON 0 1 * * * UTC'
AS
EXECUTE DBT PROJECT analytics_web_events COMMAND = 'dbt run';

ALTER TASK DBT_DAILY_RUN RESUME;

【テスト実行（毎日、実行後）】
CREATE OR REPLACE TASK DBT_DAILY_TEST
WAREHOUSE = DBT_WH
AFTER DBT_DAILY_RUN
AS
EXECUTE DBT PROJECT analytics_web_events COMMAND = 'dbt test';

ALTER TASK DBT_DAILY_TEST RESUME;

【ドキュメント更新（週1回）】
CREATE OR REPLACE TASK DBT_WEEKLY_DOCS
WAREHOUSE = DBT_WH
SCHEDULE = 'USING CRON 0 2 * * 1 UTC'
AS
EXECUTE DBT PROJECT analytics_web_events COMMAND = 'dbt docs generate';

ALTER TASK DBT_WEEKLY_DOCS RESUME;

【タスク実行履歴確認】
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'DBT_DAILY_RUN'))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
*/


-- =====================================================================
-- 15. 本番環境への推奨実行手順
-- =====================================================================

/*
【開発環境】
  1. dbt debug
  2. dbt deps
  3. dbt run --select staging
  4. dbt test
  5. dbt run --select intermediate
  6. dbt test
  7. dbt run --select marts
  8. dbt test

【ステージング環境】
  dbt build --profiles-dir profiles/staging

【本番環境】
  1. 定時実行タスク設定（深夜実行）
     CREATE TASK DBT_PROD_RUN ...

  2. テスト自動実行
     CREATE TASK DBT_PROD_TEST AFTER DBT_PROD_RUN ...

  3. エラー通知設定
     ... (Snowflake Alert 機能)

  4. ドキュメント自動更新
     CREATE TASK DBT_PROD_DOCS ...

【モニタリング】
  - Snowflake Query History
  - dbt Cloud UI（有料版）
  - 監視ダッシュボード構築
*/


-- =====================================================================
-- 最後のコマンド：全体実行確認
-- =====================================================================

/*
【推奨される実行順序】
  1. dbt deps
  2. dbt debug
  3. dbt run
  4. dbt test
  5. dbt docs generate
  6. dbt source freshness

【本番環境での確認コマンド】
  SELECT
    MODEL_NAME,
    EXECUTION_TIME,
    ROW_COUNT,
    STATUS
  FROM ANALYTICS.DBT_METADATA.DBT_EXECUTION_LOG
  WHERE EXECUTION_ID = 'latest'
  ORDER BY EXECUTION_TIME DESC;
*/

SELECT '✓ dbt on Snowflake コマンド実行準備完了' AS MESSAGE;
-- 次のステップ：
--   1. Snowflake Web UI → Projects
--   2. Terminal で dbt deps を実行
--   3. dbt run でモデルを実行
--   4. dbt test でテストを実行
--   5. dbt docs generate でドキュメント生成
