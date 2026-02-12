/*
================================================================================
dbt on Snowflake - プロジェクト実行コマンド集
================================================================================

【説明】
  dbt on Snowflake プロジェクトの実行、監視、トラブルシューティング用の
  コマンド集です。

【実行環境】
  - データベース: DIESELPJ_TEST
  - ウェアハウス: COMPUTE_WH
  - ロール: SANDSHREW_ADMIN
  - Snowflake Web UI の Projects セクション

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

  snowsql -a ww30191.ap-northeast-1.aws -u username -d DIESELPJ_TEST
  > EXECUTE DBT PROJECT project_name COMMAND = 'dbt run';

【Snowflake Task での実行方法】

  CREATE TASK task_name
  WAREHOUSE = COMPUTE_WH
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
  packages.yml に定義されたパッケージをインストール

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
  - DIESELPJ_TEST データベースへのアクセス権を確認
  - COMPUTE_WH ウェアハウスの権限を確認
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
  - staging/ モデル → DIESELPJ_TEST.DBT_HANDSON_STAGING に VIEW として作成
  - intermediate/ モデル → DIESELPJ_TEST.DBT_HANDSON_INTERMEDIATE に VIEW として作成
  - marts/ モデル → DIESELPJ_TEST.DBT_HANDSON_MARTS に TABLE として作成

【実行順序（全モデル実行時）】
  1. staging レイヤー
     stg_events (VIEW)
     stg_users (VIEW)
     stg_events_v2 (VIEW)
     stg_users_v2 (VIEW)

  2. intermediate レイヤー
     int_daily_events (VIEW)

  3. marts レイヤー
     daily_summary (TABLE)
     daily_summary_v2 (TABLE)
     weekly_summary (TABLE)

【推奨】
  初回実行時は --full-refresh で全モデルを再構築
  dbt run --full-refresh
*/


-- =====================================================================
-- 5. モデル実行（セレクティブ実行）
-- =====================================================================

/*
【特定モデルのみ実行】
dbt run -s stg_events_v2

【ビギナーコース（v2）のみ実行】
dbt run --select tag:beginner

【レイヤー指定実行】
  dbt run -s staging           # staging レイヤーのみ
  dbt run -s daily_summary_v2  # daily_summary_v2 のみ
  dbt run -s +daily_summary_v2 # daily_summary_v2 と上流依存

【演算子】
  stg_events_v2        ← 単一モデル
  tag:beginner         ← タグでセレクト
  +daily_summary_v2    ← upstream + 指定モデル
  daily_summary_v2+    ← 指定モデル + downstream
  +daily_summary_v2+   ← upstream + 指定 + downstream
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

【警告】
  既存のテーブルが削除されます。
*/


-- =====================================================================
-- 7. テストの実行
-- =====================================================================

/*
【すべてのテスト実行】
dbt test

【ビギナーコース（v2）のみテスト】
dbt test --select tag:beginner

【テスト内容（本プロジェクト）】
  1. ソースデータテスト
     - RAW_EVENTS, USERS の主キー一意性
     - 必須フィールドの NULL チェック
     - イベント種別の値チェック

  2. ステージングモデルテスト
     - stg_events_v2 の EVENT_ID 一意性
     - stg_users_v2 の USER_ID 一意性

  3. マートモデルテスト
     - daily_summary_v2 の NOT NULL チェック

【個別テスト実行】
dbt test -s stg_events_v2  # stg_events_v2 関連のテストのみ
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
     - stg_events_v2, stg_users_v2, daily_summary_v2 等
     - 各モデルのカラム説明
     - テスト定義

  3. Lineage DAG
     - モデル間の依存関係図

  4. Sources
     - RAW_EVENTS, USERS, SESSIONS（DIESELPJ_TEST.DBT_HANDSON）
*/


-- =====================================================================
-- 9. 複合実行：Run + Test
-- =====================================================================

/*
【コマンド】
dbt build

【説明】
  モデル実行 → テスト実行 をまとめて実行

【ビギナーコースのみビルド】
  dbt build --select tag:beginner

【オプション】
  dbt build --select staging  # staging レイヤーのビルド
  dbt build --full-refresh     # 完全リセット + ビルド
*/


-- =====================================================================
-- 10. 実行結果の確認
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

【アクセス】
  Snowflake Web UI → Projects → 対象プロジェクト
*/

-- 実行結果の確認クエリ（モデル実行後に使用）

-- staging VIEW の確認
SELECT * FROM DIESELPJ_TEST.DBT_HANDSON_STAGING.STG_EVENTS_V2 LIMIT 10;
SELECT * FROM DIESELPJ_TEST.DBT_HANDSON_STAGING.STG_USERS_V2 LIMIT 10;

-- marts TABLE の確認
SELECT * FROM DIESELPJ_TEST.DBT_HANDSON_MARTS.DAILY_SUMMARY_V2 LIMIT 10;


-- =====================================================================
-- 11. エラー時のトラブルシューティング
-- =====================================================================

/*
【エラー：Relation does not exist】
  原因：依存するモデルが実行されていない
  解決：dbt run -s +model_name で依存関係を再実行

【エラー：Object does not exist: RAW_EVENTS】
  原因：ソーステーブルが見つからない
  解決：SHOW TABLES IN DIESELPJ_TEST.DBT_HANDSON; で確認

【エラー：Insufficient privileges】
  原因：Snowflake ユーザーの権限不足
  解決：SANDSHREW_ADMIN ロールで実行しているか確認

【エラー：Warehouse suspended】
  原因：ウェアハウスが一時停止している
  解決：ALTER WAREHOUSE COMPUTE_WH RESUME;

【デバッグモード】
  dbt --debug run  # 詳細ログ出力
  dbt run --fail-fast  # 最初のエラーで停止
*/


-- =====================================================================
-- 12. スケジュール実行設定（オプション）
-- =====================================================================

/*
【毎日実行】
CREATE OR REPLACE TASK DIESELPJ_TEST.DBT_HANDSON.DBT_DAILY_RUN
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 1 * * * Asia/Tokyo'
AS
EXECUTE DBT PROJECT analytics_web_events COMMAND = 'dbt run';

ALTER TASK DIESELPJ_TEST.DBT_HANDSON.DBT_DAILY_RUN RESUME;

【テスト実行（毎日、実行後）】
CREATE OR REPLACE TASK DIESELPJ_TEST.DBT_HANDSON.DBT_DAILY_TEST
WAREHOUSE = COMPUTE_WH
AFTER DIESELPJ_TEST.DBT_HANDSON.DBT_DAILY_RUN
AS
EXECUTE DBT PROJECT analytics_web_events COMMAND = 'dbt test';

ALTER TASK DIESELPJ_TEST.DBT_HANDSON.DBT_DAILY_TEST RESUME;

【タスク実行履歴確認】
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'DBT_DAILY_RUN'))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;
*/


SELECT '✓ dbt on Snowflake コマンド実行準備完了' AS MESSAGE;
-- 次のステップ：
--   1. Snowflake Web UI → Projects
--   2. Terminal で dbt deps を実行
--   3. dbt run でモデルを実行
--   4. dbt test でテストを実行
--   5. dbt docs generate でドキュメント生成
