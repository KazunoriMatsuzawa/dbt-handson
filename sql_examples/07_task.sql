/*
================================================================================
ステップ7：Snowflake タスク - 定期実行パイプライン
================================================================================

【目的】
  ストアドプロシジャやSQLを定期的に自動実行します。
  データパイプラインの基礎を構築します。

【学習ポイント】
  - タスク定義（CREATE TASK）
  - スケジュール設定（CRON式）
  - タスク間の依存関係
  - タスク実行の監視

【実務での応用】
  - 毎日の集計テーブル更新
  - データ品質チェックの自動実行
  - レポート自動生成

【展望】
  ステップ8以降で、これらのタスク+プロシジャを dbt で置き換えます。
  dbt の方がテスト性・保守性に優れています。
*/

-- =====================================================================
-- 前提：タスク実行に必要なウェアハウスの確認
-- =====================================================================

-- タスク実行用のウェアハウスを確認・作成
CREATE OR REPLACE WAREHOUSE IF NOT EXISTS task_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60  -- 60分後に自動停止
    AUTO_RESUME = TRUE;  -- 自動開始


-- =====================================================================
-- タスク1：シンプルな定期実行タスク（毎日実行）
-- =====================================================================

-- タスク定義
CREATE OR REPLACE TASK tsk_daily_summary
WAREHOUSE = task_wh
SCHEDULE = 'USING CRON 0 1 * * * UTC'  -- 毎日 01:00 UTC に実行
AS
CALL sp_calculate_daily_summary();

-- タスク有効化
ALTER TASK tsk_daily_summary RESUME;

-- タスク状態確認
SHOW TASKS LIKE 'tsk_daily_summary%';

/*
【CRON式の説明】
  'USING CRON 0 1 * * * UTC'
   |  | | | | |
   |  | | | | +-- 曜日（0=日曜日, 1=月曜日, ..., 6=土曜日）
   |  | | | +----- 月（1-12）
   |  | | +------- 日付（1-31）
   |  | +--------- 時間（0-23）
   |  +----------- 分（0-59）
   +-------------- UTC

例：
  0 1 * * * UTC         毎日 01:00 UTC
  0 */6 * * * UTC       6時間ごと
  0 1 * * 1 UTC         毎週月曜 01:00 UTC
  0 1 1 * * UTC         毎月1日 01:00 UTC
  0 1 * * 1-5 UTC       月曜～金曜 01:00 UTC
*/


-- =====================================================================
-- タスク2：パラメータ付きタスク（複数回実行）
-- =====================================================================

-- 国別の集計タスク
CREATE OR REPLACE TASK tsk_daily_summary_us
WAREHOUSE = task_wh
SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 毎日 02:00 UTC
AS
CALL sp_calculate_daily_summary_for_country('US');

CREATE OR REPLACE TASK tsk_daily_summary_jp
WAREHOUSE = task_wh
SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 毎日 02:00 UTC
AS
CALL sp_calculate_daily_summary_for_country('JP');

-- タスク有効化
ALTER TASK tsk_daily_summary_us RESUME;
ALTER TASK tsk_daily_summary_jp RESUME;

/*
【複数タスク実行時の注意】
  - それぞれ異なるスケジュールを設定可能
  - 同一スケジュール時は並列実行も可能（ウェアハウスのリソース依存）
  - 実行順序を制御したい場合は「タスク3」の依存関係を使用
*/


-- =====================================================================
-- タスク3：タスク間の依存関係（AFTER節）
-- =====================================================================

-- 最初に実行する親タスク
CREATE OR REPLACE TASK tsk_parent_daily_summary
WAREHOUSE = task_wh
SCHEDULE = 'USING CRON 0 1 * * * UTC'
AS
CALL sp_calculate_daily_summary();

-- 親タスク完了後に実行される子タスク
CREATE OR REPLACE TASK tsk_child_weekly_summary
WAREHOUSE = task_wh
AFTER tsk_parent_daily_summary  -- 親タスク完了待ち
AS
-- 週別集計（日別集計テーブルから生成）
INSERT INTO weekly_summary (week_start, week_end, event_count, unique_users)
SELECT
    DATE_TRUNC('WEEK', event_date) AS week_start,
    DATEADD(day, 6, DATE_TRUNC('WEEK', event_date)) AS week_end,
    SUM(event_count) AS event_count,
    SUM(unique_users) AS unique_users
FROM daily_summary
GROUP BY DATE_TRUNC('WEEK', event_date);

-- タスク有効化
ALTER TASK tsk_parent_daily_summary RESUME;
ALTER TASK tsk_child_weekly_summary RESUME;

/*
【タスク依存関係の利点】
  1. 順序保証：親が完了してから子が実行
  2. 自動スケジューリング：親の完了時刻に応じて自動調整
  3. エラー伝播：親がエラーなら子は実行されない

実務パターン：
  1. 基本集計（親）
  2. 詳細分析（子1）
  3. レポート生成（子2）
  ...のような DAG（有向非環グラフ）構造
*/


-- =====================================================================
-- タスク4：複数の子タスク（ファン・アウト）
-- =====================================================================

-- 親タスク：イベントログのクレンジング
CREATE OR REPLACE TASK tsk_etl_clean_events
WAREHOUSE = task_wh
SCHEDULE = 'USING CRON 0 1 * * * UTC'
AS
DELETE FROM raw_events
WHERE event_timestamp IS NULL OR user_id IS NULL;

-- 子タスク1：日別集計
CREATE OR REPLACE TASK tsk_etl_daily_summary
WAREHOUSE = task_wh
AFTER tsk_etl_clean_events
AS
INSERT INTO daily_summary (event_date, event_count, unique_users)
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM raw_events
GROUP BY DATE(event_timestamp);

-- 子タスク2：アクティブユーザー分析
CREATE OR REPLACE TASK tsk_etl_active_users
WAREHOUSE = task_wh
AFTER tsk_etl_clean_events
AS
INSERT INTO active_users (user_id, last_event_date, total_events)
SELECT
    user_id,
    MAX(DATE(event_timestamp)) AS last_event_date,
    COUNT(*) AS total_events
FROM raw_events
WHERE DATE(event_timestamp) >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY user_id;

-- 有効化
ALTER TASK tsk_etl_clean_events RESUME;
ALTER TASK tsk_etl_daily_summary RESUME;
ALTER TASK tsk_etl_active_users RESUME;

/*
【ファン・アウト構造】
  親 (tsk_etl_clean_events)
   ├─ 子1 (tsk_etl_daily_summary)
   └─ 子2 (tsk_etl_active_users)

利点：
  - 親が完了すれば、複数の子が並列実行可能
  - リソース効率が良い
*/


-- =====================================================================
-- タスク5：直接SQLを実行（プロシジャ不要）
-- =====================================================================

CREATE OR REPLACE TASK tsk_direct_sql_insert
WAREHOUSE = task_wh
SCHEDULE = 'USING CRON 0 3 * * * UTC'
AS
INSERT INTO daily_summary (event_date, event_count, unique_users)
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM raw_events
GROUP BY DATE(event_timestamp)
ON CONFLICT DO UPDATE SET
    event_count = EXCLUDED.event_count,
    unique_users = EXCLUDED.unique_users;

ALTER TASK tsk_direct_sql_insert RESUME;

/*
【プロシジャ vs 直接SQL】
  プロシジャ：
    - 複雑なロジック（IF、LOOP）が必要な場合
    - 複数ステップを組み合わせる場合

  直接SQL：
    - シンプルな INSERT / UPDATE
    - 可読性が高い
    - Git での管理が容易

実務では、直接SQL を推奨します。
複雑さが必要な場合は dbt への移行を検討。
*/


-- =====================================================================
-- タスク6：定期実行スケジュールの例
-- =====================================================================

/*
【よく使うスケジュール例】

1. 毎日実行
   SCHEDULE = 'USING CRON 0 1 * * * UTC'

2. 毎時間実行
   SCHEDULE = 'USING CRON 0 * * * * UTC'

3. 6時間ごと実行
   SCHEDULE = 'USING CRON 0 0,6,12,18 * * * UTC'

4. 毎週月曜 実行
   SCHEDULE = 'USING CRON 0 1 * * 1 UTC'

5. 毎月1日 実行
   SCHEDULE = 'USING CRON 0 1 1 * * UTC'

6. 時間単位でのスケジュール（Snowflake独自）
   SCHEDULE = '60 MINUTE'  -- 60分ごと
   SCHEDULE = '1 HOUR'     -- 1時間ごと
   SCHEDULE = '1 DAY'      -- 1日ごと
*/


-- =====================================================================
-- タスクの管理：確認・更新
-- =====================================================================

-- すべてのタスク確認
SHOW TASKS;

-- 特定のタスク確認
SHOW TASKS LIKE 'tsk_daily%';

-- タスクの詳細情報
DESCRIBE TASK tsk_daily_summary;

-- タスクの実行履歴確認
SELECT
    NAME,
    SCHEDULED_TIME,
    QUERY_START_TIME,
    QUERY_END_TIME,
    STATE,
    QUERY_TEXT
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'tsk_daily_summary'))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

/*
【TASK_HISTORY テーブルの見方】
  - NAME：タスク名
  - SCHEDULED_TIME：予定実行時刻
  - QUERY_START_TIME：実際の開始時刻
  - QUERY_END_TIME：終了時刻
  - STATE：成功(SUCCEEDED)、失敗(FAILED)等
  - QUERY_TEXT：実行SQL

監視とデバッグに重要です。
*/


-- =====================================================================
-- タスクの制御：一時停止・再開・削除
-- =====================================================================

-- タスク一時停止（スケジュール実行を停止）
ALTER TASK tsk_daily_summary SUSPEND;

-- タスク再開
ALTER TASK tsk_daily_summary RESUME;

-- 手動実行（テスト目的）
EXECUTE TASK tsk_daily_summary;

-- タスク削除
-- DROP TASK tsk_daily_summary;

-- 依存関係のある場合は、子タスクから削除
-- DROP TASK tsk_child_weekly_summary;
-- DROP TASK tsk_parent_daily_summary;


-- =====================================================================
-- タスクのモニタリングと通知
-- =====================================================================

/*
【本番環境での推奨】

1. 実行結果をテーブルに記録
CREATE TABLE task_execution_log (
    task_name VARCHAR,
    execution_time TIMESTAMP,
    status VARCHAR,
    error_message VARCHAR,
    duration_seconds INTEGER
);

2. 失敗時の通知（Snowflake Alert機能）
CREATE ALERT task_failure_alert
IF (EXISTS(
    SELECT 1 FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(...))
    WHERE STATE = 'FAILED'
))
THEN
    -- メール通知等（外部連携）
EXECUTE FUNCTION send_email(...);

3. ダッシュボード監視
Snowflake UI でタスク実行状況をリアルタイム確認
*/


-- =====================================================================
-- タスク実行のベストプラクティス
-- =====================================================================

/*
【推奨】

1. ウェアハウスサイズの最適化
   - XSMALL：シンプルな集計
   - SMALL：複雑な JOIN、GROUP BY
   - 上記を選択後、AUTO_SUSPEND で自動停止

2. スケジュール設定
   - ピーク時間外での実行（例：深夜）
   - 他のタスクとの競合回避

3. 依存関係の設計
   - DAG（有向非環グラフ）構造
   - 循環依存を避ける

4. 監視・ログ
   - TASK_HISTORY で定期確認
   - 失敗時の検出・通知

5. テスト計画
   - 実装時に EXECUTE TASK で手動テスト
   - SCHEDULE を本番に変更する前に検証

6. ドキュメント
   - 各タスクの目的
   - 依存関係図
   - エラー時の対応方法
*/


-- =====================================================================
-- タスク vs dbt ジョブ
-- =====================================================================

/*
【Snowflake Task（本ステップ）】
メリット：
  - Snowflake native、簡単
  - CRON で柔軟なスケジュール設定
  - 依存関係管理が可能

デメリット：
  - テスト性が低い
  - Git 連携が弱い
  - ドキュメント自動生成ができない
  - バージョン管理が難しい


【dbt Job（ステップ8以降）】
メリット：
  - テスト自動化
  - Git 完全統合
  - lineage, ドキュメント自動生成
  - チーム開発に最適

デメリット：
  - セットアップが複雑
  - dbt の学習が必要

【推奨】
  複雑な ETL パイプラインは dbt を使用
  シンプルなメンテナンス系タスクは Snowflake Task で十分
*/


-- =====================================================================
-- まとめ：SQL から dbt への移行パス
-- =====================================================================

/*
【本ハンズオン第1コマの総括】

ステップ1-7 で学んだこと：
  1. SELECT, WHERE, DISTINCT：データ抽出の基本
  2. JOIN：複数テーブル統合
  3. GROUP BY：集計の基本
  4. CTE：クエリの段階化
  5. VIEW：クエリ再利用
  6. ストアドプロシジャ：複雑ロジック
  7. タスク：定期実行

得られた実装パターン：
  1. データ抽出・フィルタリング
  2. テーブル結合・属性追加
  3. 集計・KPI計算
  4. テーブル更新・データマート構築
  5. 定期実行スケジュール

【課題認識】
  - テストが難しい
  - ドキュメント不足
  - Git管理が複雑
  - デバッグが困難

【第2コマ（dbt）での改善】
  - これらの SQL ロジックを dbt に変換
  - テスト・ドキュメント自動化
  - Git 完全統合
  - パフォーマンス最適化

準備完了。次はステップ8：dbt on Snowflake！
*/
