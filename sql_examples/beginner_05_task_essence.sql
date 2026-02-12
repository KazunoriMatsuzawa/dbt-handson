/*
================================================================================
Step E：Task体験 -「依存管理の問題」（5分）
================================================================================

【目的】
  Snowflake Taskでジョブの定期実行と依存関係管理を学びます。
  同時に「依存関係の手動管理が破綻する」という壁を体験します。

【壁5】DAGが手動管理 → タスクが増えると破綻する
*/


-- =====================================================================
-- シンプルなTask：毎日1時にSPを実行
-- =====================================================================

CREATE OR REPLACE TASK DIESELPJ_TEST.DBT_HANDSON.TSK_DAILY_SUMMARY
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 1 * * * UTC'  -- 毎日 01:00 UTC
AS
CALL DIESELPJ_TEST.DBT_HANDSON.SP_CALCULATE_DAILY_SUMMARY();

/*
Taskとは：
  - SQLやSPを定期的に自動実行する仕組み
  - CRON式でスケジュールを指定（0 1 * * * = 毎日1時）
  - ALTER TASK ... RESUME で有効化
*/


-- =====================================================================
-- タスク間の依存関係（AFTER句）
-- =====================================================================

-- 親タスク：まず日別集計を実行
CREATE OR REPLACE TASK DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 1 * * * UTC'
AS
CALL DIESELPJ_TEST.DBT_HANDSON.SP_CALCULATE_DAILY_SUMMARY();

-- 子タスク：親が完了してから週別集計を実行
CREATE OR REPLACE TASK DIESELPJ_TEST.DBT_HANDSON.TSK_CHILD_WEEKLY
WAREHOUSE = COMPUTE_WH
AFTER DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY  -- 親タスク完了後に実行
AS
INSERT INTO DIESELPJ_TEST.DBT_HANDSON.WEEKLY_SUMMARY
    (WEEK_START, WEEK_END, EVENT_COUNT, UNIQUE_USERS)
SELECT
    DATE_TRUNC('WEEK', EVENT_DATE) AS WEEK_START,
    DATEADD(DAY, 6, DATE_TRUNC('WEEK', EVENT_DATE)) AS WEEK_END,
    SUM(EVENT_COUNT) AS EVENT_COUNT,
    SUM(UNIQUE_USERS) AS UNIQUE_USERS
FROM DIESELPJ_TEST.DBT_HANDSON.DAILY_SUMMARY
GROUP BY DATE_TRUNC('WEEK', EVENT_DATE);

/*
AFTER句：
  TSK_CHILD_WEEKLY は TSK_PARENT_DAILY の完了後に自動実行される
  → 日別集計 → 週別集計 の順序が保証される
*/


-- =====================================================================
-- タスクの管理と運用
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. タスクの一覧確認
-- ---------------------------------------------------------------------

-- 現在のスキーマのタスク一覧を表示
SHOW TASKS IN SCHEMA DIESELPJ_TEST.DBT_HANDSON;

-- 特定のタスクだけ表示（LIKE検索）
SHOW TASKS LIKE 'TSK_%' IN SCHEMA DIESELPJ_TEST.DBT_HANDSON;

-- タスクの状態を確認（started = 有効, suspended = 停止中）
SELECT
    "name" AS TASK_NAME,
    "state" AS STATE,           -- started / suspended
    "schedule" AS SCHEDULE,
    "warehouse" AS WAREHOUSE,
    "predecessors" AS PARENT_TASKS
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


-- ---------------------------------------------------------------------
-- 2. タスクの詳細情報確認
-- ---------------------------------------------------------------------

-- タスクの定義内容を確認
DESCRIBE TASK DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY;


-- ---------------------------------------------------------------------
-- 3. タスクの有効化（RESUME）
-- ---------------------------------------------------------------------

/*
重要：依存関係がある場合、子タスクから先に有効化する必要があります。

正しい順序：
  1. 子タスクをRESUME
  2. 親タスクをRESUME

理由：親を先に有効化すると、子がsuspendedのままで実行されないため
*/

-- ステップ1：子タスクを有効化
ALTER TASK DIESELPJ_TEST.DBT_HANDSON.TSK_CHILD_WEEKLY RESUME;

-- ステップ2：親タスクを有効化（これでスケジュール実行が開始される）
ALTER TASK DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY RESUME;

/*
有効化後、次回のスケジュール時刻（CRON: 0 1 * * * = 毎日01:00 UTC）に
自動実行されます。
*/


-- ---------------------------------------------------------------------
-- 4. タスクの一時停止（SUSPEND）
-- ---------------------------------------------------------------------

/*
重要：依存関係がある場合、親タスクから先に停止する必要があります。

正しい順序：
  1. 親タスクをSUSPEND（新しい実行を止める）
  2. 子タスクをSUSPEND

理由：子を先に止めると、親が実行された際に子が動かず不整合が起きる可能性
*/

-- ステップ1：親タスクを停止（スケジュール実行を停止）
ALTER TASK DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY SUSPEND;

-- ステップ2：子タスクを停止
ALTER TASK DIESELPJ_TEST.DBT_HANDSON.TSK_CHILD_WEEKLY SUSPEND;

/*
停止後は、スケジュール時刻になっても自動実行されません。
手動実行（EXECUTE TASK）は停止中でも可能です。
*/


-- ---------------------------------------------------------------------
-- 5. タスクの実行履歴確認
-- ---------------------------------------------------------------------

-- 過去の実行履歴を確認（直近10件）
SELECT
    NAME AS TASK_NAME,
    STATE,                    -- SUCCEEDED / FAILED / SKIPPED
    SCHEDULED_TIME,           -- スケジュール予定時刻
    QUERY_START_TIME,         -- 実際の実行開始時刻
    COMPLETED_TIME,           -- 完了時刻
    ERROR_CODE,               -- エラーコード（失敗時）
    ERROR_MESSAGE             -- エラーメッセージ（失敗時）
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY',
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- 全タスクの実行履歴を確認
SELECT
    DATABASE_NAME,
    SCHEMA_NAME,
    NAME AS TASK_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;


-- ---------------------------------------------------------------------
-- 6. タスクの手動実行（テスト用）
-- ---------------------------------------------------------------------

/*
注意：EXECUTE TASK は即座にタスクを実行します。
     本番データに影響するため、テスト環境で実行することを推奨します。
*/

-- タスクを手動で1回だけ実行（スケジュール待たずに即実行）
EXECUTE TASK DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY;

-- 実行結果を確認
SELECT
    NAME,
    STATE,
    QUERY_START_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY',
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
ORDER BY QUERY_START_TIME DESC
LIMIT 1;


-- ---------------------------------------------------------------------
-- 7. タスクの削除
-- ---------------------------------------------------------------------

/*
重要：依存関係がある場合、子タスクから先に削除する必要があります。

正しい順序：
  1. 子タスクをDROP
  2. 親タスクをDROP

理由：親を先に削除しようとすると、子が依存しているためエラーになる
*/

-- ステップ1：まず停止する（実行中のタスクは削除できない）
ALTER TASK DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY SUSPEND;
ALTER TASK DIESELPJ_TEST.DBT_HANDSON.TSK_CHILD_WEEKLY SUSPEND;

-- ステップ2：子タスクから削除
DROP TASK IF EXISTS DIESELPJ_TEST.DBT_HANDSON.TSK_CHILD_WEEKLY;

-- ステップ3：親タスクを削除
DROP TASK IF EXISTS DIESELPJ_TEST.DBT_HANDSON.TSK_PARENT_DAILY;

-- 削除確認
SHOW TASKS IN SCHEMA DIESELPJ_TEST.DBT_HANDSON;

/*
タスク管理のベストプラクティス：
  1. 有効化：子 → 親 の順
  2. 停止：親 → 子 の順
  3. 削除：停止 → 子 → 親 の順
  4. テストは必ずEXECUTE TASKで手動実行してから、RESUMEで自動化
  5. 実行履歴を定期的に確認してエラーを早期発見
*/


/*
================================================================================
【壁5：依存管理が手動 → タスクが増えると破綻する】
================================================================================

実務でタスクが増えると、こんな構造になります：

  TSK_CLEAN_EVENTS  （01:00 データクレンジング）
   ├─ TSK_DAILY_SUMMARY    （AFTER: 日別集計）
   │   ├─ TSK_WEEKLY_SUMMARY   （AFTER: 週別集計）
   │   └─ TSK_MONTHLY_KPI      （AFTER: 月別KPI）
   ├─ TSK_ACTIVE_USERS     （AFTER: アクティブユーザー更新）
   │   └─ TSK_CHURN_ANALYSIS   （AFTER: チャーン分析）
   └─ TSK_SESSION_SUMMARY  （AFTER: セッション集計）
       └─ TSK_FUNNEL_REPORT    （AFTER: ファネルレポート）

問題：
  1. AFTER句の指定ミスで実行順序が壊れる
     → 日別集計が未完了なのに週別集計が走る
  2. 新しいタスクを追加するとき、どこに依存させるか判断が困難
     → 全タスクの依存関係を手動で把握する必要がある
  3. 依存関係の全体像が見えない
     → ドキュメント化しないと誰もわからない（しかも陳腐化する）
  4. テストなし
     → タスクが正しい順序で動いているか確認する手段がない

  → dbt では ref() を書くだけで依存関係が自動解決される

  例：
    -- daily_summary.sql
    SELECT * FROM {{ ref('stg_events') }}  -- stg_eventsへの依存を自動認識

    -- weekly_summary.sql
    SELECT * FROM {{ ref('daily_summary') }}  -- daily_summaryへの依存を自動認識

  dbt が ref() から DAG（有向非巡回グラフ）を自動生成し、
  正しい順序で実行してくれます。
  Lineage グラフで依存関係が一目でわかります。


================================================================================
【SQLの5つの壁 まとめ】
================================================================================

Step A〜Eで体験した「SQLだけでは解決しにくい問題」：

  壁1（Step B）：CTE長大化     → ファイル分割できない
  壁2（Step C）：VIEW管理      → 変更の影響範囲がわからない
  壁3（Step C）：テスト手動     → テストが属人化、やらなくなる
  壁4（Step D）：SP複雑化      → テスト困難、Git管理困難
  壁5（Step E）：Task依存管理   → DAGが手動管理で破綻

次の2コマ目（dbt入門）で、これらの壁を1つずつ解決していきます。

  壁1 → dbtモデル分割 + ref() で解決
  壁2 → Lineage（データの系譜）で自動可視化
  壁3 → dbt test で自動テスト
  壁4 → 各モデルが独立SQLファイル → Git管理容易
  壁5 → ref() からDAGを自動生成 → 実行順序の自動管理
*/
