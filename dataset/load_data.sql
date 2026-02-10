/*
================================================================================
データロードスクリプト：CSVファイルからSnowflakeへのロード
================================================================================

このスクリプトは、generate_data.pyで生成されたCSVファイルを
Snowflakeにロードします。

前提条件：
1. create_tables.sql でテーブルが作成されている
2. users.csv, sessions.csv, raw_events.csvが生成されている
3. Snowflakeの内部ステージまたは外部ストレージ（S3等）が準備されている

実行手順：
1. CSVファイルをSnowflakeステージにアップロード
   - ローカルファイルの場合：PUT コマンド
   - S3の場合：CREATE EXTERNAL STAGE コマンド

2. このスクリプトを実行

注意：
  - 本番環境では、より詳細なエラーハンドリングとバリデーションを推奨
  - 大規模データロードの場合、COPY コマンドの並列度を調整してください
*/

-- =====================================================================
-- 1. 内部ステージの作成（ローカルファイルからロードする場合）
-- =====================================================================
-- 注：これはオプションです。すでにステージが存在する場合はスキップしてください

CREATE STAGE IF NOT EXISTS my_stage;

-- =====================================================================
-- 2. ローカルファイルのアップロード（コマンドライン実行）
-- =====================================================================
-- Snowflake CLIでこれらのコマンドを実行してください：
-- PUT file:///path/to/users.csv @my_stage/;
-- PUT file:///path/to/sessions.csv @my_stage/;
-- PUT file:///path/to/raw_events.csv @my_stage/;

-- =====================================================================
-- 3. ステージの内容確認
-- =====================================================================
-- アップロードされたファイルの確認
LIST @my_stage/;


-- =====================================================================
-- 4. users テーブルへのロード
-- =====================================================================
COPY INTO users (
    user_id,
    signup_date,
    country,
    plan_type,
    is_active
)
FROM @my_stage/users.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    DATE_FORMAT = 'YYYY-MM-DD',
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
ON_ERROR = ABORT_STATEMENT;  -- エラー時は即停止（問題を早期発見するため）

-- ロード確認
SELECT COUNT(*) AS user_count FROM users;
SELECT * FROM users LIMIT 5;


-- =====================================================================
-- 5. sessions テーブルへのロード
-- =====================================================================
COPY INTO sessions (
    session_id,
    user_id,
    session_start,
    session_end,
    page_views,
    device_type
)
FROM @my_stage/sessions.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    DATE_FORMAT = 'YYYY-MM-DD',
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
ON_ERROR = ABORT_STATEMENT;  -- エラー時は即停止（問題を早期発見するため）

-- ロード確認
SELECT COUNT(*) AS session_count FROM sessions;
SELECT * FROM sessions LIMIT 5;


-- =====================================================================
-- 6. raw_events テーブルへのロード
-- =====================================================================
COPY INTO raw_events (
    event_id,
    user_id,
    session_id,
    event_type,
    page_url,
    event_timestamp,
    device_type,
    country
)
FROM @my_stage/raw_events.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    DATE_FORMAT = 'YYYY-MM-DD',
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
ON_ERROR = ABORT_STATEMENT;  -- エラー時は即停止（問題を早期発見するため）

-- ロード確認
SELECT COUNT(*) AS event_count FROM raw_events;
SELECT * FROM raw_events LIMIT 5;


-- =====================================================================
-- 7. 全体的なデータ品質チェック
-- =====================================================================

-- テーブルサイズの確認
SELECT
    'users' AS table_name,
    COUNT(*) AS row_count
FROM users
UNION ALL
SELECT
    'sessions' AS table_name,
    COUNT(*) AS row_count
FROM sessions
UNION ALL
SELECT
    'raw_events' AS table_name,
    COUNT(*) AS row_count
FROM raw_events;


-- 外部キー制約の検証
-- sessions.user_id が users.user_id に存在するか確認
SELECT COUNT(*) AS orphaned_sessions
FROM sessions s
WHERE NOT EXISTS (
    SELECT 1 FROM users u WHERE u.user_id = s.user_id
);

-- raw_events.user_id が users.user_id に存在するか確認
SELECT COUNT(*) AS orphaned_events
FROM raw_events e
WHERE NOT EXISTS (
    SELECT 1 FROM users u WHERE u.user_id = e.user_id
);

-- raw_events.session_id が sessions.session_id に存在するか確認
SELECT COUNT(*) AS orphaned_events_by_session
FROM raw_events e
WHERE NOT EXISTS (
    SELECT 1 FROM sessions s WHERE s.session_id = e.session_id
);


-- =====================================================================
-- 8. サンプルクエリ：データの確認
-- =====================================================================

-- 国別のユーザー数
SELECT country, COUNT(*) as user_count
FROM users
GROUP BY country
ORDER BY user_count DESC;


-- イベント種別の分布
SELECT event_type, COUNT(*) as event_count
FROM raw_events
GROUP BY event_type
ORDER BY event_count DESC;


-- ユーザーごとのイベント数
SELECT user_id, COUNT(*) as event_count
FROM raw_events
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 10;


-- デバイス種別ごとのセッション統計
SELECT
    device_type,
    COUNT(*) as session_count,
    ROUND(AVG(page_views), 2) as avg_page_views,
    MAX(page_views) as max_page_views
FROM sessions
GROUP BY device_type;


SELECT '✓ データロード完了' AS message;
