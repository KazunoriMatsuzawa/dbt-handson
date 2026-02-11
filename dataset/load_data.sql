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

CREATE STAGE IF NOT EXISTS MY_STAGE;

-- =====================================================================
-- 2. ローカルファイルのアップロード（コマンドライン実行）
-- =====================================================================
-- Snowflake CLIでこれらのコマンドを実行してください：
PUT file:///path/to/users.csv @MY_STAGE/ AUTO_COMPRESS=TRUE;
PUT file:///path/to/sessions.csv @MY_STAGE/ AUTO_COMPRESS=TRUE;
PUT file:///path/to/raw_events.csv @MY_STAGE/ AUTO_COMPRESS=TRUE;

-- =====================================================================
-- 3. ステージの内容確認
-- =====================================================================
-- アップロードされたファイルの確認
LIST @MY_STAGE/;


-- =====================================================================
-- 4. USERS テーブルへのロード
-- =====================================================================
COPY INTO USERS (
    USER_ID,
    SIGNUP_DATE,
    COUNTRY,
    PLAN_TYPE,
    IS_ACTIVE
)
FROM @MY_STAGE/users.csv
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
SELECT COUNT(*) AS USER_COUNT FROM USERS;
SELECT * FROM USERS LIMIT 5;


-- =====================================================================
-- 5. SESSIONS テーブルへのロード
-- =====================================================================
COPY INTO SESSIONS (
    SESSION_ID,
    USER_ID,
    SESSION_START,
    SESSION_END,
    PAGE_VIEWS,
    DEVICE_TYPE
)
FROM @MY_STAGE/sessions.csv
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
SELECT COUNT(*) AS SESSION_COUNT FROM SESSIONS;
SELECT * FROM SESSIONS LIMIT 5;


-- =====================================================================
-- 6. RAW_EVENTS テーブルへのロード
-- =====================================================================
COPY INTO RAW_EVENTS (
    EVENT_ID,
    USER_ID,
    SESSION_ID,
    EVENT_TYPE,
    PAGE_URL,
    EVENT_TIMESTAMP,
    DEVICE_TYPE,
    COUNTRY
)
FROM @MY_STAGE/raw_events.csv
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
SELECT COUNT(*) AS EVENT_COUNT FROM RAW_EVENTS;
SELECT * FROM RAW_EVENTS LIMIT 5;


-- =====================================================================
-- 7. 全体的なデータ品質チェック
-- =====================================================================

-- テーブルサイズの確認
SELECT
    'USERS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM USERS
UNION ALL
SELECT
    'SESSIONS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM SESSIONS
UNION ALL
SELECT
    'RAW_EVENTS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM RAW_EVENTS;


-- 外部キー制約の検証
-- SESSIONS.USER_ID が USERS.USER_ID に存在するか確認
SELECT COUNT(*) AS ORPHANED_SESSIONS
FROM SESSIONS s
WHERE NOT EXISTS (
    SELECT 1 FROM USERS u WHERE u.USER_ID = s.USER_ID
);

-- RAW_EVENTS.USER_ID が USERS.USER_ID に存在するか確認
SELECT COUNT(*) AS ORPHANED_EVENTS
FROM RAW_EVENTS e
WHERE NOT EXISTS (
    SELECT 1 FROM USERS u WHERE u.USER_ID = e.USER_ID
);

-- RAW_EVENTS.SESSION_ID が SESSIONS.SESSION_ID に存在するか確認
SELECT COUNT(*) AS ORPHANED_EVENTS_BY_SESSION
FROM RAW_EVENTS e
WHERE NOT EXISTS (
    SELECT 1 FROM SESSIONS s WHERE s.SESSION_ID = e.SESSION_ID
);


-- =====================================================================
-- 8. サンプルクエリ：データの確認
-- =====================================================================

-- 国別のユーザー数
SELECT COUNTRY, COUNT(*) AS USER_COUNT
FROM USERS
GROUP BY COUNTRY
ORDER BY USER_COUNT DESC;


-- イベント種別の分布
SELECT EVENT_TYPE, COUNT(*) AS EVENT_COUNT
FROM RAW_EVENTS
GROUP BY EVENT_TYPE
ORDER BY EVENT_COUNT DESC;


-- ユーザーごとのイベント数
SELECT USER_ID, COUNT(*) AS EVENT_COUNT
FROM RAW_EVENTS
GROUP BY USER_ID
ORDER BY EVENT_COUNT DESC
LIMIT 10;


-- デバイス種別ごとのセッション統計
SELECT
    DEVICE_TYPE,
    COUNT(*) AS SESSION_COUNT,
    ROUND(AVG(PAGE_VIEWS), 2) AS AVG_PAGE_VIEWS,
    MAX(PAGE_VIEWS) AS MAX_PAGE_VIEWS
FROM SESSIONS
GROUP BY DEVICE_TYPE;


SELECT '✓ データロード完了' AS MESSAGE;
