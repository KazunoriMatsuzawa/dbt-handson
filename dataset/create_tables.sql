/*
================================================================================
テーブル定義：Webアクセスログ分析用データセット
================================================================================

このスクリプトは、以下の3つのテーブルを作成します：
- RAW_EVENTS: イベントログ（50万件）
- USERS: ユーザーマスタ（1万件）
- SESSIONS: セッションサマリ（10万件）

実務でよくあるログデータ分析をテーマにしており、
SQLとdbtの両方でデータ変換を体験するために使用されます。
*/

-- データベース・スキーマの確認（必要に応じて修正）
USE DATABASE DIESELPJ_TEST;
USE SCHEMA DBT_HANDSON;

-- =====================================================================
-- 1. RAW_EVENTS テーブル：イベントログ（生データ）
-- =====================================================================
/*
目的：
  生のアクセスログデータを保持します。
  実務では、WebサーバーログやアナリティクスAPIから取り込まれるデータです。

カラム説明：
  - EVENT_ID: イベントの一意な識別子
  - USER_ID: ユーザーID（USERSテーブルと結合可能）
  - SESSION_ID: セッションID（SESSIONSテーブルと結合可能）
  - EVENT_TYPE: イベント種別（page_view, click, purchase等）
  - PAGE_URL: ページURL
  - EVENT_TIMESTAMP: イベント発生日時（タイムスタンプ）
  - DEVICE_TYPE: デバイス種別（desktop, mobile, tablet）
  - COUNTRY: 国コード（2文字ISO 3166-1 alpha-2）

推奨インデックス：
  - PRIMARY KEY: EVENT_ID
  - INDEX: USER_ID, EVENT_TIMESTAMP（クエリパフォーマンス向上）
*/

CREATE OR REPLACE TABLE RAW_EVENTS (
    EVENT_ID INTEGER NOT NULL,
    USER_ID INTEGER NOT NULL,
    SESSION_ID VARCHAR(50) NOT NULL,
    EVENT_TYPE VARCHAR(50) NOT NULL,
    PAGE_URL VARCHAR(500),
    EVENT_TIMESTAMP TIMESTAMP NOT NULL,
    DEVICE_TYPE VARCHAR(20),
    COUNTRY VARCHAR(2),
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (EVENT_ID)
);

-- イベントタイプのバリデーションのためのチェック制約
-- 注意：SnowflakeのCHECK制約は「情報提供のみ」であり、実際にはデータの挿入を阻止しません。
-- データ品質の保証には、dbtテスト（accepted_values）やアプリケーション側のバリデーションが必要です。
-- ALTER TABLE RAW_EVENTS
-- ADD CONSTRAINT CHECK_EVENT_TYPE
-- CHECK (EVENT_TYPE IN ('page_view', 'click', 'purchase', 'sign_up', 'add_to_cart', 'checkout'));


-- =====================================================================
-- 2. USERS テーブル：ユーザーマスタ
-- =====================================================================
/*
目的：
  ユーザー属性情報を保持します。
  RAW_EVENTSテーブルと結合することで、
  ユーザーの属性に基づいた集計が可能になります。

カラム説明：
  - USER_ID: ユーザーID（主キー）
  - SIGNUP_DATE: 登録日（DATE型）
  - COUNTRY: 国コード
  - PLAN_TYPE: プランタイプ（free or premium）
  - IS_ACTIVE: アクティブフラグ（TRUE/FALSE）

推奨インデックス：
  - PRIMARY KEY: USER_ID
  - INDEX: COUNTRY, PLAN_TYPE（フィルタリングが頻繁な場合）
*/

CREATE OR REPLACE TABLE USERS (
    USER_ID INTEGER NOT NULL,
    SIGNUP_DATE DATE NOT NULL,
    COUNTRY VARCHAR(2) NOT NULL,
    PLAN_TYPE VARCHAR(20) NOT NULL,
    IS_ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
    UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (USER_ID)
);

-- プランタイプのバリデーション（Snowflakeでは情報提供のみ、強制されません）
-- ALTER TABLE USERS
-- ADD CONSTRAINT CHECK_PLAN_TYPE
-- CHECK (PLAN_TYPE IN ('free', 'premium'));


-- =====================================================================
-- 3. SESSIONS テーブル：セッションサマリ
-- =====================================================================
/*
目的：
  セッション単位で集約されたデータを保持します。
  実務では、ユーザーセッションのメトリクスが事前に集計されていることが多いです。

カラム説明：
  - SESSION_ID: セッションID（主キー）
  - USER_ID: ユーザーID（USERSテーブルと結合可能）
  - SESSION_START: セッション開始時刻
  - SESSION_END: セッション終了時刻
  - PAGE_VIEWS: セッション中のページビュー数
  - DEVICE_TYPE: デバイス種別

推奨インデックス：
  - PRIMARY KEY: SESSION_ID
  - FOREIGN KEY: USER_ID（USERSテーブル参照）
  - INDEX: SESSION_START（時系列分析用）
*/

CREATE OR REPLACE TABLE SESSIONS (
    SESSION_ID VARCHAR(50) NOT NULL,
    USER_ID INTEGER NOT NULL,
    SESSION_START TIMESTAMP NOT NULL,
    SESSION_END TIMESTAMP NOT NULL,
    PAGE_VIEWS INTEGER NOT NULL,
    DEVICE_TYPE VARCHAR(20),
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (SESSION_ID),
    FOREIGN KEY (USER_ID) REFERENCES USERS(USER_ID)
);


-- =====================================================================
-- パーティショニング・クラスタリングの提案
-- =====================================================================
/*
大規模データ（50万件以上）を扱う場合、パフォーマンス最適化のため
以下のようにパーティショニング・クラスタリングの設定を推奨します：

ALTER TABLE RAW_EVENTS
CLUSTER BY (YEAR(EVENT_TIMESTAMP), USER_ID);

これにより、時系列＋ユーザーIDでのクエリが高速化されます。
ただし、Snowflakeの無料版では利用できない場合があるため、
環境に応じて設定してください。
*/

-- =====================================================================
-- テーブル確認
-- =====================================================================
SHOW TABLES;

-- 各テーブルの構造確認
DESC TABLE RAW_EVENTS;
DESC TABLE USERS;
DESC TABLE SESSIONS;
