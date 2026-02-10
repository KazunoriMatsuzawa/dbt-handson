/*
================================================================================
テーブル定義：Webアクセスログ分析用データセット
================================================================================

このスクリプトは、以下の3つのテーブルを作成します：
- raw_events: イベントログ（50万件）
- users: ユーザーマスタ（1万件）
- sessions: セッションサマリ（10万件）

実務でよくあるログデータ分析をテーマにしており、
SQLとdbtの両方でデータ変換を体験するために使用されます。
*/

-- データベース・スキーマの確認（必要に応じて修正）
USE DATABASE analytics;
USE SCHEMA public;

-- =====================================================================
-- 1. raw_events テーブル：イベントログ（生データ）
-- =====================================================================
/*
目的：
  生のアクセスログデータを保持します。
  実務では、WebサーバーログやアナリティクスAPIから取り込まれるデータです。

カラム説明：
  - event_id: イベントの一意な識別子
  - user_id: ユーザーID（usersテーブルと結合可能）
  - session_id: セッションID（sessionsテーブルと結合可能）
  - event_type: イベント種別（page_view, click, purchase等）
  - page_url: ページURL
  - event_timestamp: イベント発生日時（タイムスタンプ）
  - device_type: デバイス種別（desktop, mobile, tablet）
  - country: 国コード（2文字ISO 3166-1 alpha-2）

推奨インデックス：
  - PRIMARY KEY: event_id
  - INDEX: user_id, event_timestamp（クエリパフォーマンス向上）
*/

CREATE OR REPLACE TABLE raw_events (
    event_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    page_url VARCHAR(500),
    event_timestamp TIMESTAMP NOT NULL,
    device_type VARCHAR(20),
    country VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

-- イベントタイプのバリデーションのためのチェック制約
-- 注意：SnowflakeのCHECK制約は「情報提供のみ」であり、実際にはデータの挿入を阻止しません。
-- データ品質の保証には、dbtテスト（accepted_values）やアプリケーション側のバリデーションが必要です。
ALTER TABLE raw_events
ADD CONSTRAINT check_event_type
CHECK (event_type IN ('page_view', 'click', 'purchase', 'sign_up', 'add_to_cart', 'checkout'));


-- =====================================================================
-- 2. users テーブル：ユーザーマスタ
-- =====================================================================
/*
目的：
  ユーザー属性情報を保持します。
  raw_eventsテーブルと結合することで、
  ユーザーの属性に基づいた集計が可能になります。

カラム説明：
  - user_id: ユーザーID（主キー）
  - signup_date: 登録日（DATE型）
  - country: 国コード
  - plan_type: プランタイプ（free or premium）
  - is_active: アクティブフラグ（TRUE/FALSE）

推奨インデックス：
  - PRIMARY KEY: user_id
  - INDEX: country, plan_type（フィルタリングが頻繁な場合）
*/

CREATE OR REPLACE TABLE users (
    user_id INTEGER NOT NULL,
    signup_date DATE NOT NULL,
    country VARCHAR(2) NOT NULL,
    plan_type VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (user_id)
);

-- プランタイプのバリデーション（Snowflakeでは情報提供のみ、強制されません）
ALTER TABLE users
ADD CONSTRAINT check_plan_type
CHECK (plan_type IN ('free', 'premium'));


-- =====================================================================
-- 3. sessions テーブル：セッションサマリ
-- =====================================================================
/*
目的：
  セッション単位で集約されたデータを保持します。
  実務では、ユーザーセッションのメトリクスが事前に集計されていることが多いです。

カラム説明：
  - session_id: セッションID（主キー）
  - user_id: ユーザーID（usersテーブルと結合可能）
  - session_start: セッション開始時刻
  - session_end: セッション終了時刻
  - page_views: セッション中のページビュー数
  - device_type: デバイス種別

推奨インデックス：
  - PRIMARY KEY: session_id
  - FOREIGN KEY: user_id（usersテーブル参照）
  - INDEX: session_start（時系列分析用）
*/

CREATE OR REPLACE TABLE sessions (
    session_id VARCHAR(50) NOT NULL,
    user_id INTEGER NOT NULL,
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP NOT NULL,
    page_views INTEGER NOT NULL,
    device_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (session_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);


-- =====================================================================
-- パーティショニング・クラスタリングの提案
-- =====================================================================
/*
大規模データ（50万件以上）を扱う場合、パフォーマンス最適化のため
以下のようにパーティショニング・クラスタリングの設定を推奨します：

ALTER TABLE raw_events
CLUSTER BY (YEAR(event_timestamp), user_id);

これにより、時系列＋ユーザーIDでのクエリが高速化されます。
ただし、Snowflakeの無料版では利用できない場合があるため、
環境に応じて設定してください。
*/

-- =====================================================================
-- テーブル確認
-- =====================================================================
SHOW TABLES;

-- 各テーブルの構造確認
DESC TABLE raw_events;
DESC TABLE users;
DESC TABLE sessions;
