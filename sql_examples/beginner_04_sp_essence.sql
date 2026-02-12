/*
================================================================================
Step D：ストアドプロシジャ体験 -「テスト困難の問題」（8分）
================================================================================

【目的】
  ストアドプロシジャ(SP)で複数ステップの処理をまとめる方法を学びます。
  同時に「SPはテスト困難・Git管理困難」という壁を体験します。

【壁4】テスト困難、Git管理困難
*/


-- =====================================================================
-- 前提：SPが更新するテーブルを先に作成
-- =====================================================================

CREATE OR REPLACE TABLE DIESELPJ_TEST.DBT_HANDSON.DAILY_SUMMARY (
    EVENT_DATE DATE,
    EVENT_COUNT INTEGER,
    UNIQUE_USERS INTEGER,
    UNIQUE_SESSIONS INTEGER,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================================
-- シンプルなストアドプロシジャ
-- =====================================================================

CREATE OR REPLACE PROCEDURE DIESELPJ_TEST.DBT_HANDSON.SP_CALCULATE_DAILY_SUMMARY()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- ステップ1：既存データをクリア
    DELETE FROM DIESELPJ_TEST.DBT_HANDSON.DAILY_SUMMARY;

    -- ステップ2：日別集計を挿入
    INSERT INTO DIESELPJ_TEST.DBT_HANDSON.DAILY_SUMMARY (
        EVENT_DATE, EVENT_COUNT, UNIQUE_USERS, UNIQUE_SESSIONS
    )
    SELECT
        DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
        COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS
    FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
    GROUP BY DATE(EVENT_TIMESTAMP);

    RETURN 'Daily summary completed';
END;
$$;

-- 実行
CALL DIESELPJ_TEST.DBT_HANDSON.SP_CALCULATE_DAILY_SUMMARY();

-- 結果確認
SELECT * FROM DIESELPJ_TEST.DBT_HANDSON.DAILY_SUMMARY
ORDER BY EVENT_DATE DESC
LIMIT 10;

/*
ストアドプロシジャとは：
  - 複数のSQLを順番に実行する「プログラム」
  - DELETE → INSERT のような複数ステップを1つにまとめられる
  - CALL で実行する
*/


-- =====================================================================
-- 実務規模のSP：3ステップの統合パイプライン（見せるだけ）
-- =====================================================================

/*
実務では、こんなSPが作られがちです：

CREATE PROCEDURE sp_run_nightly_pipeline()
AS
BEGIN
    -- ステップ1：日別集計テーブルを更新
    DELETE FROM DAILY_SUMMARY;
    INSERT INTO DAILY_SUMMARY (...) SELECT ... FROM RAW_EVENTS GROUP BY ...;

    -- ステップ2：週別集計テーブルを更新
    DELETE FROM WEEKLY_SUMMARY;
    INSERT INTO WEEKLY_SUMMARY (...) SELECT ... FROM DAILY_SUMMARY GROUP BY ...;

    -- ステップ3：アクティブユーザーテーブルを更新
    DELETE FROM ACTIVE_USERS;
    INSERT INTO ACTIVE_USERS (...) SELECT ... FROM RAW_EVENTS WHERE ...;

    RETURN 'Pipeline completed';
END;

1つのSPに全ロジックが詰め込まれています。
*/


/*
================================================================================
【壁4：テスト困難・Git管理困難】
================================================================================

SPの問題点を具体的に見てみましょう：

■ テスト困難
  - SPを実行すると DELETE + INSERT が走る（副作用がある）
  - 「ステップ2だけテストしたい」ができない
  - 結果確認は毎回 SELECT で手動チェック

■ デバッグ困難
  - ステップ2でエラーが出ても、ステップ1のDELETEは既に実行済み
  - 途中のステップの中間結果が見えない
  - エラーメッセージが不親切なことが多い

■ Git管理困難
  - SPの定義は CREATE PROCEDURE 文の中にSQL文が埋め込まれている
  - 差分（diff）が見づらい → コードレビューが困難
  - ロジックの一部だけ変更しても、SP全体を再定義する必要がある

■ 再利用困難
  - 「日別集計」のロジックを別のSPでも使いたい → コピペ
  - コピペが増える → ロジックの不整合が発生


  → dbt では各ステップが独立したSQLファイル（モデル）になる
     - 各モデルを個別にテスト可能
     - 各モデルが普通のSQLファイル → Git差分が明確
     - ref() で再利用 → コピペ不要

  dbtでの構成：
    stg_events.sql       → ステップ1（前処理）
    stg_users.sql        → ステップ1（前処理）
    daily_summary.sql    → ステップ2（日別集計）
    weekly_summary.sql   → ステップ3（週別集計）

  それぞれ独立してテスト・デバッグ・レビューできます。
*/
