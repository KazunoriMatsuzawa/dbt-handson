/*
================================================================================
ステップ6：ストアドプロシジャ
================================================================================

【目的】
  複数ステップの処理をまとめ、定期実行パイプラインの基礎を構築します。
  SQLでのプログラミング的な制御フローを実装します。

【学習ポイント】
  - プロシジャの定義と実行
  - 変数、制御フロー（IF、LOOP）
  - トランザクション管理
  - エラーハンドリング

【実務での応用】
  - データ加工パイプライン
  - 複数テーブルの同時更新
  - 定期実行ジョブの基盤

【警告】
  ストアドプロシジャは強力ですが、保守性・テスト性に課題があります。
  実務では dbt への移行を強く推奨します（ステップ8以降で詳説）
*/

-- =====================================================================
-- 前提：プロシジャが参照するテーブルの作成
-- =====================================================================
-- 注意：プロシジャを定義・実行する前に、参照先テーブルが必要です

CREATE OR REPLACE TABLE daily_summary (
    event_date DATE,
    event_count INTEGER,
    unique_users INTEGER,
    unique_sessions INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE daily_summary_by_country (
    event_date DATE,
    country VARCHAR,
    event_count INTEGER,
    unique_users INTEGER,
    purchase_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE weekly_summary (
    week_start DATE,
    week_end DATE,
    event_count INTEGER,
    unique_users INTEGER,
    purchase_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE active_users (
    user_id INTEGER,
    last_event_date DATE,
    total_events INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- =====================================================================
-- プロシジャ1：シンプルな集計プロシジャ
-- =====================================================================

CREATE OR REPLACE PROCEDURE sp_calculate_daily_summary()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- ステップ1：既存テーブルをクリア（あれば）
    DELETE FROM daily_summary;

    -- ステップ2：日別集計を挿入
    INSERT INTO daily_summary (
        event_date,
        event_count,
        unique_users,
        unique_sessions
    )
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT session_id) AS unique_sessions
    FROM raw_events
    GROUP BY DATE(event_timestamp);

    -- ステップ3：完了メッセージを返す
    RETURN '✓ Daily summary calculation completed successfully';
END;
$$;

-- プロシジャの実行
CALL sp_calculate_daily_summary();

/*
【プロシジャの特性】
  1. 複数のSQLステートメントを順序付けて実行
  2. 変数、制御フロー（IF、LOOP）が使用可能
  3. トランザクション管理（COMMIT、ROLLBACK）
  4. エラーハンドリング可能

【メリット】
  - 複雑なロジックを1つのプロシジャにまとめられる
  - 定期実行が容易（タスクから呼び出し可能）
  - データベース側での実行（ネットワークオーバーヘッド最小）

【デメリット】
  - テストが難しい（実行時に副作用が発生）
  - バージョン管理が煩雑
  - デバッグが困難（ログが限定的）
  - SQLの知識が必須（開発者向け）
*/


-- =====================================================================
-- プロシジャ2：入力パラメータを持つプロシジャ
-- =====================================================================

CREATE OR REPLACE PROCEDURE sp_calculate_daily_summary_for_country(
    p_country VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- 指定国のデータのみ処理
    INSERT INTO daily_summary_by_country (
        event_date,
        country,
        event_count,
        unique_users,
        purchase_count
    )
    SELECT
        DATE(e.event_timestamp) AS event_date,
        p_country AS country,
        COUNT(*) AS event_count,
        COUNT(DISTINCT e.user_id) AS unique_users,
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchase_count
    FROM raw_events e
    INNER JOIN users u ON e.user_id = u.user_id
    WHERE u.country = p_country
    GROUP BY DATE(e.event_timestamp);

    RETURN '✓ Processing completed for country: ' || p_country;
END;
$$;

-- パラメータ付きで実行
CALL sp_calculate_daily_summary_for_country('US');
CALL sp_calculate_daily_summary_for_country('JP');

/*
【パラメータ】
  p_country：処理対象の国コード

  || で文字列連結

実務での応用：
  - 対象期間、対象国等をパラメータ化
  - 同じロジックを異なる入力で複数実行
*/


-- =====================================================================
-- プロシジャ3：変数とIF文を使った制御フロー
-- =====================================================================

CREATE OR REPLACE PROCEDURE sp_calculate_and_validate_daily_summary()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_record_count INTEGER;
    v_today_events INTEGER;
    v_status_message VARCHAR;
BEGIN
    -- ステップ1：既存テーブルをクリア
    DELETE FROM daily_summary;

    -- ステップ2：集計実行
    INSERT INTO daily_summary (
        event_date,
        event_count,
        unique_users
    )
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM raw_events
    GROUP BY DATE(event_timestamp);

    -- ステップ3：挿入件数を取得
    SELECT COUNT(*) INTO v_record_count FROM daily_summary;

    -- ステップ4：今日のイベント数を取得
    SELECT COUNT(*) INTO v_today_events FROM raw_events
    WHERE DATE(event_timestamp) = CURRENT_DATE();

    -- ステップ5：バリデーション
    IF v_record_count > 0 THEN
        SET v_status_message = '✓ Summary inserted: ' || v_record_count || ' days';
    ELSE
        SET v_status_message = '✗ Error: No summary data inserted';
    END IF;

    IF v_today_events > 0 THEN
        SET v_status_message = v_status_message || ' | Today events: ' || v_today_events;
    ELSE
        SET v_status_message = v_status_message || ' | Warning: No events today';
    END IF;

    RETURN v_status_message;
END;
$$;

-- 実行
CALL sp_calculate_and_validate_daily_summary();

/*
【変数の宣言・利用】
  DECLARE v_変数名 型;
  SET v_変数名 = 値;
  SELECT ... INTO v_変数名 FROM ...;

【IF-ELSE文】
  IF 条件 THEN
      処理
  ELSE
      別の処理
  END IF;

実務での応用：
  - バリデーション（データ品質チェック）
  - エラーハンドリング
  - 条件分岐処理
*/


-- =====================================================================
-- プロシジャ4：複数ステップの統合パイプライン
-- =====================================================================

CREATE OR REPLACE PROCEDURE sp_run_nightly_data_pipeline()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_step_1_rows INTEGER;
    v_step_2_rows INTEGER;
    v_step_3_rows INTEGER;
BEGIN
    SET v_start_time = CURRENT_TIMESTAMP();

    -- ========== ステップ1：日別集計テーブルを更新 ==========
    TRUNCATE TABLE daily_summary;

    INSERT INTO daily_summary (event_date, event_count, unique_users)
    SELECT
        DATE(event_timestamp) AS event_date,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM raw_events
    GROUP BY DATE(event_timestamp);

    SELECT COUNT(*) INTO v_step_1_rows FROM daily_summary;

    -- ========== ステップ2：週別集計テーブルを更新 ==========
    TRUNCATE TABLE weekly_summary;

    INSERT INTO weekly_summary (
        week_start,
        week_end,
        event_count,
        unique_users,
        purchase_count
    )
    SELECT
        DATE_TRUNC('WEEK', event_timestamp) AS week_start,
        DATEADD(day, 6, DATE_TRUNC('WEEK', event_timestamp)) AS week_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_id END) AS purchase_count
    FROM raw_events
    GROUP BY DATE_TRUNC('WEEK', event_timestamp);

    SELECT COUNT(*) INTO v_step_2_rows FROM weekly_summary;

    -- ========== ステップ3：アクティブユーザーテーブルを更新 ==========
    TRUNCATE TABLE active_users;

    INSERT INTO active_users (user_id, last_event_date, total_events)
    SELECT
        user_id,
        MAX(DATE(event_timestamp)) AS last_event_date,
        COUNT(*) AS total_events
    FROM raw_events
    WHERE DATE(event_timestamp) >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY user_id;

    SELECT COUNT(*) INTO v_step_3_rows FROM active_users;

    -- ========== ステップ4：実行時間を計算 ==========
    SET v_end_time = CURRENT_TIMESTAMP();

    -- ========== 完了メッセージ ==========
    RETURN 'Pipeline execution completed successfully! ' ||
           'Step1: ' || v_step_1_rows || ' rows, ' ||
           'Step2: ' || v_step_2_rows || ' rows, ' ||
           'Step3: ' || v_step_3_rows || ' rows. ' ||
           'Execution time: ' ||
           DATEDIFF(second, v_start_time, v_end_time) || ' seconds.';
END;
$$;

-- 実行
CALL sp_run_nightly_data_pipeline();

/*
このプロシジャは実務での典型的なデータパイプラインです：
  1. 複数のテーブルを段階的に更新
  2. 各ステップの結果を記録
  3. 実行時間を測定
  4. 最終レポートを返す

次のステップ7（タスク）で、このプロシジャを定期実行します。
*/


-- =====================================================================
-- プロシジャ5：エラーハンドリング
-- =====================================================================

CREATE OR REPLACE PROCEDURE sp_safe_data_update()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_error_message VARCHAR;
BEGIN
    -- トランザクション開始（Snowflakeでは暗黙的）

    BEGIN
        -- リスキーな操作をTRY-CATCH で囲む
        DELETE FROM daily_summary WHERE 1=1;  -- 全行削除

        INSERT INTO daily_summary (event_date, event_count, unique_users)
        SELECT
            DATE(event_timestamp) AS event_date,
            COUNT(*) AS event_count,
            COUNT(DISTINCT user_id) AS unique_users
        FROM raw_events
        GROUP BY DATE(event_timestamp);

        -- 成功時
        RETURN '✓ Data update completed successfully';

    EXCEPTION
        WHEN STATEMENT_ERROR THEN
            -- SQLエラー時（例：テーブルが存在しない）
            SET v_error_message = 'SQL Error occurred during data update';
            RETURN '✗ ' || v_error_message;

        WHEN OTHER THEN
            -- その他のエラー
            SET v_error_message = 'Unknown error occurred';
            RETURN '✗ ' || v_error_message;
    END;
END;
$$;

-- 実行
CALL sp_safe_data_update();

/*
【エラーハンドリング】
  BEGIN ... EXCEPTION ... END で例外処理

注意：
  本番環境では、より詳細なエラーログ記録が必須です
*/


-- =====================================================================
-- プロシジャのアンチパターン
-- =====================================================================

/*
【避けるべきパターン】

1. 過度に複雑なプロシジャ（数百行）
   ❌ 1つのプロシジャで全処理を実装
   ✓ 処理を分割して複数プロシジャに分ける

2. ハードコーディング（固定値の埋め込み）
   ❌ WHERE event_timestamp >= '2025-12-01'
   ✓ パラメータ化：DATEADD(day, -30, CURRENT_DATE())

3. テスト困難なロジック（副作用のある処理）
   ❌ DELETE や UPDATE が含まれ、ロールバックできない
   ✓ SELECT で検証後に実行

4. ドキュメント不足
   ❌ コメントなし、目的不明
   ✓ 各ステップに詳細なコメント

5. バージョン管理なし
   ❌ プロシジャのコードが Git に保存されない
   ✓ dbt や Version Control で管理
*/


-- =====================================================================
-- プロシジャの確認・管理
-- =====================================================================

-- プロシジャ一覧
SHOW PROCEDURES;

-- プロシジャの定義確認
SELECT GET_DDL('PROCEDURE', 'sp_calculate_daily_summary()');

-- プロシジャ削除（必要に応じて）
-- DROP PROCEDURE sp_calculate_daily_summary();


-- =====================================================================
-- プロシジャのベストプラクティス
-- =====================================================================

/*
【推奨】

1. 説明的な名前を使用
   ✓ sp_run_nightly_data_pipeline
   ✗ sp_process (何を処理するのか不明)

2. パラメータを活用
   ✓ PROCEDURE(p_country, p_start_date)
   ✗ ハードコーディング

3. 詳細なコメント
   -- ========== ステップN：説明 ==========

4. エラーハンドリング
   BEGIN ... EXCEPTION ... END

5. ログ・トレース機能
   実行結果をテーブルに記録

6. テスト計画
   - 単体テスト：SELECT で検証
   - 統合テスト：プロシジャ実行で検証

7. ドキュメント化
   - 目的
   - 入力・出力
   - 依存する他のプロシジャ
   - エラー時の対応

8. Git バージョン管理
   プロシジャのコードを Git で管理
*/


-- =====================================================================
-- SQL と dbt の移行パス
-- =====================================================================

/*
【本ハンズオンで学んだストアドプロシジャの課題】

1. デバッグ困難
   - 各ステップの中間結果が見づらい
   - エラーが発生すると原因特定が難しい

2. テスト性の欠如
   - ユニットテストが書きにくい
   - SELECT で結果検証が困難

3. バージョン管理
   - Git との統合が弱い
   - 変更履歴の追跡が困難

4. 再利用性
   - ロジックの一部を再利用しづらい
   - 複数プロジェクト間での共有が困難


【dbt への移行で解決できること】

1. モジュラー設計
   - 各 SQL ファイルが独立したモデル
   - 容易に検証・テスト可能

2. 自動テスト
   - schema.yml でテスト定義
   - データ品質を自動チェック

3. Git 統合
   - 各ファイルがテキスト形式
   - 差分追跡が容易

4. ドキュメント自動生成
   - ER図、lineage自動生成

【推奨】
  ストアドプロシジャは複雑すぎる場合、dbt への移行を検討してください。
  ステップ8以降で dbt の利点を実体験いただきます。
*/

/*
======== 結論 ========

ストアドプロシジャは以下の場合に有効：
  - シンプルな定期実行ジョブ
  - データベース側での計算が必須の場合
  - リアルタイム処理が必須

一方、複雑な ETL パイプラインは dbt に任せるべきです：
  - テスト性、保守性が高い
  - チーム開発に適している
  - 実務でのデファクトスタンダード

次のステップ7では、このプロシジャをタスクで定期実行します。
その後、ステップ8で dbt への移行方法を学びます。
*/
