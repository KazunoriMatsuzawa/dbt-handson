/*
================================================================================
Step C：VIEW体験 -「手動管理の問題」（7分）
================================================================================

【目的】
  よく使うクエリを再利用可能な「仮想テーブル」として保存します。
  複数のアナリストが同じロジックを共有できます。

【学習ポイント】
  - VIEW（ビュー）：リアルタイムデータ参照
  - ビューの利点・制限事項
  - いつどちらを使うか

【実務での応用】
  - 複雑なクエリ定義を再利用
  - データアクセス権限の制御
  - 標準レポート定義
*/

-- =====================================================================
-- VIEW作成：よく使うクエリを保存する
-- =====================================================================

CREATE OR REPLACE VIEW DIESELPJ_TEST.DBT_HANDSON.V_DAILY_EVENTS AS
SELECT
    DATE(EVENT_TIMESTAMP) AS EVENT_DATE,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT USER_ID) AS UNIQUE_USERS,
    COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS
GROUP BY DATE(EVENT_TIMESTAMP);

-- VIEWを普通のテーブルのように使える
SELECT * FROM DIESELPJ_TEST.DBT_HANDSON.V_DAILY_EVENTS
WHERE EVENT_DATE >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC;

/*
VIEWとは：
  - SQLクエリに名前を付けて保存したもの（仮想テーブル）
  - データは保持しない → 参照するたびにクエリが実行される
  - 複数のアナリストが同じロジックを共有できる
  - 鮮度の良いデータが得られる
*/


-- =====================================================================
-- 2つ目のVIEW：JOINを含むVIEW
-- =====================================================================

CREATE OR REPLACE VIEW DIESELPJ_TEST.DBT_HANDSON.V_COUNTRY_DAILY_SUMMARY AS
SELECT
    DATE(E.EVENT_TIMESTAMP) AS EVENT_DATE,
    U.COUNTRY,
    COUNT(*) AS EVENT_COUNT,
    COUNT(DISTINCT E.USER_ID) AS USER_COUNT,
    COUNT(DISTINCT CASE WHEN E.EVENT_TYPE = 'purchase' THEN E.EVENT_ID END) AS PURCHASE_COUNT
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS E
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS U ON E.USER_ID = U.USER_ID
GROUP BY DATE(E.EVENT_TIMESTAMP), U.COUNTRY;

-- 使用例
SELECT
    EVENT_DATE,
    COUNTRY,
    EVENT_COUNT,
    PURCHASE_COUNT,
    ROUND(PURCHASE_COUNT::FLOAT / USER_COUNT, 4) AS PURCHASE_RATE
FROM DIESELPJ_TEST.DBT_HANDSON.V_COUNTRY_DAILY_SUMMARY
WHERE EVENT_DATE >= DATEADD(DAY, -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, PURCHASE_COUNT DESC;


/*
================================================================================
【壁2：変更の影響範囲がわからない】
================================================================================

VIEWが増えていくと、こんな状況になります：

  V_DAILY_EVENTS        → RAW_EVENTSを参照
  V_COUNTRY_DAILY_SUMMARY → RAW_EVENTS + USERSを参照
  V_WEEKLY_REPORT       → V_DAILY_EVENTSを参照（VIEWの上にVIEW）
  V_MONTHLY_KPI         → V_COUNTRY_DAILY_SUMMARYを参照
  V_EXECUTIVE_DASHBOARD → V_WEEKLY_REPORT + V_MONTHLY_KPIを参照
  ...

問題：
  1. RAW_EVENTSのカラム名を変更したら、どのVIEWが壊れる？
     → 手動で全VIEWの定義を確認するしかない
  2. VIEWの依存関係がわからない
     → どのVIEWがどのVIEWを参照しているか追跡困難
  3. 処理負荷が上がる

  → dbt では「Lineage（データの系譜）」で自動的に依存関係が見える
     変更の影響範囲が一目でわかります。


================================================================================
【壁3：テストが手動】
================================================================================

VIEWの結果が正しいか確認するには、手動でSELECTを実行するしかない：
*/

-- 手動テスト例：EVENT_DATEにNULLがないか確認
SELECT COUNT(*) AS NULL_DATES
FROM DIESELPJ_TEST.DBT_HANDSON.V_DAILY_EVENTS
WHERE EVENT_DATE IS NULL;

-- 手動テスト例：EVENT_COUNTが0以上か確認
SELECT COUNT(*) AS NEGATIVE_COUNTS
FROM DIESELPJ_TEST.DBT_HANDSON.V_DAILY_EVENTS
WHERE EVENT_COUNT < 0;

/*
問題：
  1. 毎回手動で実行するのは面倒 → そのうちやらなくなる
  2. 何をテストすべきか属人化 → チームで共有できない
  3. テスト結果の記録がない → 品質の継続的な管理ができない

  → dbt では schema.yml にテストを定義し、
     `dbt test` 1コマンドで全テストを自動実行できます。

  例：
    - unique: EVENT_DATEが一意か
    - not_null: 必須カラムにNULLがないか
    - accepted_range: 値が想定範囲内か
*/
