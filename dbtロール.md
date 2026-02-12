# dbt on Snowflake — 必要な権限とロール設定

## 概要

dbt on Snowflake を実行するには、`CREATE` 権限だけでは不十分であり、複数の権限を階層的に付与する必要がある。

---

## 1. ウェアハウス（コンピュートリソース）

クエリを実行するためにウェアハウスへの **USAGE** 権限が必須。

```sql
GRANT USAGE ON WAREHOUSE transforming TO ROLE transformer;
```

---

## 2. ソースデータ（読み取り元）

ソースデータが格納されたデータベース・スキーマ・テーブルに対する読み取り権限。

| 対象 | 必要な権限 | 目的 |
|------|-----------|------|
| DATABASE | USAGE | データベースへのアクセス |
| SCHEMA | USAGE | スキーマ内オブジェクトの参照 |
| TABLE / VIEW | SELECT | データの読み取り |

```sql
GRANT USAGE ON DATABASE raw TO ROLE transformer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE raw TO ROLE transformer;
GRANT SELECT ON ALL TABLES IN DATABASE raw TO ROLE transformer;
-- 将来作成されるオブジェクトにも適用
GRANT SELECT ON FUTURE TABLES IN DATABASE raw TO ROLE transformer;
```

---

## 3. 出力先（書き込み先）

dbt がモデルをマテリアライズするデータベース・スキーマへの書き込み権限。

| 対象 | 必要な権限 | 目的 |
|------|-----------|------|
| DATABASE | USAGE | データベースへのアクセス |
| DATABASE | CREATE SCHEMA | dbt によるスキーマの自動作成 |
| SCHEMA | CREATE TABLE | テーブルの作成 |
| SCHEMA | CREATE VIEW | ビューの作成 |

```sql
GRANT USAGE ON DATABASE analytics TO ROLE transformer;
GRANT CREATE SCHEMA ON DATABASE analytics TO ROLE transformer;
-- 既存スキーマに対して個別に付与する場合
GRANT CREATE TABLE ON SCHEMA analytics.dbt_prod TO ROLE transformer;
GRANT CREATE VIEW ON SCHEMA analytics.dbt_prod TO ROLE transformer;
```

---

## 4. dbt コマンド別の必要権限

| dbt コマンド | 主に必要な操作 |
|-------------|---------------|
| `dbt compile` | ソースへの USAGE / SELECT（メタデータ参照のみ、書き込みなし） |
| `dbt run` | CREATE TABLE / VIEW + SELECT + INSERT |
| `dbt test` | SELECT（テスト対象テーブルの読み取り） |
| `dbt snapshot` | CREATE TABLE + SELECT + INSERT + UPDATE + DELETE |
| `dbt seed` | CREATE TABLE + INSERT |

---

## 5. 推奨ロール構成

dbt 公式ドキュメントでは、用途別にロールを分離する構成が推奨されている。

| ロール | 用途 | 主な権限 |
|--------|------|----------|
| `loader` | データ取り込み（Fivetran, Stitch 等） | ソース DB への書き込み |
| `transformer` | dbt による変換 | ソース DB の読み取り + 出力先 DB の読み書き |
| `reporter` | BI ツールからの参照（Looker, Tableau 等） | 出力先 DB の読み取り |

### セットアップ例

```sql
-- ===== ロール作成 =====
USE ROLE securityadmin;
CREATE ROLE loader;
CREATE ROLE transformer;
CREATE ROLE reporter;

-- ===== ウェアハウス =====
CREATE WAREHOUSE transforming
  WAREHOUSE_SIZE = xsmall
  AUTO_SUSPEND = 60
  AUTO_RESUME = true
  INITIALLY_SUSPENDED = true;

GRANT USAGE ON WAREHOUSE transforming TO ROLE transformer;

-- ===== ソース DB（読み取りのみ） =====
GRANT USAGE ON DATABASE raw TO ROLE transformer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE raw TO ROLE transformer;
GRANT SELECT ON ALL TABLES IN DATABASE raw TO ROLE transformer;
GRANT SELECT ON FUTURE TABLES IN DATABASE raw TO ROLE transformer;

-- ===== 出力先 DB（読み書き） =====
GRANT USAGE ON DATABASE analytics TO ROLE transformer;
GRANT CREATE SCHEMA ON DATABASE analytics TO ROLE transformer;

-- ===== ユーザーへのロール付与 =====
CREATE USER dbt_user
  PASSWORD = '_generate_this_'
  DEFAULT_WAREHOUSE = transforming
  DEFAULT_ROLE = transformer;

GRANT ROLE transformer TO USER dbt_user;
```

---

## 6. まとめ

dbt on Snowflake の実行には、最低限以下の権限の組み合わせが必要となる。

- **USAGE** — データベース、スキーマ、ウェアハウスへのアクセス
- **SELECT** — ソーステーブルの読み取り
- **CREATE TABLE / VIEW** — 出力先へのオブジェクト作成
- **UPDATE / DELETE** — snapshot を使用する場合に追加で必要

---

## 公式ドキュメント

- [Snowflake permissions — dbt Docs](https://docs.getdbt.com/reference/database-permissions/snowflake-permissions)
- [What privileges does my database user need to use dbt? — dbt Docs](https://docs.getdbt.com/faqs/Warehouse/database-privileges)
- [grants — dbt Docs](https://docs.getdbt.com/reference/resource-configs/grants)
- [Access control privileges — Snowflake Documentation](https://docs.snowflake.com/en/user-guide/security-access-control-privileges)
- [Access control for dbt projects on Snowflake — Snowflake Documentation](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-access-control)
