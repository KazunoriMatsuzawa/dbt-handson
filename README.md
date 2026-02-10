# SQL & dbt ハンズオンレクチャー

Snowflakeを使用したデータ分析の基礎から応用まで、実務で必要なスキルを習得するハンズオンプロジェクトです。

## 📚 プロジェクト概要

### テーマ
**Webアクセスログ分析** - 実務でよくあるWebサイトのアクセスログを分析

### 対象者
- データ変換未経験者
- SQL、dbtの初心者
- データ分析基盤構築に興味がある方

### ボリューム
- **2コマ（90分）**
  - **第1コマ（45分）**：SQL基礎→応用（SELECT, JOIN, GROUP BY, CTE, VIEW, ストアドプロシジャ, タスク）
  - **第2コマ（45分）**：同じロジックをdbtで実装 → dbtの利点を体験

### 学習目標
1. ✅ SQLの基本から応用まで習得
2. ✅ dbtがいかに開発を効率化するか体験
3. ✅ 実務でよく使うデータ変換パターンを学習
4. ✅ テスト・ドキュメント管理の自動化を理解

---

## 📁 ディレクトリ構成

```
.
├── README.md                              ← このファイル
├── 説明資料.md                            ← 詳細な講座資料
├── dataset/                               ← データセット
│   ├── create_tables.sql                 # Snowflakeテーブル定義
│   ├── generate_data.py                  # ダミーデータ生成スクリプト
│   └── load_data.sql                     # データロードスクリプト
├── sql_examples/                          ← SQL実装例（第1コマ）
│   ├── 01_select_where_distinct.sql      # ステップ1：基本的なSELECT, WHERE, DISTINCT
│   ├── 02_join.sql                       # ステップ2：JOIN（結合）
│   ├── 03_group_by.sql                   # ステップ3：GROUP BY, 集計関数
│   ├── 04_cte.sql                        # ステップ4：CTE（WITH句）
│   ├── 05_views.sql                      # ステップ5：ビュー, マテリアライズドビュー
│   ├── 06_stored_procedure.sql           # ステップ6：ストアドプロシジャ
│   └── 07_task.sql                       # ステップ7：Snowflakeタスク
└── dbt_project/                           ← dbtプロジェクト（第2コマ）
    ├── dbt_project.yml                   # dbtプロジェクト設定
    ├── create_dbt_project.sql            # dbt on Snowflake作成コマンド
    ├── execute_dbt_project.sql           # dbt実行コマンド集
    ├── models/
    │   ├── staging/                      # Staging層（前処理）
    │   │   ├── stg_events.sql           # イベントログの前処理
    │   │   └── stg_users.sql            # ユーザー情報の前処理
    │   ├── intermediate/                 # Intermediate層（統合・加工）
    │   │   └── int_daily_events.sql     # 日別イベント集計
    │   └── marts/                        # Marts層（最終テーブル）
    │       ├── daily_summary.sql        # 日別パフォーマンスサマリー
    │       └── weekly_summary.sql       # 週別パフォーマンスサマリー
    ├── macros/                           # dbt Macros（共通ロジック）
    │   └── common_logic.sql              # 再利用可能なマクロ定義
    ├── tests/                            # dbtテスト定義
    │   └── schema.yml                    # テスト・ドキュメント定義
    └── README.md                         # dbtプロジェクト詳細

```

---

## 🚀 クイックスタート

### 前提条件

- **Snowflakeアカウント** - エンタープライズ版以上（dbt on Snowflakeを使用）
- **Python 3.8以上** - ダミーデータ生成用
- **Git** - バージョン管理用
- **SnowSQL** または **Snowflake Web UI** - SQL実行用

### ステップ1：データセット準備

#### 1.1 テーブル作成

```bash
# Snowflakeにログイン（Web UIまたはSnowSQL）
# 以下を実行：
```

```sql
-- dataset/create_tables.sql の内容をSnowflakeで実行
-- テーブル定義：raw_events, users, sessions を作成
```

#### 1.2 ダミーデータ生成（ローカル）

```bash
# Pythonで必要なパッケージをインストール
pip install pandas faker

# ダミーデータを生成（CSV出力）
cd dataset
python generate_data.py

# 生成ファイル確認
ls -la *.csv
# → users.csv, sessions.csv, raw_events.csv が生成される
```

#### 1.3 Snowflakeへのデータロード

```sql
-- dataset/load_data.sql の内容をSnowflakeで実行
-- COPY コマンドでCSVファイルをSnowflakeにロード

-- 或いはSnowflake UI でファイルアップロード
-- Admin > Data Transfer > File > PUT コマンド使用
```

### ステップ2：第1コマ - SQL実装例

```bash
# sql_examples/ フォルダ内のSQLファイルを順番に実行
# Snowflake Web UI または SnowSQL で実行

# 01_select_where_distinct.sql → 基本的なSELECT、WHERE、DISTINCT
# 02_join.sql → JOIN（INNER, LEFT）
# 03_group_by.sql → GROUP BY、集計関数
# 04_cte.sql → CTE（WITH句）
# 05_views.sql → ビュー、マテリアライズドビュー
# 06_stored_procedure.sql → ストアドプロシジャ
# 07_task.sql → Snowflakeタスク

# 各ファイルはコメント付きで説明が含まれています
```

### ステップ3：第2コマ - dbtプロジェクト実行

#### 3.1 dbt on Snowflake セットアップ

```sql
-- dbt_project/create_dbt_project.sql の内容をSnowflakeで実行
-- 以下を作成：
-- - dbtプロジェクト用ウェアハウス（dbt_wh）
-- - dbtプロジェクト用スキーマ（staging, intermediate, marts）
-- - dbtロール（transformer）
```

#### 3.2 Git統合設定（Snowflake UI）

1. **Snowflake Web UI** → **Projects** を開く
2. **Create New Project** をクリック
3. **Develop in Git** を選択
4. このリポジトリのURLを入力
5. GitHub認証を設定
6. **Create** をクリック

#### 3.3 dbtコマンド実行

```bash
# Snowflake UI の Projects > Terminal で以下を実行

# 依存パッケージのインストール
dbt deps

# 接続確認
dbt debug

# モデル実行（段階的）
dbt run -s staging
dbt run -s intermediate
dbt run -s marts

# または全実行
dbt run

# テスト実行
dbt test

# ドキュメント生成
dbt docs generate
```

---

## 💾 Snowflake環境のセットアップ

### 必要な権限

以下の権限が必要です：

```sql
-- アカウント管理者権限
-- または以下の権限：
GRANT CREATE DATABASE ON ACCOUNT TO ROLE [your_role];
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE [your_role];
GRANT EXECUTE DBT PROJECT ON ACCOUNT TO ROLE [your_role];
```

### ウェアハウスサイズの推奨

| 用途 | サイズ | 自動停止 |
|------|--------|--------|
| **開発・テスト** | XSMALL | 5分 |
| **本番実行** | SMALL | 60分 |
| **大規模データ** | MEDIUM | 60分 |

### アカウント設定確認

```sql
-- 利用可能なウェアハウス確認
SHOW WAREHOUSES;

-- アカウント利用可能なリージョン確認
SELECT * FROM information_schema.databases;

-- 現在のロール確認
SELECT CURRENT_ROLE();
```

---

## 📖 学習順序

### 推奨される進め方

#### セッション1：データ準備（10分）
1. `dataset/create_tables.sql` を実行
2. `python generate_data.py` でデータ生成
3. `dataset/load_data.sql` を実行

#### セッション2：SQL基礎（45分）
各SQLファイルを順番に実行し、コメント説明を読みながら学習：

**必須（受講者が手を動かす）**：
1. `01_select_where_distinct.sql` - データ抽出の基本（10分）
2. `02_join.sql` - テーブル結合（10分）
3. `03_group_by.sql` - 集計（10分）
4. `04_cte.sql` - クエリの段階化（10分）

**デモ中心（講師が実演、受講者は観察）**：
5. `05_views.sql` - ビュー活用（5分）
6. `06_stored_procedure.sql` - プロシジャ（デモ）
7. `07_task.sql` - スケジュール実行（デモ）

各ファイル内の「必須実行」セクションのみ手動実行し、残りは参考として後で確認してください。

#### セッション3：dbtセットアップ（15分）
1. `dbt_project/create_dbt_project.sql` を実行
2. Snowflake UI で dbt on Snowflake プロジェクト作成
3. Git統合設定

#### セッション4：dbt実装（45分）
1. `dbt deps` - 依存パッケージインストール
2. `dbt run` - モデル実行
3. `dbt test` - テスト実行
4. `dbt docs generate` - ドキュメント生成
5. Snowflake UI で Lineage（系統図）確認

---

## 🔍 トラブルシューティング

### エラー：「Database does not exist」

**原因**：analytics データベースが作成されていない

**解決**：
```sql
CREATE DATABASE analytics;
```

### エラー：「Insufficient privileges」

**原因**：ユーザーの権限不足

**解決**：
```sql
GRANT USAGE ON DATABASE analytics TO ROLE [your_role];
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE [your_role];
GRANT CREATE TABLE ON SCHEMA analytics.staging TO ROLE [your_role];
```

### エラー：「Warehouse suspended」

**原因**：ウェアハウスが一時停止している

**解決**：
```sql
ALTER WAREHOUSE dbt_wh RESUME;
```

### エラー：「Git authentication failed」

**原因**：GitHub API Integration の設定不正

**解決**：
1. Snowflake Admin が API Integration を再設定
2. OAuth App の認証情報を確認
3. リポジトリのアクセス権を確認

### Python実行エラー：「ModuleNotFoundError: No module named 'pandas'」

**原因**：必要なパッケージがインストールされていない

**解決**：
```bash
pip install pandas faker
```

### CSV ファイルが見つからない

**原因**：スクリプトが実行されていない、または出力先を確認していない

**解決**：
```bash
cd dataset
python generate_data.py
ls -la *.csv  # 確認
```

---

## 📊 データセット仕様

### raw_events テーブル
- **行数**：500,000 件
- **日付範囲**：過去90日
- **カラム**：event_id, user_id, session_id, event_type, page_url, event_timestamp, device_type, country

### users テーブル
- **行数**：10,000 件
- **カラム**：user_id, signup_date, country, plan_type, is_active

### sessions テーブル
- **行数**：100,000 件
- **カラム**：session_id, user_id, session_start, session_end, page_views, device_type

**イベント種別**：page_view, click, purchase, sign_up, add_to_cart, checkout

**デバイス種別**：desktop, mobile, tablet

**対象国**：US, JP, GB, DE, FR, CA, AU, SG, IN, BR

---

## 🎯 実装のコツ

### SQLベストプラクティス

| ポイント | 例 |
|---------|-----|
| **必要なカラムのみ SELECT** | `SELECT event_id, user_id, event_type` ← ✓ |
| **フィルタは早期に** | WHERE 句を JOIN 前に実施 |
| **複雑なロジックは CTE に** | WITH layer1 AS ... |
| **NULL処理を忘れずに** | COALESCE(), IS NULL |

### dbtベストプラクティス

| ポイント | 例 |
|---------|-----|
| **モデル名で層を表現** | stg_events, int_daily, fct_summary |
| **テストを必須化** | unique, not_null, accepted_values |
| **ドキュメント必須** | description, columns |
| **Macros で再利用** | funnel_stage_generator() |

---

## 📚 参考リソース

### 公式ドキュメント
- [dbt公式ドキュメント](https://docs.getdbt.com)
- [Snowflake SQL Reference](https://docs.snowflake.com/en/sql-reference.html)
- [Snowflake dbt Integration](https://docs.snowflake.com/en/user-guide/dbt.html)

### 学習サイト
- [dbt Learn](https://learn.getdbt.com) - 無料オンラインコース
- [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/) - 分析エンジニアリング解説

### コミュニティ
- [dbt Slack](https://slack.getdbt.com) - 質問・相談
- [dbt Discourse](https://discourse.getdbt.com) - ディスカッション
- [Snowflake Community](https://community.snowflake.com)

---

## ❓ よくある質問

### Q：SQLだけでは不十分ですか？

**A**：小規模プロジェクトであれば十分です。ただし以下の場合はdbt推奨：
- チーム開発（複数人での共同編集）
- テスト・品質管理が重要
- 複雑な変換ロジック
- 本番環境での自動実行

### Q：dbtはSnowflakeでしか使えない？

**A**：いいえ。BigQuery, PostgreSQL, Redshift, Databricksなど複数のDBMSに対応しています。ただし本ハンズオンはSnowflake環境を想定しています。

### Q：本番環境での推奨スケジュール実行頻度は？

**A**：ビジネス要件による：
- **リアルタイム関連**：1時間ごと
- **日次レポート**：毎日深夜
- **週次分析**：毎週月曜朝
- **月次決算**：月初

### Q：どのくらいの時間で習得できる？

**A**：本ハンズオン2時間で基礎習得。実務レベルには3ヶ月程度の練習が必要です。

---

## 📝 ライセンスと利用条件

このプロジェクトはオープンソースとして提供されています。
自由に利用、変更、配布できます。

**ただし**：
- Snowflakeの利用料は別途発生します
- ダミーデータはあくまで学習用です

---

## 🤝 貢献

改善提案やバグ報告は、GitHubのIssuesで受け付けています。

---

## 📞 サポート

質問やトラブルは以下のリソースで質問してください：

1. **このプロジェクトのGitHub Issues**
2. **dbt Slack コミュニティ**
3. **Snowflake サポートセンター**

---

## 更新履歴

| 日付 | 変更内容 |
|------|--------|
| 2025-02-10 | 初版作成 |

---

## まとめ

このハンズオンを通じて、以下を習得できます：

✅ **SQLの基本から応用** - 実務で必要なすべてのSQL技術
✅ **dbtの利点** - テスト・ドキュメント・バージョン管理の自動化
✅ **Snowflakeの活用** - クラウドデータウェアハウスの実践的利用法
✅ **データ分析基盤** - 本番環境への道筋を理解

**次のステップ**：
1. このハンズオンで習得した知識を実務で応用
2. 複雑なロジックに挑戦（ウィンドウ関数、Dynamic Tables）
3. dbt Cloud などの高度なツール活用
4. BI ツール（Tableau, Looker等）との連携

---

**Happy Learning! 🚀**

このハンズオンが皆さんのデータ分析スキル向上に役立つことを祈っています。
