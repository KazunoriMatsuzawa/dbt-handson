# SQL & dbt ハンズオンレクチャー

Snowflakeを使用したデータ分析の基礎から応用まで、実務で必要なスキルを習得するハンズオンプロジェクトです。

## プロジェクト概要

### テーマ
**Webアクセスログ分析** - 実務でよくあるWebサイトのアクセスログを分析

### コース構成

本ハンズオンは **初心者コース** と **中級者コース** の2コースで構成されています。
各コースとも「1コマ目=SQL、2コマ目=dbt」の構成で、dbtの良さを体験できます。

| コース | 対象者 | コマ数 | 内容 |
|--------|--------|--------|------|
| **初心者コース** | SQL未経験〜初心者 | 2コマ（90分） | SQL基礎＋応用エッセンス + dbt入門 |
| **中級者コース** | SQL基礎を習得済み | 2コマ（90分） | SQL詳細マスター + dbt実践 |

---

### 初心者コース（2コマ / 90分）

**対象者**
- データ変換未経験者
- SQL、dbtの知識がない初心者

**学習目標**
1. SQLの基本操作（SELECT, JOIN, GROUP BY）を習得
2. SQLの応用技術（CTE, VIEW, SP, Task）のエッセンスを体験
3. **SQLだけでは解決しにくい「5つの壁」を認識**
4. dbtの基本概念（モデル、ref、テスト、Lineage）を体験
5. dbtが「5つの壁」をどう解決するかを理解

**カリキュラム**

| コマ | 時間 | 内容 |
|------|------|------|
| **1コマ目：SQL基礎＋応用エッセンス** | 45分 | SQL基礎ダイジェスト（12分）→ CTE体験（8分）→ VIEW体験（7分）→ SP体験（8分）→ Task体験（5分）→ 「SQLの5つの壁」まとめ（5分） |
| **2コマ目：dbt入門** | 45分 | セットアップ（8分）→ Stagingモデル（10分）→ Martsモデル（10分）→ テスト&Lineage（10分）→ 「5つの壁」解決まとめ（7分） |

---

### 中級者コース（2コマ / 90分）

**対象者**
- SQL基礎（SELECT, JOIN, GROUP BY）を習得済みの方
- データパイプラインの構築に興味がある方

**学習目標**
1. SELECT/JOIN/GROUP BY の応用パターン（Window関数、FULL OUTER JOIN等）を習得
2. CTE、VIEW、ストアドプロシジャ、タスクを詳細に学ぶ
3. dbtのIntermediate層、Macros、高度テストを実践
4. SP+Task の複雑さ vs `dbt run` の簡潔さを実感

**カリキュラム**

| コマ | 時間 | 内容 |
|------|------|------|
| **1コマ目：SQL詳細マスター** | 45分 | SQL応用パターン（10分）→ CTEマスター（10分）→ VIEW/DYNAMIC TABLE（8分）→ SP+Taskパイプライン（12分）→ まとめ（5分） |
| **2コマ目：dbt実践** | 45分 | Intermediate層（10分）→ Macros/DRY原則（10分）→ Weekly Summary+Window関数（10分）→ 高度テスト+ドキュメント（10分）→ SP+Task→dbt比較（5分） |

---

## ディレクトリ構成

```
.
├── README.md                              ← このファイル
├── 説明資料.md                            ← 詳細な講座資料
├── dataset/                               ← データセット
│   ├── create_tables.sql                 # Snowflakeテーブル定義
│   ├── generate_data.py                  # ダミーデータ生成スクリプト
│   └── load_data.sql                     # データロードスクリプト
├── sql_examples/                          ← SQL実装例
│   ├── beginner_01_sql_digest.sql        # Step A：SQL基礎ダイジェスト（初心者）
│   ├── beginner_02_cte_essence.sql       # Step B：CTE体験（初心者）
│   ├── beginner_03_view_essence.sql      # Step C：VIEW体験（初心者）
│   ├── beginner_04_sp_essence.sql        # Step D：SP体験（初心者）
│   ├── beginner_05_task_essence.sql      # Step E：Task体験（初心者）
│   ├── 01_select_where_distinct.sql      # SELECT, WHERE, DISTINCT 詳細（中級者）
│   ├── 02_join.sql                       # JOIN 詳細（中級者）
│   ├── 03_group_by.sql                   # GROUP BY, 集計関数 詳細（中級者）
│   ├── 04_cte.sql                        # CTE 詳細（中級者）
│   ├── 05_views.sql                      # VIEW, DYNAMIC TABLE 詳細（中級者）
│   ├── 06_stored_procedure.sql           # ストアドプロシジャ 詳細（中級者）
│   └── 07_task.sql                       # Snowflakeタスク 詳細（中級者）
└── dbt_project/                           ← dbtプロジェクト
    ├── dbt_project.yml                   # dbtプロジェクト設定
    ├── create_dbt_project.sql            # dbt on Snowflake作成コマンド
    ├── execute_dbt_project.sql           # dbt実行コマンド集
    ├── models/
    │   ├── staging/                      # Staging層
    │   │   ├── stg_events_beginner.sql  # イベント前処理（初心者用・マクロ不使用）
    │   │   ├── stg_users_beginner.sql   # ユーザー前処理（初心者用・マクロ不使用）
    │   │   ├── stg_events.sql           # イベント前処理（中級者用・マクロ使用）
    │   │   └── stg_users.sql            # ユーザー前処理（中級者用・マクロ使用）
    │   ├── intermediate/                 # Intermediate層（中級者用）
    │   │   └── int_daily_events.sql     # 日別イベント集計
    │   └── marts/                        # Marts層
    │       ├── daily_summary_beginner.sql # 日別サマリー（初心者用・staging直接集計）
    │       ├── daily_summary.sql        # 日別サマリー（中級者用）
    │       └── weekly_summary.sql       # 週別サマリー（中級者用）
    ├── macros/                           # dbt Macros（中級者用）
    │   └── common_logic.sql              # 再利用可能なマクロ定義
    ├── tests/                            # dbtテスト定義
    │   ├── schema_beginner.yml          # テスト定義（初心者用：unique/not_nullのみ）
    │   └── schema.yml                    # テスト定義（中級者用：高度テスト含む）
    └── README.md                         # dbtプロジェクト詳細

```

---

## クイックスタート

### 前提条件

- **Snowflakeアカウント** - エンタープライズ版以上（dbt on Snowflakeを使用）
- **Python 3.8以上** - ダミーデータ生成用
- **Git** - バージョン管理用
- **SnowSQL** または **Snowflake Web UI** - SQL実行用

### データセット準備（両コース共通）

#### 1. テーブル作成

```sql
-- dataset/create_tables.sql の内容をSnowflakeで実行
-- テーブル定義：raw_events, users, sessions を作成
```

#### 2. ダミーデータ生成

```bash
pip install pandas faker
cd dataset
python generate_data.py
```

#### 3. Snowflakeへのデータロード

```sql
-- dataset/load_data.sql の内容をSnowflakeで実行
```

---

### 初心者コースの進め方

#### 1コマ目：SQL基礎＋応用エッセンス（45分）

SQLファイルを順番に実行し、「SQLの5つの壁」を体験：

1. `sql_examples/beginner_01_sql_digest.sql` - SQL基礎ダイジェスト【ハンズオン】（12分）
2. `sql_examples/beginner_02_cte_essence.sql` - CTE体験 → 壁1【デモ】（8分）
3. `sql_examples/beginner_03_view_essence.sql` - VIEW体験 → 壁2, 壁3【デモ】（7分）
4. `sql_examples/beginner_04_sp_essence.sql` - SP体験 → 壁4【デモ】（8分）
5. `sql_examples/beginner_05_task_essence.sql` - Task体験 → 壁5【デモ】（5分）
6. 「SQLの5つの壁」まとめ（5分）

**ポイント**：Step 1はハンズオン、Step 2〜5は講師デモ形式。

#### 2コマ目：dbt入門 -「5つの壁」を突破（45分）

1. **セットアップ**（8分）
   - `dbt_project/create_dbt_project.sql` を実行
   - Snowflake UI で dbt on Snowflake プロジェクト作成
   - Git統合設定

2. **Stagingモデル → 壁1（CTE長大化）+ 壁2（VIEW管理）を突破**（10分）
   ```bash
   dbt run -s stg_events_beginner stg_users_beginner
   ```

3. **Martsモデル → 壁4（SP複雑化）を突破**（10分）
   ```bash
   dbt run -s daily_summary_beginner
   ```

4. **テスト & Lineage → 壁3（テスト手動）+ 壁5（Task依存）を突破**（10分）
   ```bash
   dbt test -s stg_events_beginner stg_users_beginner daily_summary_beginner
   dbt docs generate
   ```

5. **「5つの壁」解決マトリクス**（7分）

**初心者向け dbtの"良さ"の見せ方：**
- 1コマ目の「5つの壁」がdbtで1つずつ解決されることを実感
- `ref()` でモデル分割・依存管理が自動化
- `dbt test` 1コマンドでデータ品質チェック自動化
- Lineageグラフで依存関係が一目でわかる

---

### 中級者コースの進め方

#### 1コマ目：SQL詳細マスター（45分）

SQLファイルを順番に実行：

1. `sql_examples/01〜03` から応用パターン - FULL OUTER JOIN, Window関数, HAVING応用（10分）
2. `sql_examples/04_cte.sql` - CTEマスター（RECURSIVE, ファネル分析）（10分）
3. `sql_examples/05_views.sql` - VIEW + DYNAMIC TABLE 設計（8分）
4. `sql_examples/06_stored_procedure.sql` + `07_task.sql` - SP+Taskパイプライン構築（12分）

#### 2コマ目：dbt実践（45分）

1. **Intermediate層の意義**（10分）
   ```bash
   dbt run -s staging intermediate
   ```

2. **Macros（DRY原則）**（10分）
   - `macros/common_logic.sql` を確認
   - マクロが `stg_events.sql` でどう使われているか確認

3. **Weekly Summary + Window関数**（10分）
   ```bash
   dbt run -s weekly_summary
   ```

4. **高度テスト + ドキュメント生成**（10分）
   ```bash
   dbt test
   dbt docs generate
   ```

5. **SP+Task → dbt置き換え比較**（5分）

**中級者向け dbtの"良さ"の見せ方：**
- 1コマ目のSP + Task の複雑さ vs `dbt run` の簡潔さ
- Macrosでコピペ撲滅（DRY原則）
- 高度テストでビジネスロジック品質保証
- VIEW手動管理 vs dbtモデル自動管理（Lineage可視化）

---

## Snowflake環境のセットアップ

### 必要な権限

```sql
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

---

## データセット仕様

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

## トラブルシューティング

### エラー：「Database does not exist」

```sql
CREATE DATABASE analytics;
```

### エラー：「Insufficient privileges」

```sql
GRANT USAGE ON DATABASE analytics TO ROLE [your_role];
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE [your_role];
GRANT CREATE TABLE ON SCHEMA analytics.staging TO ROLE [your_role];
```

### エラー：「Warehouse suspended」

```sql
ALTER WAREHOUSE dbt_wh RESUME;
```

### エラー：「Git authentication failed」

1. Snowflake Admin が API Integration を再設定
2. OAuth App の認証情報を確認
3. リポジトリのアクセス権を確認

### Python実行エラー：「ModuleNotFoundError: No module named 'pandas'」

```bash
pip install pandas faker
```

---

## 参考リソース

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

## よくある質問

### Q：初心者コースと中級者コース、どちらから始めるべき？

**A**：SQLの経験で判断してください：
- **SQL未経験** → 初心者コースから
- **SELECT, JOIN, GROUP BYがわかる** → 中級者コースから
- **不安な場合** → 初心者コースから始めて、中級者コースに進むのがおすすめ

### Q：SQLだけでは不十分ですか？

**A**：小規模プロジェクトであれば十分です。ただし以下の場合はdbt推奨：
- チーム開発（複数人での共同編集）
- テスト・品質管理が重要
- 複雑な変換ロジック
- 本番環境での自動実行

### Q：dbtはSnowflakeでしか使えない？

**A**：いいえ。BigQuery, PostgreSQL, Redshift, Databricksなど複数のDBMSに対応しています。ただし本ハンズオンはSnowflake環境を想定しています。

### Q：どのくらいの時間で習得できる？

**A**：各コース90分で基礎習得。両コース受講しても3時間です。実務レベルには3ヶ月程度の練習が必要です。

---

## ライセンスと利用条件

このプロジェクトはオープンソースとして提供されています。
自由に利用、変更、配布できます。

**ただし**：
- Snowflakeの利用料は別途発生します
- ダミーデータはあくまで学習用です

---

## 貢献

改善提案やバグ報告は、GitHubのIssuesで受け付けています。

---

## サポート

質問やトラブルは以下のリソースで質問してください：

1. **このプロジェクトのGitHub Issues**
2. **dbt Slack コミュニティ**
3. **Snowflake サポートセンター**

---

## 更新履歴

| 日付 | 変更内容 |
|------|--------|
| 2026-02-12 | 初心者コース再構成（SQLの5つの壁 + dbt解決のストーリーライン導入） |
| 2025-02-11 | 初心者コース / 中級者コースに分割 |
| 2025-02-10 | 初版作成 |

---

## まとめ

### 初心者コースで習得できること

- **SQL基礎＋応用エッセンス** - SELECT, JOIN, GROUP BY + CTE/VIEW/SP/Taskのエッセンス
- **「SQLの5つの壁」** - SQLだけでは解決しにくい管理・自動化の課題の認識
- **dbt入門** - モデル分割、ref()、テスト、Lineageでの壁の突破

### 中級者コースで習得できること

- **SQL詳細マスター** - 01〜07の応用パターンを詳細に学ぶ
- **dbt実践** - Intermediate層、Macros、高度テスト
- **パイプライン比較** - SP+Task vs dbt の違いを体感

**次のステップ**：
1. このハンズオンで習得した知識を実務で応用
2. 複雑なロジックに挑戦（ウィンドウ関数、Dynamic Tables）
3. dbt Cloud などの高度なツール活用
4. BI ツール（Tableau, Looker等）との連携
