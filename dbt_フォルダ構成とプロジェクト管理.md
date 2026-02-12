# dbt フォルダ構成とプロジェクト管理ガイド

## 1. dbt のフォルダ構成は「dbtプロジェクト」に対して1つ

1つの `dbt_project.yml` に対して1つのフォルダ構成（models, macros, tests 等）が存在する。

1つのdbtプロジェクトで複数の実プロジェクト（業務プロジェクト）を管理する場合、**全ての実プロジェクトが同じフォルダツリーの中に入る**。実プロジェクトの分離は、フォルダ（サブディレクトリ）と命名規則で行う。dbtとしてはあくまで「models配下のサブフォルダ」であり、「別プロジェクト」とは認識していない。

```
my_org_dbt/                         ← 1つのdbtプロジェクト
├── dbt_project.yml                 ← 1つ
├── profiles.yml                    ← 1つ
├── macros/                         ← 共有
│   └── generate_schema_name.sql
├── models/                         ← この中で実プロジェクトを分ける
│   ├── staging/
│   │   ├── project_a/              ← 実プロジェクトA
│   │   │   ├── _project_a__sources.yml
│   │   │   └── stg_project_a__orders.sql
│   │   └── project_b/              ← 実プロジェクトB
│   │       ├── _project_b__sources.yml
│   │       └── stg_project_b__events.sql
│   └── marts/
│       ├── project_a/
│       │   └── fct_project_a_sales.sql
│       └── project_b/
│           └── fct_project_b_events.sql
├── tests/                          ← 共有
├── seeds/                          ← 共有
└── snapshots/                      ← 共有
```

---

## 2. 実プロジェクトごとに独立させたい場合

実プロジェクトごとに独立したフォルダ構成（独自の `dbt_project.yml`）を持たせたい場合は、**dbtプロジェクト自体を分ける**必要がある。

```
repo: dbt_project_a/               ← 独立したdbtプロジェクト
├── dbt_project.yml
├── profiles.yml
├── macros/
├── models/
│   ├── staging/
│   └── marts/
└── tests/

repo: dbt_project_b/               ← 独立したdbtプロジェクト
├── dbt_project.yml
├── profiles.yml
├── macros/
├── models/
│   ├── staging/
│   └── marts/
└── tests/
```

この場合は dbt Mesh でプロジェクト間を連携するか、`source()` で相互参照する形になる。

---

## 3. どちらを選ぶか

| 観点 | 1つのdbtプロジェクト（サブフォルダで分離） | 実プロジェクトごとにdbtプロジェクト |
|------|------|------|
| リネージ | 全体が1つのDAGで見える | プロジェクト間は切れる（Mesh以外） |
| マクロ・テスト | 共有できる | 各プロジェクトで個別管理 |
| CI/CD | 1パイプライン | プロジェクトごとに独立 |
| チーム自律性 | 低い（変更が全体に影響しうる） | 高い |
| 運用の複雑さ | シンプル | 複雑 |

組織のSnowflakeアカウントが1つで、チーム規模がそこまで大きくなければ、**1つのdbtプロジェクト + サブフォルダで実プロジェクトを分ける方式**で始めるのが現実的。

---

## 4. 新しい実プロジェクトを追加するときに必要な作業

### 必ず追加・更新が必要なもの

```
my_org_dbt/
├── dbt_project.yml              ← ✏️ 更新（project_c のスキーマ設定を追加）
├── models/
│   ├── staging/
│   │   └── project_c/           ← 🆕 追加
│   │       ├── _project_c__sources.yml
│   │       └── stg_project_c__xxx.sql
│   └── marts/
│       └── project_c/           ← 🆕 追加
│           └── fct_project_c_xxx.sql
```

`dbt_project.yml` への追記例：

```yaml
models:
  my_org:
    staging:
      project_a:
        +schema: stg_project_a
      project_b:
        +schema: stg_project_b
      project_c:                   # ← 追加
        +schema: stg_project_c
    marts:
      project_a:
        +schema: mart_project_a
      project_b:
        +schema: mart_project_b
      project_c:                   # ← 追加
        +schema: mart_project_c
```

### 場合によって更新が必要なもの

| ファイル/フォルダ | 更新が必要なケース |
|---|---|
| `macros/` | project_c 固有のマクロが必要な場合 |
| `tests/` | project_c 固有のカスタムテストが必要な場合 |
| `seeds/` | project_c で CSV シードデータを使う場合 |
| `snapshots/` | project_c でスナップショットを使う場合 |
| `packages.yml` | 新しい dbt パッケージが必要な場合 |

### 通常は触らなくてよいもの

`profiles.yml` と `macros/generate_schema_name.sql` は、最初に正しく設計されていればプロジェクト追加時に変更不要。

---

## 5. 初心者が共有ファイルを触るリスク

### 起きうるトラブル

- **`dbt_project.yml` の誤編集** — 他プロジェクトのスキーマ設定を消したり、マテリアライゼーション設定を壊したりする
- **共有マクロの変更** — `generate_schema_name.sql` を誤って変更すると、全プロジェクトのスキーマ名が壊れる
- **`packages.yml` の変更** — パッケージバージョンを変えると全体に影響する

---

## 6. 推奨する対策

### A. Git のブランチ保護 + コードレビュー（最重要）

```
main（本番）     ← マージには承認が必須
  ↑ Pull Request（レビュー必須）
feature/add-project-c  ← 各自ここで作業
```

GitHub / GitLab で以下を設定する。

- `main` ブランチへの直接プッシュを禁止
- Pull Request にレビュー承認を必須化（最低1名）
- 共有ファイルの変更にはシニアメンバーの承認を必須化

### B. CODEOWNERS で共有ファイルの変更を管理

GitHub の `CODEOWNERS` ファイルを使い、共有ファイルの変更時に自動的に特定の人にレビューが割り当てられるようにする。

```
# .github/CODEOWNERS

# 共有設定ファイル → シニアメンバーのみ承認可
dbt_project.yml                    @data-platform-team
macros/generate_schema_name.sql    @data-platform-team
packages.yml                       @data-platform-team
profiles.yml                       @data-platform-team

# 各プロジェクトのモデル → 各チームが管理
models/staging/project_a/          @team-a
models/marts/project_a/            @team-a
models/staging/project_b/          @team-b
models/marts/project_b/            @team-b
models/staging/project_c/          @team-c
models/marts/project_c/            @team-c
```

### C. CI/CD でテストを自動実行

Pull Request 作成時に自動で `dbt build` を走らせ、壊れていたらマージできないようにする。

```yaml
# .github/workflows/dbt_ci.yml（GitHub Actions の例）
name: dbt CI
on:
  pull_request:
    branches: [main]

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dbt
        run: pip install dbt-core dbt-snowflake
      - name: dbt deps
        run: dbt deps
      - name: dbt build (CI)
        run: dbt build --target ci
```

### D. フォルダ単位での実行制限

各チームが自分のプロジェクトだけ実行するようにセレクタを使う。

```bash
# project_c チームは自分のモデルだけ実行
dbt run --select staging.project_c marts.project_c

# project_c のテストだけ実行
dbt test --select staging.project_c marts.project_c
```

---

## 7. Snowflake 側の権限分離

dbt プロジェクトは1つでも、Snowflake 側でプロジェクトごとに権限を分離できる。

### プロジェクトごとのロール作成

```sql
-- プロジェクトごとのロールを作成
CREATE ROLE transformer_project_a;
CREATE ROLE transformer_project_b;
CREATE ROLE transformer_project_c;

-- project_c のロールには project_c のスキーマのみ書き込み許可
GRANT USAGE ON DATABASE ORG_DB TO ROLE transformer_project_c;

-- ソース（読み取り）
GRANT USAGE ON SCHEMA ORG_DB.raw_project_c TO ROLE transformer_project_c;
GRANT SELECT ON ALL TABLES IN SCHEMA ORG_DB.raw_project_c TO ROLE transformer_project_c;

-- 出力先（読み書き）
GRANT USAGE ON SCHEMA ORG_DB.stg_project_c TO ROLE transformer_project_c;
GRANT CREATE TABLE ON SCHEMA ORG_DB.stg_project_c TO ROLE transformer_project_c;
GRANT CREATE VIEW ON SCHEMA ORG_DB.stg_project_c TO ROLE transformer_project_c;
GRANT USAGE ON SCHEMA ORG_DB.mart_project_c TO ROLE transformer_project_c;
GRANT CREATE TABLE ON SCHEMA ORG_DB.mart_project_c TO ROLE transformer_project_c;

-- 他プロジェクトのマートを参照したい場合は SELECT のみ
GRANT SELECT ON ALL TABLES IN SCHEMA ORG_DB.mart_project_a TO ROLE transformer_project_c;
```

### ロールの使い分け

| 場面 | ロール | 用途 |
|------|--------|------|
| 開発時 | `transformer_project_c` | 各チームが自分のスキーマのみ操作 |
| 本番デプロイ | `transformer_prod`（統合ロール） | CI/CD から全モデルをビルド |

### 開発者ごとの profiles.yml

```yaml
my_org:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: xxx
      user: taro
      database: ORG_DB
      schema: dbt_taro
      role: transformer_project_c    # ← 自分のプロジェクトのロール
      warehouse: transforming
```

---

## 8. まとめ

| リスク | 対策 |
|--------|------|
| 共有ファイルの誤編集 | CODEOWNERS + レビュー必須化 |
| 他プロジェクトへの影響 | CI/CD で `dbt build` を自動テスト |
| Snowflake の権限混在 | プロジェクトごとにロールを分離 |
| 初心者の操作ミス | ブランチ保護 + main への直接プッシュ禁止 |

dbtのフォルダ構成は1つでも、**Git（CODEOWNERS + ブランチ保護）と Snowflake（ロール分離）の組み合わせ**で、実プロジェクト単位のガバナンスは十分実現できる。
