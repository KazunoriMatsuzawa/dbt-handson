# dbt プロジェクトのテンプレート・ひな形リポジトリまとめ

## 公式テンプレート・サンプル

### 1. dbt Starter Project（`dbt init` で生成）

`dbt init` コマンドを実行すると自動的に生成される最小構成のひな形。

- **リポジトリ:** https://github.com/dbt-labs/dbt-starter-project

```
my_project/
├── dbt_project.yml
├── models/
│   └── example/
│       ├── my_first_dbt_model.sql
│       └── my_second_dbt_model.sql
├── analyses/
├── macros/
├── seeds/
├── snapshots/
└── tests/
```

非常にシンプルなため、実際のプロジェクトにはもう少し構造が必要。

---

### 2. Jaffle Shop（公式チュートリアル用）

dbt Labs が提供する架空の EC サイトのデモプロジェクト。staging → marts のレイヤー構造、テスト、ドキュメントが一通り含まれている。

- **リポジトリ:** https://github.com/dbt-labs/jaffle-shop
- 学習用として最適で、dbt 公式チュートリアルとセット

---

### 3. GitLab Data Team（大規模な実例）

GitLab の社内 dbt プロジェクトがオープンソースで公開されている。数百モデル規模の本番プロジェクトで、大規模で dbt をどう使うかの実例として参考になる。

- **リポジトリ:** https://gitlab.com/gitlab-data/analytics/-/tree/master/transform/snowflake-dbt

---

## コミュニティテンプレート

### 4. dbt Project Template（実務向けひな形）

staging / intermediate / marts のフォルダ構造、README、マクロ、テストのひな形が整った実務寄りのテンプレート。

- **リポジトリ:** https://github.com/jmbrooks/dbt-project-template

---

## 公式ベストプラクティスガイド

テンプレートではないが、フォルダ構成・命名規則・レイヤー設計の考え方を詳しく解説した公式ガイド。

- **How we structure our dbt projects:** https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview

---

## 推奨フォルダ構成（公式ガイド準拠）

```
models/
├── staging/                          # ソースデータの軽い整形（ビュー中心）
│   ├── jaffle_shop/
│   │   ├── _jaffle_shop__sources.yml     # ソース定義
│   │   ├── _jaffle_shop__models.yml      # モデルのドキュメント・テスト
│   │   ├── stg_jaffle_shop__orders.sql
│   │   └── stg_jaffle_shop__customers.sql
│   └── stripe/
│       ├── _stripe__sources.yml
│       └── stg_stripe__payments.sql
│
├── intermediate/                     # 中間変換（複雑な結合・集計の分割）
│   └── finance/
│       └── int_payments_pivoted.sql
│
└── marts/                            # 最終成果物（BI・アナリスト向け）
    ├── finance/
    │   └── fct_orders.sql
    └── marketing/
        └── dim_customers.sql
```

### 命名規則

| レイヤー | プレフィックス | 例 |
|----------|---------------|-----|
| staging | `stg_` | `stg_jaffle_shop__orders.sql` |
| intermediate | `int_` | `int_payments_pivoted.sql` |
| marts (ファクト) | `fct_` | `fct_orders.sql` |
| marts (ディメンション) | `dim_` | `dim_customers.sql` |

### source 定義ファイルの命名規則

```
_<ソース名>__sources.yml     # ソース定義
_<ソース名>__models.yml      # モデルのドキュメント・テスト定義
```

先頭の `_`（アンダースコア）により、ファイルツリー上でYAMLファイルがSQLファイルより上に表示される。

---

## その他の公式サンプルプロジェクト

| プロジェクト | 説明 | リポジトリ |
|---|---|---|
| Jaffle Shop | EC サイトデモ（学習用） | https://github.com/dbt-labs/jaffle-shop |
| Jaffle Shop (DuckDB) | DuckDB 版 Jaffle Shop | https://github.com/dbt-labs/jaffle-shop-duckdb |
| dummy-dbt | コンテナ化された dbt テストプロジェクト（Postgres / Sakila） | https://github.com/dbt-labs/dummy-dbt |
| Google Analytics 4 | GA4 BigQuery エクスポートの変換デモ | https://github.com/stacktonic-com/stacktonic-dbt-example-project |

---

## まとめ

| リソース | 用途 | URL |
|---|---|---|
| dbt Starter Project | 最小ひな形 | https://github.com/dbt-labs/dbt-starter-project |
| Jaffle Shop | 学習・チュートリアル | https://github.com/dbt-labs/jaffle-shop |
| GitLab Data Team | 大規模実例 | https://gitlab.com/gitlab-data/analytics |
| dbt Project Template | 実務向けテンプレート | https://github.com/jmbrooks/dbt-project-template |
| 公式構造ガイド | 設計思想・命名規則 | https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview |
| 公式ベストプラクティス一覧 | 各種ベストプラクティス | https://docs.getdbt.com/best-practices |

### 推奨の進め方

1. **Jaffle Shop** をクローンして動かし、dbt の基本構造に慣れる
2. **公式構造ガイド**を読み、設計思想を理解する
3. **dbt Project Template** を参考に、自組織用にカスタマイズする
4. 規模が大きくなったら **GitLab Data Team** のリポジトリを参考にする
