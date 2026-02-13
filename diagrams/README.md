# SQL実行フロー図

各SQLクエリの実行内容を図解したものです。

## 使い方

### 図の表示方法
`.drawio.svg` ファイルはMarkdownで直接画像として埋め込めます：

```markdown
![INNER JOIN](./01_inner_join.drawio.svg)
```

### `.drawio.svg` の作成手順
1. VSCode で `.drawio` ファイルを開く（Draw.io Integration 拡張機能が必要）
2. 図を編集
3. タブを右クリック → **Export** → **svg** を選択
4. ファイル名を `xxx.drawio.svg` にして保存

> `.drawio.svg` は **SVG画像としてMarkdownで表示可能** + **draw.ioで再編集可能** の一石二鳥フォーマットです。

---

## Step A：SQL基礎ダイジェスト

### INNER JOIN：イベントログにユーザー属性を結合

<!-- .drawio.svgとしてExportした後、以下のように埋め込む -->
![INNER JOIN クエリ](./sample_inner_join.drawio.svg)

```sql
SELECT
    E.EVENT_ID, E.USER_ID, E.EVENT_TYPE, E.EVENT_TIMESTAMP,
    U.COUNTRY, U.PLAN_TYPE
FROM DIESELPJ_TEST.DBT_HANDSON.RAW_EVENTS E
INNER JOIN DIESELPJ_TEST.DBT_HANDSON.USERS U
    ON E.USER_ID = U.USER_ID
WHERE E.EVENT_TYPE = 'purchase'
LIMIT 20;
```

**ポイント：**
- `INNER JOIN` で両テーブルに存在するレコードのみ結合
- `ON` 句で結合キー（`USER_ID`）を指定
- `WHERE` で結合後にフィルタリング
