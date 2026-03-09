🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [English](../README.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [Français](README.fr.md) | 日本語

<p align="center">
  <img src="logo.png" alt="AIVectorMemory Logo" width="200">
</p>
<h1 align="center">AIVectorMemory</h1>
<p align="center">
  <strong>AIコーディングアシスタントに記憶を — セッション間永続記憶MCPサーバー</strong>
</p>
<p align="center">
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
  <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
</p>
---

> **まだ CLAUDE.md / MEMORY.md を記憶として使っていますか？** この Markdown ファイル記憶方式には致命的な欠陥があります：ファイルは膨らみ続け、毎回のセッションで全量注入して大量のトークンを消費；内容はキーワード検索しかできず、「データベースタイムアウト」で検索しても「MySQL コネクションプールの落とし穴」は見つからない；複数プロジェクトで1つのファイルを共有すると相互汚染；タスク追跡がなく、開発進捗は頭の中だけ；200行での切り捨て、手動メンテナンス、重複排除や統合ができないという日常的な問題も。
>
> **AIVectorMemory はまったく異なるアプローチです。** ローカルベクトルデータベースに保存し、セマンティック検索で的確に呼び出し（言葉が違っても一致）、オンデマンド検索で関連する記憶だけを読み込み（トークン消費を50%+削減）、マルチプロジェクトを自動隔離して干渉ゼロ、内蔵の問題追跡 + タスク管理で AI が開発フロー全体を自動管理。すべてのデータはあなたのマシンに永久保存 — クラウド依存ゼロ、セッションや IDE を切り替えても失われません。

## ✨ 主な機能

| 機能 | 説明 |
|------|------|
| 🧠 **クロスセッション記憶** | AIがついにプロジェクトを覚えてくれる — 踏んだ地雷、下した決定、決めた規約、セッションが変わっても忘れない |
| 🔍 **セマンティック検索** | 原文の書き方を覚えていなくてOK —「データベースタイムアウト」で検索すれば「MySQLコネクションプール問題」が見つかる |
| 💰 **50%+トークン節約** | 毎回プロジェクト背景をコピペする必要なし。セマンティック検索でオンデマンド呼び出し、一括注入とはお別れ |
| 🔗 **タスク駆動開発** | 問題追跡 → タスク分割 → ステータス同期 → 連動アーカイブ。AIが開発フロー全体を自動管理 |
| 📊 **Webダッシュボード** | すべての記憶とタスクを視覚的に管理、3Dベクトルネットワークで知識の繋がりが一目瞭然 |
| 🏠 **完全ローカル** | クラウド依存ゼロ。ONNXローカル推論、APIキー不要、データはマシンから出ない |
| 🔌 **全IDE対応** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — ワンクリックインストール、すぐ使える |
| 📁 **マルチプロジェクト分離** | 1つのDBで全プロジェクト管理、自動分離で干渉なし、プロジェクト切り替えもシームレス |
| 🔄 **スマート重複排除** | 類似度 > 0.95 で自動マージ更新、記憶ストアは常にクリーン — 使い続けても散らからない |

<p align="center">
  QQ群：1085682431 &nbsp;|&nbsp; 微信：changhuibiz<br>
  共同参与项目开发加QQ群或微信交流
</p>

## 🏗️ アーキテクチャ

```
┌─────────────────────────────────────────────────┐
│                   AI IDE                         │
│  OpenCode / Claude Code / Cursor / Kiro / ...   │
└──────────────────────┬──────────────────────────┘
                       │ MCP Protocol (stdio)
┌──────────────────────▼──────────────────────────┐
│              AIVectorMemory Server               │
│                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────┐ │
│  │ remember │ │  recall   │ │   auto_save      │ │
│  │ forget   │ │  task     │ │   status/track   │ │
│  └────┬─────┘ └────┬─────┘ └───────┬──────────┘ │
│       │            │               │             │
│  ┌────▼────────────▼───────────────▼──────────┐  │
│  │         Embedding Engine (ONNX)            │  │
│  │      intfloat/multilingual-e5-small        │  │
│  └────────────────────┬───────────────────────┘  │
│                       │                          │
│  ┌────────────────────▼───────────────────────┐  │
│  │     SQLite + sqlite-vec（ベクトルインデックス）│  │
│  │     ~/.aivectormemory/memory.db            │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## 🚀 クイックスタート

### 方法1：pip インストール（推奨）

```bash
# インストール
pip install aivectormemory

# 最新版にアップグレード
pip install --upgrade aivectormemory

# プロジェクトディレクトリに移動し、ワンクリックで IDE を設定
cd /path/to/your/project
run install
```

`run install` は対話式で IDE を選択し、MCP 設定・Steering ルール・Hooks を自動生成します。手動設定は不要です。

> **macOS ユーザーへの注意**：
> - `externally-managed-environment` エラーが出た場合は `--break-system-packages` を追加してください
> - `enable_load_extension` エラーが出た場合、現在の Python が SQLite 拡張読み込みに対応していません（macOS 標準 Python および python.org 公式インストーラは非対応）。Homebrew Python をご利用ください：
>   ```bash
>   brew install python
>   /opt/homebrew/bin/python3 -m pip install aivectormemory
>   ```

### 方法2：uvx 実行（インストール不要）

`pip install` 不要、直接実行できます：

```bash
cd /path/to/your/project
uvx aivectormemory install
```

> 事前に [uv](https://docs.astral.sh/uv/getting-started/installation/) のインストールが必要です。`uvx` が自動的にダウンロード・実行するため、手動でのパッケージインストールは不要です。

### 方法3：手動設定

```json
{
  "mcpServers": {
    "aivectormemory": {
      "command": "run",
      "args": ["--project-dir", "/path/to/your/project"]
    }
  }
}
```

<details>
<summary>📍 各IDE設定ファイルの場所</summary>

| IDE | 設定ファイルパス |
|-----|----------------|
| Kiro | `.kiro/settings/mcp.json` |
| Cursor | `.cursor/mcp.json` |
| Claude Code | `.mcp.json` |
| Windsurf | `.windsurf/mcp.json` |
| VSCode | `.vscode/mcp.json` |
| Trae | `.trae/mcp.json` |
| OpenCode | `opencode.json` |

</details>

## 🛠️ 8つのMCPツール

### `remember` — 記憶を保存

```
content (string, 必須)   記憶内容、Markdown形式
tags    (string[], 必須)  タグ、例 ["つまずき", "python"]
scope   (string)          "project"（デフォルト）/ "user"（プロジェクト横断）
```

類似度 > 0.95 で既存の記憶を自動更新、重複保存なし。

### `recall` — セマンティック検索

```
query   (string)     セマンティック検索キーワード
tags    (string[])   タグ精密フィルター
scope   (string)     "project" / "user" / "all"
top_k   (integer)    返却数、デフォルト 5
```

ベクトル類似度マッチング — 異なる言葉でも関連する記憶を発見。

### `forget` — 記憶を削除

```
memory_id  (string)     単一ID
memory_ids (string[])   一括ID
```

### `status` — セッション状態

```
state (object, 任意)   省略=読み取り、指定=更新
  is_blocked, block_reason, current_task,
  next_step, progress[], recent_changes[], pending[]
```

セッション間で作業進捗を維持、新セッションで自動的にコンテキストを復元。

### `track` — 問題追跡

```
action   (string)   "create" / "update" / "archive" / "list"
title    (string)   問題タイトル
issue_id (integer)  問題ID
status   (string)   "pending" / "in_progress" / "completed"
content  (string)   調査内容
```

### `task` — タスク管理

```
action     (string, 必須)  "batch_create" / "update" / "list" / "delete" / "archive"
feature_id (string)        関連機能識別子（list 時必須）
tasks      (array)         タスクリスト（batch_create、サブタスク対応）
task_id    (integer)       タスクID（update）
status     (string)        "pending" / "in_progress" / "completed" / "skipped"
```

feature_id で spec ドキュメントと連携。update で tasks.md チェックボックスと関連イシューステータスを自動同期。

### `readme` — README生成

```
action   (string)    "generate"（デフォルト）/ "diff"（差分比較）
lang     (string)    言語：en / zh-TW / ja / de / fr / es
sections (string[])  指定セクション：header / tools / deps
```

TOOL_DEFINITIONS / pyproject.toml から README コンテンツを自動生成、多言語対応。

### `auto_save` — プリファレンス自動保存

```
preferences  (string[])  ユーザーが表明した技術的プリファレンス（固定 scope=user、プロジェクト横断）
extra_tags   (string[])  追加タグ
```

各会話の終了時にユーザープリファレンスを自動抽出・保存、スマート重複排除。

## 📊 Webダッシュボード

```bash
run web --port 9080
run web --port 9080 --quiet          # リクエストログを非表示
run web --port 9080 --quiet --daemon  # バックグラウンド実行（macOS/Linux）
```

ブラウザで `http://localhost:9080` にアクセス。デフォルトユーザー名 `admin`、パスワード `admin123`（初回ログイン後に設定で変更可能）。

- マルチプロジェクト切り替え、記憶の閲覧/検索/編集/削除/エクスポート/インポート
- セマンティック検索（ベクトル類似度マッチング）
- プロジェクトデータのワンクリック削除
- セッション状態、問題追跡
- タグ管理（名前変更、統合、一括削除）
- Token認証保護
- 3Dベクトル記憶ネットワーク可視化
- 🌐 多言語対応（简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語）

<p align="center">
  <img src="003.png" alt="ログイン画面" width="100%">
  <br>
  <em>ログイン画面</em>
</p>

<p align="center">
  <img src="dashboard-projects.png" alt="プロジェクト選択" width="100%">
  <br>
  <em>プロジェクト選択</em>
</p>

<p align="center">
  <img src="dashboard-overview.png" alt="統計概要 & ベクトルネットワーク可視化" width="100%">
  <br>
  <em>統計概要 & ベクトルネットワーク可視化</em>
</p>

<p align="center">
  <img src="20260306234753_6_1635.jpg" alt="WeChatグループ" width="280">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="8_1635.jpg" alt="QQグループ：1085682431" width="280">
  <br>
  <em>WeChatグループに参加 &nbsp;|&nbsp; QQグループに参加</em>
</p>

## ⚡ Steeringルールとの組み合わせ

AIVectorMemoryはストレージ層です。Steeringルールを使ってAIに**いつ、どのように**ツールを呼び出すかを指示します。

`run install` を実行すると、Steeringルールとフック設定が自動生成されます。手動設定は不要です。

| IDE | Steeringの場所 | Hooks |
|-----|---------------|-------|
| Kiro | `.kiro/steering/aivectormemory.md` | `.kiro/hooks/*.hook` |
| Cursor | `.cursor/rules/aivectormemory.md` | `.cursor/hooks.json` |
| Claude Code | `CLAUDE.md`（追記） | `.claude/settings.json` |
| Windsurf | `.windsurf/rules/aivectormemory.md` | `.windsurf/hooks.json` |
| VSCode | `.github/copilot-instructions.md`（追記） | `.claude/settings.json` |
| Trae | `.trae/rules/aivectormemory.md` | — |
| OpenCode | `AGENTS.md`（追記） | `.opencode/plugins/*.js` |

<details>
<summary>📋 Steeringルール例（自動生成）</summary>

```markdown
# AIVectorMemory - ワークフロールール

## 1. 新セッション起動（順番に実行必須）

1. `recall`（tags: ["プロジェクト知識"], scope: "project", top_k: 100）プロジェクト知識を読み込み
2. `recall`（tags: ["preference"], scope: "user", top_k: 20）ユーザー設定を読み込み
3. `status`（stateなし）セッション状態を読み取り
4. ブロック中 → 報告して待機；ブロックなし → 処理フローへ

## 2. メッセージ処理フロー

- ステップA：`status` で状態読み取り、ブロック中なら待機
- ステップB：メッセージ種別判定（雑談/修正/設定/コード問題）
- ステップC：`track create` で問題記録
- ステップD：調査（`recall` でつまずき検索 + コード確認 + 根本原因特定）
- ステップE：ユーザーに方針説明、ブロック設定して確認待ち
- ステップF：コード修正（修正前に `recall` でつまずき確認）
- ステップG：テスト実行で検証
- ステップH：ブロック設定してユーザー検証待ち
- ステップI：ユーザー確認 → `track archive` + ブロック解除

## 3. ブロッキングルール

方針提案時・修正完了検証待ち時は必ず `status({ is_blocked: true })`。
ユーザーの明確な確認後のみブロック解除可能。自己解除禁止。

## 4-9. 問題追跡 / コードチェック / Specタスク管理 / 記憶品質 / ツール一覧 / 開発規範

（完全なルールは `run install` で自動生成）
```

</details>

<details>
<summary>🔗 フック設定例（Kiro専用、自動生成）</summary>

セッション終了時の自動保存は削除済み、開発ワークフローチェック（`.kiro/hooks/dev-workflow-check.kiro.hook`）：

```json
{
  "enabled": true,
  "name": "開発ワークフローチェック",
  "version": "1",
  "when": { "type": "promptSubmit" },
  "then": {
    "type": "askAgent",
    "prompt": "核心原則：行動前に検証、盲目的なテスト禁止、テスト合格後のみ完了とマーク"
  }
}
```

</details>

## 🇨🇳 中国本土のユーザー

初回実行時にEmbeddingモデル（約200MB）が自動ダウンロードされます。遅い場合：

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

またはMCP設定にenvを追加：

```json
{
  "env": { "HF_ENDPOINT": "https://hf-mirror.com" }
}
```

## 📦 技術スタック

| コンポーネント | 技術 |
|---------------|------|
| ランタイム | Python >= 3.10 |
| ベクトルDB | SQLite + sqlite-vec |
| Embedding | ONNX Runtime + intfloat/multilingual-e5-small |
| トークナイザー | HuggingFace Tokenizers |
| プロトコル | Model Context Protocol (MCP) |
| Web | ネイティブHTTPServer + Vanilla JS |

## 📋 更新履歴

### v1.0.3

**recall 検索最適化**
- 🔍 `recall` に `tags_mode` パラメータ追加：`any`（OR マッチ）/ `all`（AND マッチ）
- 🔍 `query + tags` 時はデフォルトで OR マッチ（いずれかのタグ一致で候補入り）、複数タグ検索の漏れを解決
- 🔍 `tags` のみの場合は AND マッチ維持（正確なカテゴリ閲覧）、後方互換
- 📝 Steering ルールに検索ガイドラインを追加

### v0.2.8

**Web ダッシュボード**
- 📋 アーカイブ問題詳細モーダル：アーカイブカードをクリックすると読み取り専用の詳細を表示（全構造化フィールド：調査過程/根本原因/解決策/テスト結果/変更ファイル）、下部の赤い削除ボタンでアーカイブ記録を完全削除

**Steering ルール強化**
- 📝 `track create` で `content` フィールドが必須に（問題の現象と背景を簡潔に記述）、タイトルのみの登録を禁止
- 📝 調査後の `track update` で `investigation`（調査過程）と `root_cause`（根本原因）の記入を必須化
- 📝 修正後の `track update` で `solution`（解決策）、`files_changed`（変更ファイル）、`test_result`（テスト結果）の記入を必須化
- 📝 第4節に「フィールド記入規範」サブセクションを追加、各段階の必須フィールドを明確化
- 📝 第5節を「コード修正チェック」から「操作前チェック」に拡張、ダッシュボード起動/PyPI公開/サービス再起動前のrecall踩坑記録ルールを追加
- 📝 `install.py` STEERING_CONTENT を全変更と同期

**ツール最適化**
- 🔧 `track` ツールの `content` フィールド説明を「調査内容」から「問題説明（create時必須、問題の現象と背景を簡潔に記述）」に変更

### v0.2.7

**自動キーワード抽出**
- 🔑 `remember`/`auto_save` がコンテンツからキーワードを自動抽出してタグに追加 — AIが完全なタグを手動で渡す必要がなくなりました
- 🔑 jieba中国語分詞 + 英語正規表現抽出を採用、中英混合コンテンツでも高品質なキーワードを正確に抽出
- 🔑 新しい依存関係 `jieba>=0.42`

### v0.2.6

**Steeringルール再構築**
- 📝 Steeringルールドキュメントを旧3セクション構造から9セクション構造に書き換え（セッション開始/メッセージ処理/ブロッキングルール/問題追跡/コードレビュー/Specタスク管理/メモリ品質/ツールリファレンス/開発規範）
- 📝 `install.py` STEERING_CONTENTテンプレート同期更新、新プロジェクトはインストール時に更新されたルールを取得
- 📝 タグを固定リストから動的抽出に変更（コンテンツからキーワードを抽出）、メモリ検索精度を向上

**バグ修正**
- 🐛 `readme`ツール `handle_readme()` に `**_` が不足、MCP呼び出しエラー `unexpected keyword argument 'engine'` を修正
- 🐛 Webダッシュボードメモリ検索ページネーション修正（検索クエリ時にページネーション前に全量フィルタリング、不完全な検索結果を修正）

**ドキュメント更新**
- 📖 READMEツール数 7→8、アーキテクチャ図 `digest`→`task`、`task`/`readme`ツール説明追加
- 📖 `auto_save`パラメータを旧 `decisions[]/modifications[]/pitfalls[]/todos[]` から `preferences[]/extra_tags[]` に更新
- 📖 Steeringルール例を3セクション形式から9セクション構造サマリーに更新
- 📖 6言語バージョンに同期更新

### v0.2.5

**タスク駆動開発モード**
- 🔗 イシュー追跡（track）とタスク管理（task）を `feature_id` で完全なワークフローに統合：問題発見 → タスク作成 → タスク実行 → ステータス自動同期 → 連動アーカイブ
- 🔄 `task update` でタスクステータス更新時に関連イシューのステータスを自動同期（全完了→completed、進行中あり→in_progress）
- 📦 `track archive` でイシューアーカイブ時に関連タスクを自動アーカイブ（最後のアクティブイシューアーカイブ時に連動）
- 📦 `task` ツールに `archive` アクション追加、機能グループの全タスクを `tasks_archive` テーブルに移動
- 📊 イシューカードに関連タスク進捗を表示（例：`5/10`）、タスクページでアーカイブフィルタリング対応

**新規ツール**
- 🆕 `task` ツール — タスク管理（batch_create/update/list/delete/archive）、ツリー構造サブタスク対応、feature_id で spec ドキュメントと連携
- 🆕 `readme` ツール — TOOL_DEFINITIONS/pyproject.toml から README コンテンツを自動生成、多言語・差分比較対応

**ツール強化**
- 🔧 `track` に delete アクション追加、9 つの構造化フィールド（description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id）、list で issue_id 単一検索
- 🔧 `recall` に source パラメータフィルタリング（manual/auto_save）と brief 簡潔モード（content+tags のみ返却、コンテキスト節約）を追加
- 🔧 `auto_save` がメモリに source="auto_save" を付与、手動メモリと自動保存を区別

**ナレッジベーステーブル分割リファクタリング**
- 🗃️ project_memories + user_memories を独立テーブル化、scope/filter_dir 混合クエリを解消、クエリ性能向上
- 📊 DB Schema v4→v6：issues に 9 つの構造化フィールド追加 + tasks/tasks_archive テーブル + memories.source フィールド

**Web ダッシュボード**
- 📊 トップページにブロック状態カード追加（赤ブロック警告/緑正常稼働）、クリックでセッション状態ページへ遷移
- 📊 タスク管理ページ新規追加（機能グループ折りたたみ/展開、ステータスフィルタ、検索、CRUD）
- 📊 サイドバーナビゲーション順序最適化（セッション状態・イシュー・タスクをコア位置に移動）
- 📊 メモリリストに source フィルタリングと exclude_tags 除外フィルタを追加

**安定性と規範**
- 🛡️ Server メインループにグローバル例外キャッチ追加、単一メッセージエラーで server が終了しなくなった
- 🛡️ Protocol 層に空行スキップと JSON パースエラー耐性を追加
- 🕐 タイムスタンプを UTC からローカルタイムゾーンに変更
- 🧹 冗長コードのクリーンアップ（未使用メソッド・冗長インポート・バックアップファイルの削除）
- 📝 Steering テンプレートに Spec ワークフローとタスク管理セクション、context transfer 継続ルールを追加

### v0.2.4

- 🔇 Stop hook プロンプトを直接指示に変更、Claude Code の重複応答を解消
- 🛡️ Steering ルール auto_save 仕様にショートサーキット保護追加、セッション終了時に他のルールをスキップ
- 🐛 `_copy_check_track_script` 冪等性修正（変更状態を返却し「同期済み」の誤報を回避）
- 🐛 issue_repo delete の `row.get()` が `sqlite3.Row` と非互換の修正（`row.keys()` で判定）
- 🐛 Web ダッシュボード プロジェクト選択ページのスクロール修正（プロジェクトが多い場合にスクロール不可）
- 🐛 Web ダッシュボード CSS 汚染修正（strReplace グローバル置換で 6 つのスタイルセレクタが破損）
- 🔄 Web ダッシュボードの全 confirm() ダイアログをカスタム showConfirm モーダルに置換（メモリ/イシュー/タグ/プロジェクト削除）
- 🔄 Web ダッシュボード削除操作に API エラーレスポンス処理を追加（alert の代わりに toast）
- 🧹 `.gitignore` に `.devmemory/` レガシーディレクトリの無視ルールを追加
- 🧪 pytest 一時プロジェクト DB 残留の自動クリーンアップ（conftest.py セッションフィクスチャ）

### v0.2.3

- 🛡️ PreToolUse Hook：Edit/Write 前に track issue を強制チェック、アクティブな問題がなければ実行拒否（Claude Code / Kiro / OpenCode 対応）
- 🔌 OpenCode プラグインを `@opencode-ai/plugin` SDK 形式にアップグレード（tool.execute.before hook）
- 🔧 `run install` で check_track.sh スクリプトを自動デプロイ、パスを動的に注入
- 🐛 issue_repo archive/delete の `row.get()` が `sqlite3.Row` と非互換な問題を修正
- 🐛 session_id を DB から最新値を読み取ってからインクリメント、マルチインスタンス競合を回避
- 🐛 track date パラメータ形式バリデーション（YYYY-MM-DD）+ issue_id 型バリデーション
- 🐛 Web API リクエスト解析のセキュリティ強化（Content-Length バリデーション + 10MB 上限 + JSON エラーハンドリング）
- 🐛 Tag フィルター scope ロジック修正（`filter_dir is not None` で falsy 判定を置換）
- 🐛 Export ベクトルデータ struct.unpack バイト長バリデーション
- 🐛 Schema バージョン管理マイグレーション（schema_version テーブル + v1/v2/v3 増分マイグレーション）
- 🐛 `__init__.py` バージョン番号同期修正

### v0.2.2

- 🔇 Web ダッシュボード `--quiet` パラメータでリクエストログを非表示
- 🔄 Web ダッシュボード `--daemon` パラメータでバックグラウンド実行（macOS/Linux）
- 🔧 `run install` MCP 設定生成の修正（sys.executable + 完全なフィールド）
- 📋 問題追跡の CRUD とアーカイブ（Web ダッシュボード 追加/編集/アーカイブ/削除 + 記憶関連付け）
- 👆 リスト行の任意の場所をクリックで編集モーダルを表示（記憶/問題/タグ）
- 🔒 セッション継続/コンテキスト転送時にブロッキングルールを強制適用（再確認が必要）

### v0.2.1

- ➕ Webダッシュボードからプロジェクト追加（ディレクトリブラウザ + 手動入力）
- 🏷️ タグのプロジェクト間汚染修正（タグ操作を現在のプロジェクト + グローバル記憶に限定）
- 📐 モーダルページネーション省略記号切り詰め + 幅80%
- 🔌 OpenCode installがauto_saveプラグインを自動生成（session.idleイベントトリガー）
- 🔗 Claude Code / Cursor / Windsurf installがHooks設定を自動生成（セッション終了時の自動保存）
- 🎯 Webダッシュボード操作体験改善（Toastフィードバック、空状態ガイド、エクスポート/インポートツールバー）
- 🔧 統計カードクリックジャンプ（記憶数/問題数クリックで詳細表示）
- 🏷️ タグ管理ページでプロジェクト/グローバルタグの出所を区別（📁/🌐 マーカー）
- 🏷️ プロジェクトカードのタグ数にグローバル記憶のタグを統合

### v0.2.0

- 🔐 WebダッシュボードToken認証
- ⚡ Embeddingベクトルキャッシュ、同一コンテンツの冗長計算なし
- 🔍 recall が query + tags の組み合わせ検索をサポート
- 🗑️ forget が一括削除をサポート（memory_ids パラメータ）
- 📤 記憶のエクスポート/インポート（JSON形式）
- 🔎 Webダッシュボードセマンティック検索
- 🗂️ Webダッシュボードプロジェクト削除ボタン
- 📊 Webダッシュボードパフォーマンス最適化（フルテーブルスキャン排除）
- 🧠 digest スマート圧縮
- 💾 session_id 永続化
- 📏 content 長さ制限保護
- 🏷️ version 動的参照（ハードコードなし）

### v0.1.x

- 初期リリース：7つのMCPツール、Webダッシュボード、3Dベクトル可視化、多言語対応

## License

Apache-2.0
