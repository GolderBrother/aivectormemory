🌐 [简体中文](README.zh-CN.md) | 繁體中文 | [English](../README.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [Français](README.fr.md) | [日本語](README.ja.md)

<p align="center">
  <img src="logo.png" alt="AIVectorMemory Logo" width="200">
</p>
<h1 align="center">AIVectorMemory</h1>
<p align="center">
  <strong>為 AI 程式助手裝上記憶 — 跨會話持久化記憶 MCP Server</strong>
</p>
<p align="center">
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
  <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
</p>
---

> **你還在用 CLAUDE.md / MEMORY.md 當記憶？** 這類 Markdown 檔案記憶模式有致命缺陷：檔案越寫越大，每次會話全量注入吃掉大量 Token；內容只能關鍵詞匹配，搜「資料庫逾時」找不到「MySQL 連線池踩坑」；多專案共用一個檔案互相污染；沒有任務追蹤，開發進度全靠人腦記；更別提 200 行截斷、手動維護、無法去重合併這些日常痛點了。
>
> **AIVectorMemory 是完全不同的方案。** 本地向量資料庫儲存，語義搜尋精準召回（用詞不同也能匹配），按需檢索只載入相關記憶（Token 消耗直降 50%+），多專案自動隔離互不干擾，內建問題追蹤 + 任務管理讓 AI 全自動管理開發流程。所有資料永久保存在你的電腦上，零雲端依賴，換會話、換 IDE 都不丟。

## ✨ 核心特性

| 特性 | 說明 |
|------|------|
| 🧠 **跨會話記憶** | AI 終於能記住你的專案了 — 踩過的坑、做過的決策、定下的規範，換個會話照樣記得 |
| 🔍 **語義搜尋** | 不用記原文怎麼寫的，搜「資料庫逾時」就能找到「MySQL 連線池踩坑」 |
| 💰 **省 50%+ Token** | 不再每次對話都複製貼上專案背景，語義檢索按需召回，告別全量上下文注入 |
| 🔗 **任務驅動開發** | 問題追蹤 → 任務拆分 → 狀態同步 → 聯動歸檔，AI 自動管理完整開發流程 |
| 📊 **Web 看板** | 視覺化管理所有記憶和任務，3D 向量網路一眼看清知識關聯 |
| 🏠 **完全本地** | 零依賴雲端服務，ONNX 本地推理，無需 API Key，資料不出你的電腦 |
| 🔌 **全 IDE 通吃** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — 一鍵安裝，開箱即用 |
| 📁 **多專案隔離** | 一個 DB 管所有專案，自動隔離互不干擾，切換專案無感知 |
| 🔄 **智慧去重** | 相似度 > 0.95 自動合併更新，記憶庫永遠乾淨，不會越用越亂 |

<p align="center">
  QQ群：1085682431 &nbsp;|&nbsp; 微信：changhuibiz<br>
  共同参与项目开发加QQ群或微信交流
</p>

## 🏗️ 架構

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
│  │     SQLite + sqlite-vec（向量索引）         │  │
│  │     ~/.aivectormemory/memory.db            │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## 🚀 快速開始

### 方式一：pip 安裝（推薦）

```bash
# 安裝
pip install aivectormemory

# 升級到最新版
pip install --upgrade aivectormemory

# 進入你的專案目錄，一鍵配置 IDE
cd /path/to/your/project
run install
```

`run install` 會互動式引導你選擇 IDE，自動生成 MCP 配置、Steering 規則和 Hooks，無需手動編寫。

> **macOS 使用者注意**：
> - 遇到 `externally-managed-environment` 錯誤，加 `--break-system-packages`
> - 遇到 `enable_load_extension` 錯誤，說明當前 Python 不支援 SQLite 擴展載入（macOS 自帶 Python 和 python.org 官方安裝包均不支援），請改用 Homebrew Python：
>   ```bash
>   brew install python
>   /opt/homebrew/bin/python3 -m pip install aivectormemory
>   ```

### 方式二：uvx 執行（零安裝）

無需 `pip install`，直接執行：

```bash
cd /path/to/your/project
uvx aivectormemory install
```

> 需要先安裝 [uv](https://docs.astral.sh/uv/getting-started/installation/)，`uvx` 會自動下載並執行，無需手動安裝套件。

### 方式三：手動配置

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
<summary>📍 各 IDE 設定檔位置</summary>

| IDE | 設定檔路徑 |
|-----|------------|
| Kiro | `.kiro/settings/mcp.json` |
| Cursor | `.cursor/mcp.json` |
| Claude Code | `.mcp.json` |
| Windsurf | `.windsurf/mcp.json` |
| VSCode | `.vscode/mcp.json` |
| Trae | `.trae/mcp.json` |
| OpenCode | `opencode.json` |

</details>

## 🛠️ 8 個 MCP 工具

### `remember` — 存入記憶

```
content (string, 必填)   記憶內容，Markdown 格式
tags    (string[], 必填)  標籤，如 ["踩坑", "python"]
scope   (string)          "project"（預設）/ "user"（跨專案）
```

相似度 > 0.95 自動更新已有記憶，不重複儲存。

### `recall` — 語義搜尋

```
query   (string)     語義搜尋關鍵詞
tags    (string[])   標籤精確過濾
scope   (string)     "project" / "user" / "all"
top_k   (integer)    回傳數量，預設 5
```

向量相似度匹配，用詞不同也能找到相關記憶。

### `forget` — 刪除記憶

```
memory_id  (string)     單個 ID
memory_ids (string[])   批次 ID
```

### `status` — 會話狀態

```
state (object, 可選)   不傳=讀取，傳=更新
  is_blocked, block_reason, current_task,
  next_step, progress[], recent_changes[], pending[]
```

跨會話保持工作進度，新會話自動恢復上下文。

### `track` — 問題追蹤

```
action   (string)   "create" / "update" / "archive" / "list"
title    (string)   問題標題
issue_id (integer)  問題 ID
status   (string)   "pending" / "in_progress" / "completed"
content  (string)   排查內容
```

### `task` — 任務管理

```
action     (string, 必填)  "batch_create" / "update" / "list" / "delete" / "archive"
feature_id (string)        關聯功能標識（list 時必填）
tasks      (array)         任務列表（batch_create，支援子任務）
task_id    (integer)       任務 ID（update）
status     (string)        "pending" / "in_progress" / "completed" / "skipped"
```

透過 feature_id 關聯 spec 文件，update 自動同步 tasks.md checkbox 並聯動問題狀態。

### `readme` — README 生成

```
action   (string)    "generate"（預設）/ "diff"（差異對比）
lang     (string)    語言：en / zh-TW / ja / de / fr / es
sections (string[])  指定章節：header / tools / deps
```

從 TOOL_DEFINITIONS / pyproject.toml 自動生成 README 內容，支援多語言。

### `auto_save` — 自動儲存偏好

```
preferences  (string[])  使用者表達的技術偏好（固定 scope=user，跨專案通用）
extra_tags   (string[])  額外標籤
```

每次對話結束自動提取並儲存使用者偏好，智慧去重。

## 📊 Web 看板

```bash
run web --port 9080
run web --port 9080 --quiet          # 屏蔽請求日誌
run web --port 9080 --quiet --daemon  # 背景執行（macOS/Linux）
```

瀏覽器存取 `http://localhost:9080`，預設用戶名 `admin`，密碼 `admin123`（首次登入後可在設定中修改）

- 多專案切換，記憶瀏覽/搜尋/編輯/刪除/匯出/匯入
- 語義搜尋（向量相似度匹配）
- 專案資料一鍵刪除
- 會話狀態、問題追蹤
- 標籤管理（重新命名、合併、批次刪除）
- Token 認證保護
- 3D 向量記憶網路視覺化
- 🌐 多語言支援（简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語）

<p align="center">
  <img src="003.png" alt="登入介面" width="100%">
  <br>
  <em>登入介面</em>
</p>

<p align="center">
  <img src="dashboard-projects.png" alt="專案選擇" width="100%">
  <br>
  <em>專案選擇</em>
</p>

<p align="center">
  <img src="dashboard-overview.png" alt="統計概覽 & 向量網路視覺化" width="100%">
  <br>
  <em>統計概覽 & 向量網路視覺化</em>
</p>

<p align="center">
  <img src="20260306234753_6_1635.jpg" alt="微信群" width="280">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="8_1635.jpg" alt="QQ群：1085682431" width="280">
  <br>
  <em>微信掃碼加群 &nbsp;|&nbsp; QQ掃碼加群</em>
</p>

## ⚡ 搭配 Steering 規則

AIVectorMemory 是儲存層，透過 Steering 規則告訴 AI **何時、如何**呼叫這些工具。

執行 `run install` 會自動產生 Steering 規則和 Hooks 設定，無需手動編寫。

| IDE | Steering 位置 | Hooks |
|-----|--------------|-------|
| Kiro | `.kiro/steering/aivectormemory.md` | `.kiro/hooks/*.hook` |
| Cursor | `.cursor/rules/aivectormemory.md` | `.cursor/hooks.json` |
| Claude Code | `CLAUDE.md`（追加） | `.claude/settings.json` |
| Windsurf | `.windsurf/rules/aivectormemory.md` | `.windsurf/hooks.json` |
| VSCode | `.github/copilot-instructions.md`（追加） | `.claude/settings.json` |
| Trae | `.trae/rules/aivectormemory.md` | — |
| OpenCode | `AGENTS.md`（追加） | `.opencode/plugins/*.js` |

<details>
<summary>📋 Steering 規則範例（自動產生）</summary>

```markdown
# AIVectorMemory - 工作規則

## 1. 新會話啟動（必須按順序執行）

1. `recall`（tags: ["項目知識"], scope: "project", top_k: 100）載入項目知識
2. `recall`（tags: ["preference"], scope: "user", top_k: 20）載入使用者偏好
3. `status`（不傳 state）讀取會話狀態
4. 有阻塞 → 匯報並等待；無阻塞 → 進入處理流程

## 2. 收到訊息後的處理流程

- 步驟 A：`status` 讀取狀態，有阻塞則等待
- 步驟 B：判斷訊息類型（閒聊/糾正/偏好/程式碼問題）
- 步驟 C：`track create` 記錄問題
- 步驟 D：排查（`recall` 查踩坑 + 查看程式碼 + 找根因）
- 步驟 E：向使用者說明方案，設阻塞等確認
- 步驟 F：修改程式碼（修改前 `recall` 查踩坑）
- 步驟 G：執行測試驗證
- 步驟 H：設阻塞等待使用者驗證
- 步驟 I：使用者確認 → `track archive` + 清阻塞

## 3. 阻塞規則

提方案等確認、修復完等驗證時必須 `status({ is_blocked: true })`。
使用者明確確認後才能清除阻塞，禁止自行清除。

## 4-9. 問題追蹤 / 程式碼檢查 / Spec 任務管理 / 記憶品質 / 工具速查 / 開發規範

（完整規則由 `run install` 自動產生）
```

</details>

<details>
<summary>🔗 Hooks 設定範例（全 IDE 支援，自動產生）</summary>

會話結束自動儲存已移除，開發流程檢查（`.kiro/hooks/dev-workflow-check.kiro.hook`）：

```json
{
  "enabled": true,
  "name": "開發流程檢查",
  "version": "1",
  "when": { "type": "promptSubmit" },
  "then": {
    "type": "askAgent",
    "prompt": "核心原則：操作前驗證、禁止盲目測試、自測通過才能說完成"
  }
}
```

</details>

## 🇨🇳 中國大陸使用者

首次執行自動下載 Embedding 模型（~200MB），如果慢：

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

或在 MCP 設定中加 env：

```json
{
  "env": { "HF_ENDPOINT": "https://hf-mirror.com" }
}
```

## 📦 技術棧

| 元件 | 技術 |
|------|------|
| 執行環境 | Python >= 3.10 |
| 向量資料庫 | SQLite + sqlite-vec |
| Embedding | ONNX Runtime + intfloat/multilingual-e5-small |
| 分詞器 | HuggingFace Tokenizers |
| 協議 | Model Context Protocol (MCP) |
| Web | 原生 HTTPServer + Vanilla JS |

## 📋 更新日誌

### v1.0.3

**recall 搜尋優化**
- 🔍 `recall` 新增 `tags_mode` 參數：`any`（OR 匹配）/ `all`（AND 匹配）
- 🔍 `query + tags` 時預設 OR 匹配（匹配任一標籤即進入候選），解決多標籤搜尋漏結果問題
- 🔍 僅 `tags` 時保持 AND 匹配（精確分類瀏覽），向後相容
- 📝 Steering 規則更新搜尋規範，按搜尋目的選標籤，禁止所有搜尋都帶「踩坑」

### v0.2.8

**Web 看板**
- 📋 歸檔問題詳情彈窗：點擊歸檔卡片彈出唯讀詳情（含所有結構化欄位：排查過程/根因/解決方案/自測結果/修改檔案），底部紅色刪除按鈕支援永久刪除歸檔記錄

**Steering 規則強化**
- 📝 `track create` 強制要求 `content` 必填（簡述問題現象和背景），禁止只傳 title 留空
- 📝 排查後 `track update` 強制要求填充 `investigation`（排查過程）和 `root_cause`（根本原因）
- 📝 修復後 `track update` 強制要求填充 `solution`（解決方案）、`files_changed`（修改檔案）、`test_result`（自測結果）
- 📝 第 4 節新增「欄位填充規範」子節，明確各階段必填欄位
- 📝 第 5 節從「程式碼修改檢查」擴展為「操作前檢查」，新增看板啟動/PyPI 發佈/服務重啟等操作前 recall 踩坑記錄規則
- 📝 `install.py` STEERING_CONTENT 同步全部變更

**工具優化**
- 🔧 `track` 工具 `content` 欄位描述從「排查內容」改為「問題描述（create 時必填，簡述問題現象和背景）」

### v0.2.7

**自動關鍵詞提取**
- 🔑 `remember`/`auto_save` 自動從內容提取關鍵詞補充到 tags，無需 AI 手動傳遞完整標籤
- 🔑 採用 jieba 中文分詞 + 英文正則提取，中英文混合內容均能準確提取高品質關鍵詞
- 🔑 新增 `jieba>=0.42` 依賴

### v0.2.6

**Steering 規則重構**
- 📝 Steering 規則文檔從舊的 3 節結構重寫為 9 節結構（新會話啟動/處理流程/阻塞規則/問題追蹤/代碼檢查/Spec任務管理/記憶品質/工具速查/開發規範）
- 📝 `install.py` STEERING_CONTENT 模板同步更新，新專案安裝即用新規則
- 📝 tags 從固定列表改為動態提取（從內容提取關鍵詞標籤），提升記憶檢索精度

**Bug 修復**
- 🐛 `readme` 工具 `handle_readme()` 缺少 `**_` 導致 MCP 調用報錯 `unexpected keyword argument 'engine'`
- 🐛 Web 看板記憶搜尋分頁修復（有搜尋詞時先全量過濾再分頁，解決搜尋結果不完整問題）

**文檔更新**
- 📖 README 工具數量 7→8、架構圖 `digest`→`task`、工具描述新增 `task`/`readme`
- 📖 `auto_save` 參數從舊的 `decisions[]/modifications[]/pitfalls[]/todos[]` 更新為 `preferences[]/extra_tags[]`
- 📖 Steering 規則範例從 3 節格式更新為 9 節結構摘要
- 📖 同步更新 6 個語言版本（簡體中文/English/Español/Deutsch/Français/日本語）

### v0.2.5

**任務驅動開發模式**
- 🔗 問題追蹤（track）與任務管理（task）透過 `feature_id` 打通成完整鏈路：發現問題 → 建立任務 → 執行任務 → 狀態自動同步 → 聯動歸檔
- 🔄 `task update` 更新任務狀態時自動同步關聯問題狀態（全部完成→completed，有進行中→in_progress）
- 📦 `track archive` 歸檔問題時自動歸檔關聯任務（最後一個活躍問題歸檔時聯動）
- 📦 `task` 工具新增 `archive` action，將功能組所有任務移入 `tasks_archive` 歸檔表
- 📊 問題卡片顯示關聯任務進度（如 `5/10`），任務頁面支援歸檔篩選

**新增工具**
- 🆕 `task` 工具 — 任務管理（batch_create/update/list/delete/archive），支援樹狀子任務，透過 feature_id 關聯 spec 文件
- 🆕 `readme` 工具 — 從 TOOL_DEFINITIONS/pyproject.toml 自動生成 README 內容，支援多語言和差異對比

**工具增強**
- 🔧 `track` 新增 delete action、9 個結構化欄位（description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id）、list 按 issue_id 查單條
- 🔧 `recall` 新增 source 參數過濾（manual/auto_save）和 brief 精簡模式（只回傳 content+tags，節省上下文）
- 🔧 `auto_save` 寫入記憶標記 source="auto_save"，區分手動記憶和自動儲存

**知識庫拆表重構**
- 🗃️ project_memories + user_memories 獨立表，消除 scope/filter_dir 混合查詢，查詢效能提升
- 📊 DB Schema v4→v6：issues 新增 9 個結構化欄位 + tasks/tasks_archive 表 + memories.source 欄位

**Web 看板**
- 📊 首頁新增阻塞狀態卡片（紅色阻塞警告/綠色正常運行），點擊跳轉會話狀態頁
- 📊 新增任務管理頁面（功能組摺疊/展開、狀態篩選、搜尋、CRUD）
- 📊 側邊欄導覽順序最佳化（會話狀態、問題追蹤、任務管理提前至核心位置）
- 📊 記憶列表新增 source 過濾和 exclude_tags 排除過濾

**穩定性與規範**
- 🛡️ Server 主迴圈全域例外捕獲，單條訊息錯誤不再導致 server 退出
- 🛡️ Protocol 層空行跳過和 JSON 解析例外容錯
- 🕐 時間戳從 UTC 改為本地時區
- 🧹 清理冗餘程式碼（刪除無呼叫方法、冗餘匯入、備份檔案）
- 📝 Steering 範本新增 Spec 流程與任務管理章節、context transfer 續接規則

### v0.2.4

- 🔇 Stop hook prompt 改為直接指令，消除 Claude Code 重複回覆
- 🛡️ Steering 規則 auto_save 規範增加短路防護，會話結束場景跳過其他規則
- 🐛 `_copy_check_track_script` 冪等性修復（返回變更狀態避免誤報「已同步」）
- 🐛 issue_repo delete 中 `row.get()` 對 `sqlite3.Row` 不相容修復（改用 `row.keys()` 判斷）
- 🐛 Web 看板專案選擇頁面捲動修復（專案多時無法捲動）
- 🐛 Web 看板 CSS 污染修復（strReplace 全域替換導致 6 處樣式異常）
- 🔄 Web 看板所有 confirm() 彈窗替換為自訂 showConfirm 模態框（記憶/問題/標籤/專案刪除）
- 🔄 Web 看板刪除操作增加 API 錯誤回應處理（toast 提示替代 alert）
- 🧹 `.gitignore` 補充 `.devmemory/` 舊版殘留目錄忽略規則
- 🧪 pytest 臨時專案資料庫殘留自動清理（conftest.py session fixture）

### v0.2.3

- 🛡️ PreToolUse Hook：Edit/Write 前強制檢查 track issue，無活躍問題則拒絕執行（Claude Code / Kiro / OpenCode 三端支援）
- 🔌 OpenCode 外掛升級為 `@opencode-ai/plugin` SDK 格式（tool.execute.before hook）
- 🔧 `run install` 自動部署 check_track.sh 檢查腳本並動態填充路徑
- 🐛 issue_repo archive/delete 中 `row.get()` 對 `sqlite3.Row` 不相容修復
- 🐛 session_id 從 DB 讀取最新值再遞增，避免多實例競態
- 🐛 track date 參數格式校驗（YYYY-MM-DD）+ issue_id 類型校驗
- 🐛 Web API 請求解析安全加固（Content-Length 校驗 + 10MB 上限 + JSON 異常捕獲）
- 🐛 Tag 過濾 scope 邏輯修復（`filter_dir is not None` 替代 falsy 判斷）
- 🐛 Export 向量資料 struct.unpack 位元組長度校驗
- 🐛 Schema 版本化遷移（schema_version 表 + v1/v2/v3 增量遷移）
- 🐛 `__init__.py` 版本號同步修復

### v0.2.2

- 🔇 Web 看板 `--quiet` 參數屏蔽請求日誌
- 🔄 Web 看板 `--daemon` 參數背景執行（macOS/Linux）
- 🔧 `run install` MCP 配置生成修復（sys.executable + 完整欄位）
- 📋 問題追蹤增刪改歸檔（Web 看板新增/編輯/歸檔/刪除 + 記憶關聯）
- 👆 所有列表頁點擊行任意位置彈出編輯彈窗（記憶/問題/標籤）
- 🔒 會話延續/上下文轉移時阻塞規則強制生效（跨會話必須重新確認）

### v0.2.1

- ➕ 看板前端新增專案（目錄瀏覽器 + 手動輸入）
- 🏷️ 標籤跨專案污染修復（標籤操作限定當前專案 + 全域記憶範圍）
- 📐 彈窗分頁省略號截斷 + 寬度 80%
- 🔌 OpenCode install 自動生成 auto_save 外掛（session.idle 事件觸發）
- 🔗 Claude Code / Cursor / Windsurf install 自動生成 Hooks 設定（會話結束自動儲存）
- 🎯 看板互動體驗補全（Toast 回饋、空狀態引導、匯出/匯入工具列）
- 🔧 統計概覽卡片點擊跳轉（點擊記憶數/問題數直接彈窗查看）
- 🏷️ 標籤管理頁區分專案/全域標籤來源（📁/🌐 標記）
- 🏷️ 專案卡片標籤數合併全域記憶標籤

### v0.2.0

- 🔐 Web 看板 Token 認證機制
- ⚡ Embedding 向量快取，相同內容不重複計算
- 🔍 recall 支援 query + tags 組合查詢
- 🗑️ forget 支援批次刪除（memory_ids 參數）
- 📤 記憶匯出/匯入（JSON 格式）
- 🔎 Web 看板語義搜尋
- 🗂️ Web 看板專案刪除按鈕
- 📊 Web 看板效能最佳化（消除全表掃描）
- 🧠 digest 智慧壓縮
- 💾 session_id 持久化
- 📏 content 長度限制保護
- 🏷️ version 動態引用（不再寫死）

### v0.1.x

- 初始版本：7 個 MCP 工具、Web 看板、3D 向量視覺化、多語言支援

## License

Apache-2.0
