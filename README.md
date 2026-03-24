🌐 [简体中文](docs/README.zh-CN.md) | [繁體中文](docs/README.zh-TW.md) | English | [Español](docs/README.es.md) | [Deutsch](docs/README.de.md) | [Français](docs/README.fr.md) | [日本語](docs/README.ja.md)

<p align="center">
  <img src="docs/logo.png" alt="AIVectorMemory Logo" width="200">
</p>

<p align="center">
  <img src="docs/image.png" alt="AI Vector Memory Architecture" width="100%">
</p>
<h1 align="center">AIVectorMemory</h1>
<p align="center">
  <strong>Give your AI coding assistant a memory — Cross-session persistent memory MCP Server</strong>
</p>
<p align="center">
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
  <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
</p>

---

> **Still using CLAUDE.md / MEMORY.md as memory?** This Markdown-file memory approach has fatal flaws: the file keeps growing, injecting everything into every session and burning massive tokens; content only supports keyword matching — search "database timeout" and you won't find "MySQL connection pool pitfall"; sharing one file across projects causes cross-contamination; there's no task tracking, so dev progress lives entirely in your head; not to mention the 200-line truncation, manual maintenance, and inability to deduplicate or merge.
>
> **AIVectorMemory is a fundamentally different approach.** Local vector database storage with semantic search for precise recall (matches even when wording differs), on-demand retrieval that loads only relevant memories (token usage drops 50%+), automatic multi-project isolation with zero interference, and built-in issue tracking + task management that lets AI fully automate your dev workflow. All data is permanently stored on your machine — zero cloud dependency, never lost when switching sessions or IDEs.

## ✨ Core Features

| Feature | Description |
|---------|-------------|
| 🧠 **Cross-Session Memory** | Your AI finally remembers your project — pitfalls, decisions, conventions all persist across sessions |
| 🔍 **Semantic Search** | No need to recall exact wording — search "database timeout" and find "MySQL connection pool issue" |
| 💰 **Save 50%+ Tokens** | Stop copy-pasting project context every conversation. Semantic retrieval on demand, no more bulk injection |
| 🔗 **Task-Driven Dev** | Issue tracking → task breakdown → status sync → linked archival. AI manages the full dev workflow |
| 📊 **Desktop App + Web Dashboard** | Native desktop app (macOS/Windows/Linux) + Web dashboard, visual management for memories and tasks, 3D vector network reveals knowledge connections at a glance |
| 🏠 **Fully Local** | Zero cloud dependency. ONNX local inference, no API Key, data never leaves your machine |
| 🔌 **All IDEs** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — one-click install, works out of the box |
| 📁 **Multi-Project Isolation** | One DB for all projects, auto-isolated with zero interference, seamless project switching |
| 🔄 **Smart Dedup** | Similarity > 0.95 auto-merges updates, keeping your memory store clean — never gets messy over time |
| 🌐 **7 Languages** | 简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語, full-stack i18n for dashboard + Steering rules |

<p align="center">
  QQ群：1085682431 &nbsp;|&nbsp; 微信：changhuibiz<br>
  共同参与项目开发加QQ群或微信交流
</p>

<p align="center">
  <img src="docs/003.png" alt="Login" width="100%">
  <br>
  <em>Login</em>
</p>

<p align="center">
  <img src="docs/001.png" alt="Project Selection" width="100%">
  <br>
  <em>Project Selection</em>
</p>

<p align="center">
  <img src="docs/002.png" alt="Overview & Vector Network" width="100%">
  <br>
  <em>Overview & Vector Network</em>
</p>

## 🏗️ Architecture

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
│  │     SQLite + sqlite-vec (Vector Index)     │  │
│  │     ~/.aivectormemory/memory.db            │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Option 1: pip install (Recommended)

```bash
# Install
pip install aivectormemory

# Upgrade to latest version
pip install --upgrade aivectormemory

# Navigate to your project directory, one-click IDE setup
cd /path/to/your/project
run install
```

`run install` interactively guides you to select your IDE, auto-generating MCP config, Steering rules, and Hooks — no manual setup needed.

> **macOS users note**:
> - If you get `externally-managed-environment` error, add `--break-system-packages`
> - If you get `enable_load_extension` error, your Python doesn't support SQLite extension loading (macOS built-in Python and python.org installers don't support it). Use Homebrew Python instead:
>   ```bash
>   brew install python
>   /opt/homebrew/bin/python3 -m pip install aivectormemory
>   ```

### Option 2: uvx (zero install)

No `pip install` needed, run directly:

```bash
cd /path/to/your/project
uvx aivectormemory install
```

> Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) to be installed. `uvx` auto-downloads and runs the package — no manual installation needed.

### Option 3: Manual configuration

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
<summary>📍 IDE Configuration File Locations</summary>

| IDE | Config Path |
|-----|------------|
| Kiro | `.kiro/settings/mcp.json` |
| Cursor | `.cursor/mcp.json` |
| Claude Code | `.mcp.json` |
| Windsurf | `.windsurf/mcp.json` |
| VSCode | `.vscode/mcp.json` |
| Trae | `.trae/mcp.json` |
| OpenCode | `opencode.json` |

</details>

## 🛠️ 8 MCP Tools

### `remember` — Store a memory

```
content (string, required)   Memory content in Markdown format
tags    (string[], required)  Tags, e.g. ["pitfall", "python"]
scope   (string)              "project" (default) / "user" (cross-project)
```

Similarity > 0.95 auto-updates existing memory, no duplicates.

### `recall` — Semantic search

```
query   (string)     Semantic search keywords
tags    (string[])   Exact tag filter
scope   (string)     "project" / "user" / "all"
top_k   (integer)    Number of results, default 5
```

Vector similarity matching — finds related memories even with different wording.

### `forget` — Delete memories

```
memory_id  (string)     Single ID
memory_ids (string[])   Batch IDs
```

### `status` — Session state

```
state (object, optional)   Omit to read, pass to update
  is_blocked, block_reason, current_task,
  next_step, progress[], recent_changes[], pending[]
```

Maintains work progress across sessions, auto-restores context in new sessions.

### `track` — Issue tracking

```
action   (string)   "create" / "update" / "archive" / "list"
title    (string)   Issue title
issue_id (integer)  Issue ID
status   (string)   "pending" / "in_progress" / "completed"
content  (string)   Investigation content
```

### `task` — Task management

```
action     (string, required)  "batch_create" / "update" / "list" / "delete" / "archive"
feature_id (string)            Linked feature identifier (required for list)
tasks      (array)             Task list (batch_create, supports subtasks)
task_id    (integer)           Task ID (update)
status     (string)            "pending" / "in_progress" / "completed" / "skipped"
```

Links to spec docs via feature_id. Update auto-syncs tasks.md checkboxes and linked issue status.

### `readme` — README generation

```
action   (string)    "generate" (default) / "diff" (compare differences)
lang     (string)    Language: en / zh-TW / ja / de / fr / es
sections (string[])  Specify sections: header / tools / deps
```

Auto-generates README content from TOOL_DEFINITIONS / pyproject.toml, multi-language support.

### `auto_save` — Auto save preferences

```
preferences  (string[])  User-expressed technical preferences (fixed scope=user, cross-project)
extra_tags   (string[])  Additional tags
```

Auto-extracts and stores user preferences at end of each conversation, smart dedup.

## 📊 Web Dashboard

```bash
run web --port 9080
run web --port 9080 --quiet          # Suppress request logs
run web --port 9080 --quiet --daemon  # Run in background (macOS/Linux)
```

Visit `http://localhost:9080` in your browser. Default username `admin`, password `admin123` (can be changed in settings after first login).

- Multi-project switching, memory browse/search/edit/delete/export/import
- Semantic search (vector similarity matching)
- One-click project data deletion
- Session status, issue tracking
- Tag management (rename, merge, batch delete)
- Token authentication protection
- 3D vector memory network visualization
- 🌐 Multi-language support (简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語)

## ⚡ Pairing with Steering Rules

AIVectorMemory is the storage layer. Use Steering rules to tell AI **when and how** to call these tools.

Running `run install` auto-generates Steering rules and Hooks config — no manual setup needed.

| IDE | Steering Location | Hooks |
|-----|------------------|-------|
| Kiro | `.kiro/steering/aivectormemory.md` | `.kiro/hooks/*.hook` |
| Cursor | `.cursor/rules/aivectormemory.md` | `.cursor/hooks.json` |
| Claude Code | `CLAUDE.md` (appended) | `.claude/settings.json` |
| Windsurf | `.windsurf/rules/aivectormemory.md` | `.windsurf/hooks.json` |
| VSCode | `.github/copilot-instructions.md` (appended) | `.claude/settings.json` |
| Trae | `.trae/rules/aivectormemory.md` | — |
| OpenCode | `AGENTS.md` (appended) | `.opencode/plugins/*.js` |

<details>
<summary>📋 Steering Rules Example (auto-generated)</summary>

```markdown
# AIVectorMemory - Workflow Rules

## 1. New Session Startup (execute in order)

1. `recall` (tags: ["project-knowledge"], scope: "project", top_k: 100) load project knowledge
2. `recall` (tags: ["preference"], scope: "user", top_k: 20) load user preferences
3. `status` (no state param) read session state
4. Blocked → report and wait; Not blocked → enter processing flow

## 2. Message Processing Flow

- Step A: `status` read state, wait if blocked
- Step B: Classify message type (chat/correction/preference/code issue)
- Step C: `track create` record issue
- Step D: Investigate (`recall` pitfalls + read code + find root cause)
- Step E: Present plan to user, set blocked awaiting confirmation
- Step F: Modify code (`recall` pitfalls before changes)
- Step G: Run tests to verify
- Step H: Set blocked awaiting user verification
- Step I: User confirms → `track archive` + clear block

## 3. Blocking Rules

Must `status({ is_blocked: true })` when proposing plans or awaiting verification.
Only clear after explicit user confirmation. Never self-clear.

## 4-9. Issue Tracking / Code Checks / Spec Task Mgmt / Memory Quality / Tool Reference / Dev Standards

(Full rules auto-generated by `run install`)
```

</details>

<details>
<summary>🔗 Hooks Config Example (Kiro only, auto-generated)</summary>

Auto-save on session end removed. Dev workflow check (`.kiro/hooks/dev-workflow-check.kiro.hook`):

```json
{
  "enabled": true,
  "name": "Dev Workflow Check",
  "version": "1",
  "when": { "type": "promptSubmit" },
  "then": {
    "type": "askAgent",
    "prompt": "Core principles: verify before acting, no blind testing, only mark done after tests pass"
  }
}
```

</details>

## 🇨🇳 Users in China

The embedding model (~200MB) is auto-downloaded on first run. If slow:

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

Or add env to MCP config:

```json
{
  "env": { "HF_ENDPOINT": "https://hf-mirror.com" }
}
```

## 📦 Tech Stack

| Component | Technology |
|-----------|-----------|
| Runtime | Python >= 3.10 |
| Vector DB | SQLite + sqlite-vec |
| Embedding | ONNX Runtime + intfloat/multilingual-e5-small |
| Tokenizer | HuggingFace Tokenizers |
| Protocol | Model Context Protocol (MCP) |
| Web | Native HTTPServer + Vanilla JS |

## 📋 Changelog

### v1.0.8

- 🔧 Fix PyPI package size anomaly (sdist from 32MB down to 230KB), excluded accidentally packaged dev files

### v1.0.6

**New: Native Desktop App**
- 🖥️ Native desktop client supporting macOS (ARM64), Windows (x64), Linux (x64)
- 🖥️ Desktop app shares the same database as Web dashboard, fully feature-equivalent
- 🖥️ Dark/light theme switching, Glass frosted visual style
- 🖥️ Login auth, project selection, stats overview, memory management, issue tracking, task management, tag management, settings, data maintenance — full feature coverage
- 📦 Auto-published installers via GitHub Releases, download and use

**New: CI/CD Auto Build**
- 🔄 GitHub Actions auto-builds desktop installers for all 3 platforms
- 🔄 Push a tag to trigger the full compile, package, and release pipeline

**Fixes**
- 🐛 Windows platform compatibility fixes
- 🐛 sqlite-vec extension download URL fix

### v1.0.5

**Optimization: Token Usage Reduction**
- ⚡ Steering rules changed from per-message dynamic injection to static loading, reducing repeated token consumption
- ⚡ Greatest impact for Claude Code users — ~2K fewer tokens per message

### v1.0.4

**New: Full-Stack i18n (7 Languages)**
- 🌐 Web dashboard + desktop UI fully supports 7 languages: 简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語
- 🌐 One-click language switch in settings page, takes effect immediately
- 🌐 MCP tool responses follow language setting, AI replies automatically use the corresponding language
- 🌐 Switching language auto-regenerates steering rules for all installed projects

**New: Web Dashboard Settings Page**
- ⚙️ Language switch, theme settings, system info display
- ⚙️ Database health check, repair, backup and other maintenance tools

### v1.0.3

**Optimization: Memory Search**
- 🔍 `recall` search supports OR/AND tag matching modes, fixing missed results with multi-tag searches
- 🔍 Semantic search + tag filter defaults to OR matching (broader), tags-only browsing keeps AND matching (more precise)

<details>
<summary>📋 v0.2.x and earlier changelog</summary>

See [CHANGELOG-archive.md](docs/CHANGELOG-archive.md)

</details>

## 🌐 HTTP API

除了 MCP 协议，还提供了 HTTP API 接口，便于多个 Agent 直接调用。

### 启动 HTTP 服务器

```bash
# 需要 Node.js 环境
cd scripts
node aivectormemory-http-server.js
```

服务器默认端口：`9081`

### API 端点

| 端点 | 方法 | 功能 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/remember` | POST | 存入记忆 |
| `/recall` | POST | 语义搜索 |
| `/forget` | POST | 删除记忆 |
| `/status` | POST | 会话状态 |
| `/track` | POST | 问题跟踪 |
| `/task` | POST | 任务管理 |
| `/readme` | POST | README 生成 |
| `/auto_save` | POST | 自动保存偏好 |

### 使用示例

```bash
# 健康检查
curl http://localhost:9081/health

# 存入记忆
curl -X POST http://localhost:9081/remember \
  -H "Content-Type: application/json" \
  -d '{"content": "记住这个项目使用 Python 3.11", "tags": ["python", "config"]}'

# 语义搜索
curl -X POST http://localhost:9081/recall \
  -H "Content-Type: application/json" \
  -d '{"query": "Python 版本", "top_k": 5}'

# 删除记忆
curl -X POST http://localhost:9081/forget \
  -H "Content-Type: application/json" \
  -d '{"memory_id": "xxx"}'

# 会话状态
curl -X POST http://localhost:9081/status \
  -H "Content-Type: application/json" \
  -d '{"state": {"current_task": "开发新功能"}}'

# 问题跟踪
curl -X POST http://localhost:9081/track \
  -H "Content-Type: application/json" \
  -d '{"action": "create", "title": "修复 Bug", "content": "xxx"}'

# 任务管理
curl -X POST http://localhost:9081/task \
  -H "Content-Type: application/json" \
  -d '{"action": "batch_create", "feature_id": "feature-001", "tasks": [{"title": "任务1"}]}'

# README 生成
curl -X POST http://localhost:9081/readme \
  -H "Content-Type: application/json" \
  -d '{"lang": "zh-CN"}'

# 自动保存偏好
curl -X POST http://localhost:9081/auto_save \
  -H "Content-Type: application/json" \
  -d '{"preferences": ["喜欢用 Python", "喜欢用 TypeScript"]}'
```

## License

Apache-2.0
