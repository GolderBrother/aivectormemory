🌐 [简体中文](../README.md) | [繁體中文](README.zh-TW.md) | English | [Español](README.es.md) | [Deutsch](README.de.md) | [Français](README.fr.md) | [日本語](README.ja.md)

<p align="center">
  <h1 align="center">🧠 AIVectorMemory</h1>
  <p align="center">
    <strong>Give your AI coding assistant a memory — Cross-session persistent memory MCP Server</strong>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
    <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
    <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
    <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
  </p>
</p>

---

> **Sound familiar?** Every new session, your AI starts from scratch — project conventions you taught it yesterday? Forgotten. Pitfalls it already hit? It'll hit them again. Half-finished work? Gone. You end up copy-pasting project context over and over, watching tokens burn on repeat.
>
> **AIVectorMemory gives your AI long-term memory.** All project knowledge, lessons learned, dev decisions, and task progress are permanently stored in a local vector database. New sessions auto-restore context, semantic search retrieves exactly what's needed, and token usage drops 50%+.

## ✨ Core Features

| Feature | Description |
|---------|-------------|
| 🧠 **Cross-Session Memory** | Your AI finally remembers your project — pitfalls, decisions, conventions all persist across sessions |
| 🔍 **Semantic Search** | No need to recall exact wording — search "database timeout" and find "MySQL connection pool issue" |
| 💰 **Save 50%+ Tokens** | Stop copy-pasting project context every conversation. Semantic retrieval on demand, no more bulk injection |
| 🔗 **Task-Driven Dev** | Issue tracking → task breakdown → status sync → linked archival. AI manages the full dev workflow |
| 📊 **Web Dashboard** | Visual management for all memories and tasks, 3D vector network reveals knowledge connections at a glance |
| 🏠 **Fully Local** | Zero cloud dependency. ONNX local inference, no API Key, data never leaves your machine |
| 🔌 **All IDEs** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — one-click install, works out of the box |
| 📁 **Multi-Project Isolation** | One DB for all projects, auto-isolated with zero interference, seamless project switching |
| 🔄 **Smart Dedup** | Similarity > 0.95 auto-merges updates, keeping your memory store clean — never gets messy over time |

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
| Claude Desktop | `~/Library/Application Support/Claude/claude_desktop_config.json` |

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

Visit `http://localhost:9080` in your browser.

- Multi-project switching, memory browse/search/edit/delete/export/import
- Semantic search (vector similarity matching)
- One-click project data deletion
- Session status, issue tracking
- Tag management (rename, merge, batch delete)
- Token authentication protection
- 3D vector memory network visualization
- 🌐 Multi-language support (简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語)

<p align="center">
  <img src="dashboard-projects.png" alt="Project Selection" width="100%">
  <br>
  <em>Project Selection</em>
</p>

<p align="center">
  <img src="dashboard-overview.png" alt="Overview & Vector Network Visualization" width="100%">
  <br>
  <em>Overview & Vector Network Visualization</em>
</p>

## ⚡ Pairing with Steering Rules

AIVectorMemory is the storage layer. Use Steering rules to tell AI **when and how** to call these tools.

Running `run install` auto-generates Steering rules and Hooks config — no manual setup needed.

| IDE | Steering Location | Hooks |
|-----|------------------|-------|
| Kiro | `.kiro/steering/aivectormemory.md` | `.kiro/hooks/*.hook` |
| Cursor | `.cursor/rules/aivectormemory.md` | `.cursor/hooks.json` |
| Claude Code | `CLAUDE.md` (appended) | `.claude/settings.json` |
| Windsurf | `.windsurf/rules/aivectormemory.md` | `.windsurf/hooks.json` |
| VSCode | `.github/copilot-instructions.md` (appended) | — |
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

### v0.2.8

**Web Dashboard**
- 📋 Archived issue detail modal: click archived card to view read-only details (all structured fields: investigation/root cause/solution/test result/files changed), red delete button at bottom for permanent deletion

**Steering Rules Enhancement**
- 📝 `track create` now requires `content` field (describe issue symptoms and context), no more title-only entries
- 📝 Post-investigation `track update` requires `investigation` and `root_cause` fields
- 📝 Post-fix `track update` requires `solution`, `files_changed`, and `test_result` fields
- 📝 Section 4 adds "Field Population Standards" subsection with per-stage required fields
- 📝 Section 5 expanded from "Code Modification Check" to "Pre-Operation Check", adds recall pitfall records before dashboard startup/PyPI publish/service restart
- 📝 `install.py` STEERING_CONTENT synced with all changes

**Tool Optimization**
- 🔧 `track` tool `content` field description changed from "investigation content" to "issue description (required on create)"

### v0.2.7

**Automatic Keyword Extraction**
- 🔑 `remember`/`auto_save` automatically extract keywords from content and append to tags — AI no longer needs to manually pass complete tags
- 🔑 Uses jieba Chinese word segmentation + English regex extraction, accurately extracting high-quality keywords from mixed Chinese-English content
- 🔑 New dependency `jieba>=0.42`

### v0.2.6

**Steering Rules Refactoring**
- 📝 Steering rules document rewritten from old 3-section structure to 9-section structure (Session Startup / Message Processing / Blocking Rules / Issue Tracking / Code Review / Spec Task Management / Memory Quality / Tool Reference / Development Standards)
- 📝 `install.py` STEERING_CONTENT template synced, new projects get updated rules on install
- 📝 Tags changed from fixed lists to dynamic extraction (keywords extracted from content), improving memory retrieval accuracy

**Bug Fixes**
- 🐛 `readme` tool `handle_readme()` missing `**_` caused MCP call error `unexpected keyword argument 'engine'`
- 🐛 Web dashboard memory search pagination fix (full filter before pagination when search query present, fixing incomplete search results)

**Documentation Updates**
- 📖 README tool count 7→8, architecture diagram `digest`→`task`, added `task`/`readme` tool descriptions
- 📖 `auto_save` parameters updated from old `decisions[]/modifications[]/pitfalls[]/todos[]` to `preferences[]/extra_tags[]`
- 📖 Steering rules example updated from 3-section to 9-section structure summary
- 📖 Synced updates across 6 language versions (繁體中文/简体中文/Español/Deutsch/Français/日本語)

### v0.2.5

**Task-Driven Development Mode**
- 🔗 Issue tracking (track) and task management (task) linked via `feature_id` into a complete workflow: discover issue → create tasks → execute tasks → auto-sync status → linked archival
- 🔄 `task update` auto-syncs linked issue status (all completed→completed, any in progress→in_progress)
- 📦 `track archive` auto-archives linked tasks when archiving issues (triggered on last active issue)
- 📦 `task` tool adds `archive` action, moves all tasks in a feature group to `tasks_archive` table
- 📊 Issue cards show linked task progress (e.g. `5/10`), task page supports archive filtering

**New Tools**
- 🆕 `task` tool — task management (batch_create/update/list/delete/archive), tree-structured subtasks, linked to spec docs via feature_id
- 🆕 `readme` tool — auto-generate README content from TOOL_DEFINITIONS/pyproject.toml, multi-language and diff comparison

**Tool Enhancements**
- 🔧 `track` adds delete action, 9 structured fields (description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id), list by issue_id for single item
- 🔧 `recall` adds source parameter filtering (manual/auto_save) and brief mode (returns only content+tags, saves context)
- 🔧 `auto_save` marks memories with source="auto_save", distinguishing manual memories from auto-saves

**Knowledge Base Table Split**
- 🗃️ project_memories + user_memories as independent tables, eliminates scope/filter_dir mixed queries, improved query performance
- 📊 DB Schema v4→v6: issues add 9 structured fields + tasks/tasks_archive tables + memories.source field

**Web Dashboard**
- 📊 Homepage adds block status card (red blocked warning/green normal running), click to jump to session status page
- 📊 New task management page (feature group collapse/expand, status filter, search, CRUD)
- 📊 Sidebar navigation order optimized (session status, issues, tasks moved to core positions)
- 📊 Memory list adds source filtering and exclude_tags exclusion filter

**Stability & Standards**
- 🛡️ Server main loop global exception handling, single message errors no longer crash the server
- 🛡️ Protocol layer blank line skip and JSON parse error tolerance
- 🕐 Timestamps changed from UTC to local timezone
- 🧹 Cleanup redundant code (removed uncalled methods, redundant imports, backup files)
- 📝 Steering template adds Spec workflow and task management section, context transfer continuation rules

### v0.2.4

- 🔇 Stop hook prompt changed to direct instruction, eliminating Claude Code duplicate replies
- 🛡️ Steering rules auto_save spec adds short-circuit protection, skipping other rules on session end
- 🐛 `_copy_check_track_script` idempotency fix (return change status to avoid false "synced" reports)
- 🐛 issue_repo delete `row.get()` incompatibility with `sqlite3.Row` fix (use `row.keys()` check)
- 🐛 Web dashboard project selection page scroll fix (unable to scroll with many projects)
- 🐛 Web dashboard CSS pollution fix (strReplace global replacement corrupted 6 style selectors)
- 🔄 Web dashboard all confirm() dialogs replaced with custom showConfirm modal (memory/issue/tag/project delete)
- 🔄 Web dashboard delete operations add API error response handling (toast instead of alert)
- 🧹 `.gitignore` adds `.devmemory/` legacy directory ignore rule
- 🧪 pytest temp project DB residual auto-cleanup (conftest.py session fixture)

### v0.2.3

- 🛡️ PreToolUse Hook: enforce track issue check before Edit/Write, reject if no active issues (Claude Code / Kiro / OpenCode)
- 🔌 OpenCode plugin upgraded to `@opencode-ai/plugin` SDK format (tool.execute.before hook)
- 🔧 `run install` auto-deploys check_track.sh script with dynamic path injection
- 🐛 Fix issue_repo archive/delete `row.get()` incompatibility with `sqlite3.Row`
- 🐛 Fix session_id race condition: read latest value from DB before incrementing
- 🐛 Add track date format validation (YYYY-MM-DD) + issue_id type validation
- 🐛 Harden Web API request parsing (Content-Length validation + 10MB limit + JSON error handling)
- 🐛 Fix tag filter scope logic (`filter_dir is not None` instead of falsy check)
- 🐛 Add export vector data struct.unpack byte length validation
- 🐛 Schema versioned migration (schema_version table + v1/v2/v3 incremental migration)
- 🐛 Fix `__init__.py` version number sync

### v0.2.2

- 🔇 Web dashboard `--quiet` flag to suppress request logs
- 🔄 Web dashboard `--daemon` flag for background execution (macOS/Linux)
- 🔧 `run install` MCP config generation fix (sys.executable + complete fields)
- 📋 Issue tracking CRUD & archive (Web dashboard add/edit/archive/delete + memory association)
- 👆 Click anywhere on list row to open edit modal (memories/issues/tags)
- 🔒 Blocking rules enforced across session continuations/context transfers (re-confirmation required)

### v0.2.1

- ➕ Add projects from Web dashboard (directory browser + manual input)
- 🏷️ Fix tag cross-project pollution (tag operations scoped to current project + global memories)
- 📐 Modal pagination ellipsis truncation + 80% width
- 🔌 OpenCode install auto-generates auto_save plugin (session.idle event trigger)
- 🔗 Claude Code / Cursor / Windsurf install auto-generates Hooks config (auto-save on session end)
- 🎯 Web dashboard UX improvements (Toast feedback, empty state guides, export/import toolbar)
- 🔧 Stats card click-through (click memory/issue counts to view details)
- 🏷️ Tag management page distinguishes project/global tag sources (📁/🌐 markers)
- 🏷️ Project card tag counts now include global memory tags

### v0.2.0

- 🔐 Web dashboard Token authentication
- ⚡ Embedding vector cache, no redundant computation for identical content
- 🔍 recall supports combined query + tags search
- 🗑️ forget supports batch deletion (memory_ids parameter)
- 📤 Memory export/import (JSON format)
- 🔎 Web dashboard semantic search
- 🗂️ Web dashboard project delete button
- 📊 Web dashboard performance optimization (eliminated full table scans)
- 🧠 digest smart compression
- 💾 session_id persistence
- 📏 content length limit protection
- 🏷️ version dynamic reference (no longer hardcoded)

### v0.1.x

- Initial release: 7 MCP tools, Web dashboard, 3D vector visualization, multi-language support

## License

Apache-2.0
