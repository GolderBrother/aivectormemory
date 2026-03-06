🌐 简体中文 | [繁體中文](docs/README.zh-TW.md) | [English](docs/README.en.md) | [Español](docs/README.es.md) | [Deutsch](docs/README.de.md) | [Français](docs/README.fr.md) | [日本語](docs/README.ja.md)

<p align="center">
  <img src="docs/logo.png" alt="AIVectorMemory Logo" width="200">
</p>
<h1 align="center">AIVectorMemory</h1>
<p align="center">
  <strong>给 AI 编程助手装上记忆 — 跨会话持久化记忆 MCP Server</strong>
</p>
<p align="center">
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/v/aivectormemory?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/aivectormemory/"><img src="https://img.shields.io/pypi/pyversions/aivectormemory" alt="Python"></a>
  <a href="https://github.com/Edlineas/aivectormemory/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-green" alt="License"></a>
  <a href="https://modelcontextprotocol.io"><img src="https://img.shields.io/badge/MCP-compatible-purple" alt="MCP"></a>
</p>
<p align="center">
  QQ群：1085682431 &nbsp;|&nbsp; 微信：changhuibiz<br>
  源码加QQ群或微信获取
</p>

---

> **你是否也有这样的困扰？** 每开一个新会话，AI 就像换了个人 — 昨天刚教会它的项目规范今天又忘了，踩过的坑还会再踩一遍，开发到一半的进度全部归零。你只能一遍遍复制粘贴项目背景，眼睁睁看着 Token 被重复消耗。
>
> **AIVectorMemory 让 AI 拥有长期记忆。** 所有项目知识、踩坑经验、开发决策、任务进度，跨会话永久保存在本地向量数据库中。新会话自动恢复上下文，语义搜索精准召回，Token 消耗直降 50%+。

## ✨ 核心特性

| 特性 | 说明 |
|------|------|
| 🧠 **跨会话记忆** | AI 终于能记住你的项目了 — 踩过的坑、做过的决策、定下的规范，换个会话照样记得 |
| 🔍 **语义搜索** | 不用记原文怎么写的，搜"数据库超时"就能找到"MySQL 连接池踩坑" |
| 💰 **省 50%+ Token** | 不再每次对话都复制粘贴项目背景，语义检索按需召回，告别全量上下文注入 |
| 🔗 **任务驱动开发** | 问题追踪 → 任务拆分 → 状态同步 → 联动归档，AI 自动管理完整开发流程 |
| 📊 **Web 看板** | 可视化管理所有记忆和任务，3D 向量网络一眼看清知识关联 |
| 🏠 **完全本地** | 零依赖云服务，ONNX 本地推理，无需 API Key，数据不出你的电脑 |
| 🔌 **全 IDE 通吃** | Cursor / Kiro / Claude Code / Windsurf / VSCode / OpenCode / Trae — 一键安装，开箱即用 |
| 📁 **多项目隔离** | 一个 DB 管所有项目，自动隔离互不干扰，切换项目无感知 |
| 🔄 **智能去重** | 相似度 > 0.95 自动合并更新，记忆库永远干净，不会越用越乱 |

## 🏗️ 架构

```
┌─────────────────────────────────────────────────┐
│                   AI IDE                         │
│  OpenCode / Claude Code / Cursor / Kiro / ...   │
└──────────────────────┬──────────────────────────┘
                       │ MCP Protocol (stdio)
┌──────────────────────▼──────────────────────────┐
│              AIVectorMemory Server                    │
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
│  │     SQLite + sqlite-vec (向量索引)          │  │
│  │     ~/.aivectormemory/memory.db                 │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```


## 🚀 快速开始

### 方式一：pip 安装（推荐）

```bash
# 安装
pip install aivectormemory

# 升级到最新版
pip install --upgrade aivectormemory

# 进入你的项目目录，一键配置 IDE
cd /path/to/your/project
run install
```

`run install` 会交互式引导你选择 IDE，自动生成 MCP 配置、Steering 规则和 Hooks，无需手动编写。

> **macOS 用户注意**：
> - 遇到 `externally-managed-environment` 错误，加 `--break-system-packages`
> - 遇到 `enable_load_extension` 错误，说明当前 Python 不支持 SQLite 扩展加载（macOS 自带 Python 和 python.org 官方安装包均不支持），请改用 Homebrew Python：
>   ```bash
>   brew install python
>   /opt/homebrew/bin/python3 -m pip install aivectormemory
>   ```

### 方式二：uvx 运行（零安装）

无需 `pip install`，直接运行：

```bash
cd /path/to/your/project
uvx aivectormemory install
```

> 需要先安装 [uv](https://docs.astral.sh/uv/getting-started/installation/)，`uvx` 会自动下载并运行，无需手动安装包。

### 方式三：手动配置

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
<summary>📍 各 IDE 配置文件位置</summary>

| IDE | 配置文件路径 |
|-----|------------|
| Kiro | `.kiro/settings/mcp.json` |
| Cursor | `.cursor/mcp.json` |
| Claude Code | `.mcp.json` |
| Windsurf | `.windsurf/mcp.json` |
| VSCode | `.vscode/mcp.json` |
| Trae | `.trae/mcp.json` |
| OpenCode | `opencode.json` |

</details>

## 🛠️ 8 个 MCP 工具

### `remember` — 存入记忆

```
content (string, 必填)   记忆内容，Markdown 格式
tags    (string[], 必填)  标签，如 ["踩坑", "python"]
scope   (string)          "project"（默认）/ "user"（跨项目）
```

相似度 > 0.95 自动更新已有记忆，不重复存储。

### `recall` — 语义搜索

```
query   (string)     语义搜索关键词
tags    (string[])   标签精确过滤
scope   (string)     "project" / "user" / "all"
top_k   (integer)    返回数量，默认 5
```

向量相似度匹配，用词不同也能找到相关记忆。

### `forget` — 删除记忆

```
memory_id  (string)     单个 ID
memory_ids (string[])   批量 ID
```

### `status` — 会话状态

```
state (object, 可选)   不传=读取，传=更新
  is_blocked, block_reason, current_task,
  next_step, progress[], recent_changes[], pending[]
```

跨会话保持工作进度，新会话自动恢复上下文。

### `track` — 问题跟踪

```
action   (string)   "create" / "update" / "archive" / "list"
title    (string)   问题标题
issue_id (integer)  问题 ID
status   (string)   "pending" / "in_progress" / "completed"
content  (string)   排查内容
```

### `task` — 任务管理

```
action     (string, 必填)  "batch_create" / "update" / "list" / "delete" / "archive"
feature_id (string)        关联功能标识（list 时必填）
tasks      (array)         任务列表（batch_create，支持子任务）
task_id    (integer)       任务 ID（update）
status     (string)        "pending" / "in_progress" / "completed" / "skipped"
```

通过 feature_id 关联 spec 文档，update 自动同步 tasks.md checkbox 并联动问题状态。

### `readme` — README 生成

```
action   (string)    "generate"（默认）/ "diff"（差异对比）
lang     (string)    语言：en / zh-TW / ja / de / fr / es
sections (string[])  指定章节：header / tools / deps
```

从 TOOL_DEFINITIONS / pyproject.toml 自动生成 README 内容，支持多语言。

### `auto_save` — 自动保存偏好

```
preferences  (string[])  用户表达的技术偏好（固定 scope=user，跨项目通用）
extra_tags   (string[])  额外标签
```

每次对话结束自动提取并存储用户偏好，智能去重。

## 📊 Web 看板

```bash
run web --port 9080
run web --port 9080 --quiet          # 屏蔽请求日志
run web --port 9080 --quiet --daemon  # 后台运行（macOS/Linux）
```

浏览器访问 `http://localhost:9080`

- 多项目切换，记忆浏览/搜索/编辑/删除/导出/导入
- 语义搜索（向量相似度匹配）
- 项目数据一键删除
- 会话状态、问题追踪
- 标签管理（重命名、合并、批量删除）
- Token 认证保护
- 3D 向量记忆网络可视化
- 🌐 多语言支持（简体中文 / 繁體中文 / English / Español / Deutsch / Français / 日本語）

<p align="center">
  <img src="docs/dashboard-projects.png" alt="项目选择" width="100%">
  <br>
  <em>项目选择</em>
</p>

<p align="center">
  <img src="docs/dashboard-overview.png" alt="统计概览 & 向量网络可视化" width="100%">
  <br>
  <em>统计概览 & 向量网络可视化</em>
</p>

<p align="center">
  <img src="docs/20260306234753_6_1635.jpg" alt="微信群" width="280">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="docs/8_1635.jpg" alt="QQ群：1085682431" width="280">
  <br>
  <em>微信扫码加群 &nbsp;|&nbsp; QQ扫码加群</em>
</p>

## ⚡ 配合 Steering 规则

AIVectorMemory 是存储层，通过 Steering 规则告诉 AI **何时、如何**调用这些工具。

运行 `run install` 会自动生成 Steering 规则和 Hooks 配置，无需手动编写。

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
<summary>📋 Steering 规则范例（自动生成）</summary>

```markdown
# AIVectorMemory - 工作规则

## 1. 新会话启动（必须按顺序执行）

1. `recall`（tags: ["项目知识"], scope: "project", top_k: 100）加载项目知识
2. `recall`（tags: ["preference"], scope: "user", top_k: 20）加载用户偏好
3. `status`（不传 state）读取会话状态
4. 有阻塞 → 汇报并等待；无阻塞 → 进入处理流程

## 2. 收到消息后的处理流程

- 步骤 A：`status` 读取状态，有阻塞则等待
- 步骤 B：判断消息类型（闲聊/纠正/偏好/代码问题）
- 步骤 C：`track create` 记录问题
- 步骤 D：排查（`recall` 查踩坑 + 查看代码 + 找根因）
- 步骤 E：向用户说明方案，设阻塞等确认
- 步骤 F：修改代码（修改前 `recall` 查踩坑）
- 步骤 G：运行测试验证
- 步骤 H：设阻塞等待用户验证
- 步骤 I：用户确认 → `track archive` + 清阻塞

## 3. 阻塞规则

提方案等确认、修复完等验证时必须 `status({ is_blocked: true })`。
用户明确确认后才能清除阻塞，禁止自行清除。

## 4-9. 问题追踪 / 代码检查 / Spec 任务管理 / 记忆质量 / 工具速查 / 开发规范

（完整规则由 `run install` 自动生成）
```

</details>

<details>
<summary>🔗 Hooks 配置范例（Kiro 专属，自动生成）</summary>

会话结束自动保存已移除，开发流程检查（`.kiro/hooks/dev-workflow-check.kiro.hook`）：

```json
{
  "enabled": true,
  "name": "开发流程检查",
  "version": "1",
  "when": { "type": "promptSubmit" },
  "then": {
    "type": "askAgent",
    "prompt": "核心原则：操作前验证、禁止盲目测试、自测通过才能说完成"
  }
}
```

</details>

## 🇨🇳 中国大陆用户

首次运行自动下载 Embedding 模型（~200MB），如果慢：

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

或在 MCP 配置中加 env：

```json
{
  "env": { "HF_ENDPOINT": "https://hf-mirror.com" }
}
```

## 📦 技术栈

| 组件 | 技术 |
|------|------|
| 运行时 | Python >= 3.10 |
| 向量数据库 | SQLite + sqlite-vec |
| Embedding | ONNX Runtime + intfloat/multilingual-e5-small |
| 分词器 | HuggingFace Tokenizers |
| 协议 | Model Context Protocol (MCP) |
| Web | 原生 HTTPServer + Vanilla JS |

## 📋 更新日志

### v0.2.8

**Web 看板**
- 📋 归档问题详情弹窗：点击归档卡片弹出只读详情（含所有结构化字段：排查过程/根因/解决方案/自测结果/修改文件），底部红色删除按钮支持永久删除归档记录

**Steering 规则强化**
- 📝 `track create` 强制要求 `content` 必填（简述问题现象和背景），禁止只传 title 留空
- 📝 排查后 `track update` 强制要求填充 `investigation`（排查过程）和 `root_cause`（根本原因）
- 📝 修复后 `track update` 强制要求填充 `solution`（解决方案）、`files_changed`（修改文件）、`test_result`（自测结果）
- 📝 第 4 节新增「字段填充规范」子节，明确各阶段必填字段
- 📝 第 5 节从「代码修改检查」扩展为「操作前检查」，新增看板启动/PyPI 发布/服务重启等操作前 recall 踩坑记录规则
- 📝 `install.py` STEERING_CONTENT 同步全部变更

**工具优化**
- 🔧 `track` 工具 `content` 字段描述从「排查内容」改为「问题描述（create 时必填，简述问题现象和背景）」

### v0.2.7

**自动关键词提取**
- 🔑 `remember`/`auto_save` 自动从内容提取关键词补充到 tags，无需 AI 手动传递完整标签
- 🔑 采用 jieba 中文分词 + 英文正则提取，中英文混合内容均能准确提取高质量关键词
- 🔑 新增 `jieba>=0.42` 依赖

### v0.2.6

**Steering 规则重构**
- 📝 Steering 规则文档从旧的 3 节结构重写为 9 节结构（新会话启动/处理流程/阻塞规则/问题追踪/代码检查/Spec任务管理/记忆质量/工具速查/开发规范）
- 📝 `install.py` STEERING_CONTENT 模板同步更新，新项目安装即用新规则
- 📝 tags 从固定列表改为动态提取（从内容提取关键词标签），提升记忆检索精度

**Bug 修复**
- 🐛 `readme` 工具 `handle_readme()` 缺少 `**_` 导致 MCP 调用报错 `unexpected keyword argument 'engine'`
- 🐛 Web 看板记忆搜索分页修复（有搜索词时先全量过滤再分页，解决搜索结果不完整问题）

**文档更新**
- 📖 README 工具数量 7→8、架构图 `digest`→`task`、工具描述新增 `task`/`readme`
- 📖 `auto_save` 参数从旧的 `decisions[]/modifications[]/pitfalls[]/todos[]` 更新为 `preferences[]/extra_tags[]`
- 📖 Steering 规则范例从 3 节格式更新为 9 节结构摘要
- 📖 同步更新 6 个语言版本（繁體中文/English/Español/Deutsch/Français/日本語）

### v0.2.5

**任务驱动开发模式**
- 🔗 问题跟踪（track）与任务管理（task）通过 `feature_id` 打通成完整链路：发现问题 → 创建任务 → 执行任务 → 状态自动同步 → 联动归档
- 🔄 `task update` 更新任务状态时自动同步关联问题状态（全部完成→completed，有进行中→in_progress）
- 📦 `track archive` 归档问题时自动归档关联任务（最后一个活跃问题归档时联动）
- 📦 `task` 工具新增 `archive` action，将功能组所有任务移入 `tasks_archive` 归档表
- 📊 问题卡片显示关联任务进度（如 `5/10`），任务页面支持归档筛选

**新增工具**
- 🆕 `task` 工具 — 任务管理（batch_create/update/list/delete/archive），支持树形子任务，通过 feature_id 关联 spec 文档
- 🆕 `readme` 工具 — 从 TOOL_DEFINITIONS/pyproject.toml 自动生成 README 内容，支持多语言和差异对比

**工具增强**
- 🔧 `track` 新增 delete action、9 个结构化字段（description/investigation/root_cause/solution/test_result/notes/files_changed/feature_id/parent_id）、list 按 issue_id 查单条
- 🔧 `recall` 新增 source 参数过滤（manual/auto_save）和 brief 精简模式（只返回 content+tags，节省上下文）
- 🔧 `auto_save` 写入记忆标记 source="auto_save"，区分手动记忆和自动保存

**知识库拆表重构**
- 🗃️ project_memories + user_memories 独立表，消除 scope/filter_dir 混合查询，查询性能提升
- 📊 DB Schema v4→v6：issues 新增 9 个结构化字段 + tasks/tasks_archive 表 + memories.source 字段

**Web 看板**
- 📊 首页新增阻塞状态卡片（红色阻塞警告/绿色正常运行），点击跳转会话状态页
- 📊 新增任务管理页面（功能组折叠/展开、状态筛选、搜索、CRUD）
- 📊 侧边栏导航顺序优化（会话状态、问题跟踪、任务管理提前至核心位置）
- 📊 记忆列表新增 source 过滤和 exclude_tags 排除过滤

**稳定性与规范**
- 🛡️ Server 主循环全局异常捕获，单条消息错误不再导致 server 退出
- 🛡️ Protocol 层空行跳过和 JSON 解析异常容错
- 🕐 时间戳从 UTC 改为本地时区
- 🧹 清理冗余代码（删除无调用方法、冗余导入、备份文件）
- 📝 Steering 模板新增 Spec 流程与任务管理章节、context transfer 续接规则

### v0.2.4

- 🔇 Stop hook prompt 改为直接指令，消除 Claude Code 重复回复
- 🛡️ Steering 规则 auto_save 规范增加短路防护，会话结束场景跳过其他规则
- 🐛 `_copy_check_track_script` 幂等性修复（返回变更状态避免误报"已同步"）
- 🐛 issue_repo delete 中 `row.get()` 对 `sqlite3.Row` 不兼容修复（改用 `row.keys()` 判断）
- 🐛 Web 看板项目选择页面滚动修复（项目多时无法滚动）
- 🐛 Web 看板 CSS 污染修复（strReplace 全局替换导致 6 处样式异常）
- 🔄 Web 看板所有 confirm() 弹窗替换为自定义 showConfirm 模态框（记忆/问题/标签/项目删除）
- 🔄 Web 看板删除操作增加 API 错误响应处理（toast 提示替代 alert）
- 🧹 `.gitignore` 补充 `.devmemory/` 旧版残留目录忽略规则
- 🧪 pytest 临时项目数据库残留自动清理（conftest.py session fixture）

### v0.2.3

- 🛡️ PreToolUse Hook：Edit/Write 前强制检查 track issue，无活跃问题则拒绝执行（Claude Code / Kiro / OpenCode 三端支持）
- 🔌 OpenCode 插件升级为 `@opencode-ai/plugin` SDK 格式（tool.execute.before hook）
- 🔧 `run install` 自动部署 check_track.sh 检查脚本并动态填充路径
- 🐛 issue_repo archive/delete 中 `row.get()` 对 `sqlite3.Row` 不兼容修复
- 🐛 session_id 从 DB 读取最新值再递增，避免多实例竞态
- 🐛 track date 参数格式校验（YYYY-MM-DD）+ issue_id 类型校验
- 🐛 Web API 请求解析安全加固（Content-Length 校验 + 10MB 上限 + JSON 异常捕获）
- 🐛 Tag 过滤 scope 逻辑修复（`filter_dir is not None` 替代 falsy 判断）
- 🐛 Export 向量数据 struct.unpack 字节长度校验
- 🐛 Schema 版本化迁移（schema_version 表 + v1/v2/v3 增量迁移）
- 🐛 `__init__.py` 版本号同步修复

### v0.2.2

- 🔇 Web 看板 `--quiet` 参数屏蔽请求日志
- 🔄 Web 看板 `--daemon` 参数后台运行（macOS/Linux）
- 🔧 `run install` MCP 配置生成修复（sys.executable + 完整字段）
- 📋 问题跟踪增删改归档（Web 看板添加/编辑/归档/删除 + 记忆关联）
- 👆 全部列表页点击行任意位置弹出编辑弹窗（记忆/问题/标签）
- 🔒 会话延续/上下文转移时阻塞规则强制生效（跨会话必须重新确认）

### v0.2.1

- ➕ Web 看板前端添加项目（目录浏览器 + 手动输入）
- 🏷️ 标签跨项目污染修复（标签操作限定当前项目 + 全局记忆范围）
- 📐 弹窗分页省略号截断 + 弹窗宽度 80%
- 🔌 OpenCode install 自动生成 auto_save 插件（session.idle 事件触发）
- 🔗 Claude Code / Cursor / Windsurf install 自动生成 Hooks 配置（会话结束自动保存）
- 🎯 Web 看板交互体验补全（Toast 操作反馈、空状态引导、导出/导入工具栏）
- 🔧 统计概览卡片点击跳转（点击记忆数/问题数直接弹窗查看）
- 🏷️ 标签管理页区分项目/全局标签来源（📁/🌐 标记）
- 🏷️ 项目卡片标签数合并全局记忆标签

### v0.2.0

- 🔐 Web 看板 Token 认证机制
- ⚡ Embedding 向量缓存，相同内容不重复计算
- 🔍 recall 支持 query + tags 组合查询
- 🗑️ forget 支持批量删除（memory_ids 参数）
- 📤 记忆导出/导入（JSON 格式）
- 🔎 Web 看板语义搜索
- 🗂️ Web 看板项目删除按钮
- 📊 Web 看板性能优化（消除全表扫描）
- 🧠 digest 智能压缩
- 💾 session_id 持久化
- 📏 content 长度限制保护
- 🏷️ version 动态引用（不再硬编码）

### v0.1.x

- 初始版本：7 个 MCP 工具、Web 看板、3D 向量可视化、多语言支持

## License

Apache-2.0
