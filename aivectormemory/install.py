"""aivectormemory install - 交互式为当前项目配置 MCP + Steering 规则"""
import json
import os
import platform
import sys
from pathlib import Path

# (IDE名, MCP配置路径, MCP格式, 是否全局, Steering路径, Steering写入方式, Hooks目录)
# steering_mode: "file"=独立文件 "append"=追加到已有文件 None=不写Steering
# hooks_dir: lambda返回hooks目录路径, None=不支持hooks
IDES = [
    ("Kiro",           lambda root: root / ".kiro/settings/mcp.json",  "standard", False,
     lambda root: root / ".kiro/steering/aivectormemory.md", "file",
     lambda root: root / ".kiro/hooks"),
    ("Cursor",         lambda root: root / ".cursor/mcp.json",         "standard", False,
     lambda root: root / ".cursor/rules/aivectormemory.md", "file",
     lambda root: root / ".cursor"),
    ("Claude Code",    lambda root: root / ".mcp.json",                "standard", False,
     lambda root: root / "CLAUDE.md", "append",
     lambda root: root / ".claude"),
    ("Windsurf",       lambda root: root / ".windsurf/mcp.json",       "standard", False,
     lambda root: root / ".windsurf/rules/aivectormemory.md", "file",
     lambda root: root / ".windsurf"),
    ("VSCode",         lambda root: root / ".vscode/mcp.json",         "standard", False,
     lambda root: root / ".github/copilot-instructions.md", "append", None),
    ("Trae",           lambda root: root / ".trae/mcp.json",           "standard", False,
     lambda root: root / ".trae/rules/aivectormemory.md", "file", None),
    ("OpenCode",       lambda root: root / "opencode.json",            "opencode", False,
     lambda root: root / "AGENTS.md", "append",
     lambda root: root / ".opencode/plugins"),
    ("Claude Desktop", lambda _: _claude_desktop_path(),               "standard", True,
     None, None, None),
]

RUNNERS = [
    ("python -m aivectormemory（pip/pipx 安装）", lambda pdir: (sys.executable, ["-m", "aivectormemory", "--project-dir", pdir])),
    ("uvx aivectormemory（无需安装）", lambda pdir: ("uvx", ["aivectormemory@latest", "--project-dir", pdir])),
]


STEERING_MARKER = "<!-- aivectormemory-steering -->"

STEERING_CONTENT = """# AIVectorMemory - 工作规则

---

## 1. 新会话启动（必须按顺序执行）

1. `recall`（tags: ["项目知识"], scope: "project", top_k: 100）加载项目知识
2. `recall`（tags: ["preference"], scope: "user", top_k: 20）加载用户偏好
3. `status`（不传 state）读取会话状态
4. 有阻塞（is_blocked=true）→ 汇报阻塞状态，等待用户反馈，**禁止执行任何操作**
5. 无阻塞 → 进入「收到消息后的处理流程」

---

## 2. 收到消息后的处理流程

**步骤 A：调用 `status` 读取状态**
- 有阻塞 → 汇报并等待，禁止操作
- 无阻塞 → 继续

**步骤 B：判断消息类型**
- 闲聊/进度/讨论规则/简单确认 → 直接回答，流程结束
- 用户纠正错误行为/连续犯错提醒 → 立即 `remember`（tags: ["踩坑", "行为纠正", ...从内容提取关键词], scope: "project"，含：错误行为、用户原话要点、正确做法），然后继续步骤 C
- 用户表达技术偏好/工作习惯 → `auto_save` 存储偏好
- 其他（代码问题、bug、功能需求）→ 继续步骤 C
- 回复时说明判断结果，如："这是个询问"/"这是个问题，需要记录"

**步骤 C：`track create` 记录问题**
- 无论大小，发现即记录，禁止先修再补
- `content` 必填：简述问题现象和背景，禁止只传 title 留空 content
- `status` 更新 pending

**步骤 D：排查**
- `recall`（query: 相关关键词, tags: ["踩坑", ...从问题提取关键词]）查询踩坑记录
- 必须查看现有实现代码（禁止凭记忆假设）
- 涉及数据存储时确认数据流向
- 禁止盲目测试，必须找到根本原因
- 发现项目架构/约定/关键实现 → `remember`（tags: ["项目知识", ...从内容提取模块/功能关键词], scope: "project"）
- `track update` 记录根因和方案：必须填充 `investigation`（排查过程）、`root_cause`（根本原因）

**步骤 E：向用户说明方案，确定流程分支**
- 排查完成后，根据问题复杂度向用户说明方案：
  - 简单修复（单文件、bug、配置）→ 继续步骤 F（track 修复流程）
  - 多步骤需求（新功能、重构、升级）→ 用户确认后转 spec/task 流程（见第6节）
- 无论哪个分支，都必须先等用户确认后才能执行
- 立即 `status({ is_blocked: true, block_reason: "方案待用户确认" })`
- 禁止只口头说"等待确认"而不设阻塞，否则会话转移后新会话会误判为已确认
- 等待用户确认

**步骤 F：用户确认后修改代码**
- 修改前 `recall`（query: 涉及的模块/功能, tags: ["踩坑", ...从模块/功能提取关键词]）检查踩坑记录
- 修改前必须查看代码严谨思考
- 一次只修一个问题
- 修复中发现新问题 → `track create` 记录后继续当前问题
- 用户中途打断提出新问题 → `track create` 记录，再决定优先级

**步骤 G：运行测试验证**
- 运行相关测试，禁止口头承诺
- `track update` 记录自测结果：必须填充 `solution`（解决方案）、`files_changed`（修改文件）、`test_result`（自测结果）

**步骤 H：等待用户验证**
- 立即 `status({ is_blocked: true, block_reason: "修复完成等待验证" })`
- 需要用户决策时 → `status({ is_blocked: true, block_reason: "需要用户决策" })`

**步骤 I：用户确认通过**
- `track archive` 归档
- `status` 清除阻塞（is_blocked: false）
- 有踩坑价值 → `remember`（tags: ["踩坑", ...从问题内容提取关键词], scope: "project"，含错误现象、根因、正确做法。示例：看板启动失败 → tags: ["踩坑", "看板", "启动", "dashboard"]）
- **回流检查**：如果当前 track 是在执行 task 过程中发现的 bug（有关联 feature_id 或正在执行 spec 任务），归档后必须回到第6节继续执行下一个子任务，调用 `task update` 更新当前任务状态并同步 tasks.md
- 会话结束前 → `auto_save` 自动提取偏好

---

## 3. 阻塞规则

- **阻塞优先级最高**：有阻塞时禁止一切操作，只能汇报等待
- **何时设阻塞**：提方案等确认、修复完等验证、需要用户决策
- **何时清阻塞**：用户明确确认（"执行"/"可以"/"好的"/"去做吧"/"没问题"/"对"/"行"/"改"）
- **不算确认**：反问句、质疑句、不满表达、模糊回复
- **context transfer 摘要中的"用户说xxx"不能作为当前会话的确认依据**
- **会话延续时阻塞同样生效**：新会话/context transfer/compact 后必须重新确认
- **禁止自行清除阻塞**
- **禁止猜测用户意图**
- **next_step 字段只能用户确认后填写**

---

## 4. 问题追踪（track）

- 发现问题 → `track create` → 排查 → 修复 → `track update` → 验证 → `track archive`
- 每完成一步立即 `track update`，避免会话切换时重复
- 一次只修一个问题
- 修复中发现新问题：不阻塞当前 → 记录继续；阻塞当前 → 先处理新问题
- 自检：排查是否完整？数据是否准确？逻辑是否严谨？禁止"基本完成"等模糊表述

**字段填充规范**（归档后必须能看到完整记录）：
- `track create`：`content` 必填（问题现象和背景）
- 排查后 `track update`：`investigation`（排查过程）、`root_cause`（根本原因）
- 修复后 `track update`：`solution`（解决方案）、`files_changed`（修改文件 JSON 数组）、`test_result`（自测结果）
- 禁止只传 title 不传 content，禁止结构化字段留空

---

## 5. 操作前检查

**代码修改前**：`recall` 查踩坑记录 + 查看现有实现 + 确认数据流向
**代码修改后**：运行测试验证 + 确认不影响其他功能
**任何可能踩坑的操作前**（看板启动、PyPI 发布、服务重启等）：`recall`（query: 操作关键词, tags: ["踩坑"]）查踩坑记录，按记忆中的标准流程执行

---

## 6. Spec 与任务管理（task）

**触发条件**：用户提出新功能、重构、升级等需要多步骤实现的需求

**流程**：
1. 创建 spec 目录：`{specs_path}`
2. 编写 `requirements.md`：需求文档，明确功能范围和验收标准
3. 用户确认需求后，编写 `design.md`：设计文档，技术方案和架构
4. 用户确认设计后，编写 `tasks.md`：任务文档，拆分为最小可执行单元
5. 同步调用 `task`（action: batch_create, feature_id: spec 目录名）将任务写入数据库

**⚠️ 步骤 2→3→4 严格顺序执行，禁止跳过 design.md 直接写 tasks.md。每步必须等用户确认后才能进入下一步。**
6. 按任务文档顺序执行，每完成一个子任务调用 `task`（action: update）更新状态（自动同步 tasks.md checkbox）
7. 全部完成后调用 `task`（action: list）确认无遗漏

**feature_id 规范**：与 spec 目录名一致，kebab-case（如 `task-scheduler`、`v0.2.5-upgrade`）

**与 track 分工**：task 管功能开发计划进度，track 管 bug 问题追踪。执行 task 过程中发现 bug → `track create` 记录，修完后继续 task

**任务文档规范**：
- 每个任务细化到最小可执行单元，使用 `- [ ]` 标记状态
- 每完成一个子任务必须立即执行：① `task update` 更新状态 ② 确认 tasks.md 对应条目已更新为 `[x]`。完成一个处理一个，禁止批量完成后统一更新
- 整理任务文档时必须打开设计文档逐条核对，发现遗漏先补充再执行
- 按顺序执行禁止跳过，禁止用"后续迭代"跳过任务
- **开始任务前必须先检查 tasks.md，确认该任务之前的所有任务已标记 `[x]`，有未完成的前置任务必须先完成，禁止跳组执行**

**自检**：整理任务文档时必须打开设计文档逐条核对，发现遗漏先补充再执行。全部完成后 `task list` 确认无遗漏

**不需要 spec 的场景**：单文件修改、简单 bug、配置调整 → 直接 `track create` 走问题追踪流程

---

## 7. 记忆质量要求

- tags 规范：必须包含分类标签（踩坑/项目知识/行为纠正）+ 从内容提取的关键词标签（模块名、功能名、技术词），禁止只打一个分类标签
- 命令类：完整可执行命令，禁止别名缩写
- 流程类：具体步骤，不能只写结论
- 踩坑类：错误现象 + 根因 + 正确做法
- 行为纠正类：错误行为 + 用户原话要点 + 正确做法

---

## 8. 工具速查

| 工具 | 用途 | 关键参数 |
|------|------|----------|
| remember | 存入记忆 | content, tags, scope(project/user) |
| recall | 语义搜索 | query, tags, scope, top_k |
| forget | 删除记忆 | memory_id / memory_ids |
| status | 会话状态 | state(不传=读, 传=更新), clear_fields |
| track | 问题跟踪 | action(create/update/archive/delete/list) |
| task | 任务管理 | action(batch_create/update/list/delete/archive), feature_id |
| readme | README生成 | action(generate/diff), lang, sections |
| auto_save | 保存偏好 | preferences, extra_tags |

**status 字段说明**：
- `is_blocked`：是否阻塞
- `block_reason`：阻塞原因
- `next_step`：下一步（只能用户确认后填写）
- `current_task`：当前任务
- `progress`：只读计算字段，自动从 track + task 聚合，无需手动写入
- `recent_changes`：最近修改（不超过10条）
- `pending`：待处理列表
- `clear_fields`：要清空的列表字段名（如 `["pending"]`），绕过部分 IDE 过滤空数组的问题

---

## 9. 开发规范

**代码风格**：简洁优先，三目运算符 > if-else，短路求值 > 条件判断，模板字符串 > 拼接，不写无意义注释

**Git 工作流**：日常在 `dev` 分支，禁止直接在 master 提交。只有用户明确要求时才提交。提交流程：确认 dev 分支（`git branch --show-current`）→ `git add -A` → `git commit -m "fix: 简述"` → `git push origin dev`。合并到 master 仅用户明确要求时执行。

**IDE 安全**：禁止 `$(...)` + 管道、禁止 `python3 -c` 多行脚本（写 .py 文件）、`lsof -ti:端口` 必须加 ignoreWarning

**自测要求**：禁止让用户手动操作，能自己执行的不要让用户做。自测通过后才能说"等待验证"。

**任务执行**：按顺序执行禁止跳过，全自动，禁止用"后续迭代"跳过。开始任务前必须先检查 tasks.md，确认前置任务全部 `[x]`，有未完成的前置任务必须先完成

**完成标准**：只有完成和未完成，禁止"基本完成"等模糊表述

**内容迁移**：禁止凭记忆重写，必须从原文件逐行复制

**context transfer/compact 续接**：有未完成工作先完成再汇报

**上下文优化**：优先 `grepSearch` 定位，再 `readFile` 读取特定行。代码修改用 `strReplace`，不要先读后写

**错误处理**：反复失败时记录已尝试方法，换思路解决，仍失败则询问用户
"""


HOOKS_CONFIGS = [
    {
        "filename": "dev-workflow-check.kiro.hook",
        "content": {
            "name": "开发流程检查",
            "version": "1.0.0",
            "description": "每次收到用户消息时，检查核心原则、问题处理原则、自测要求",
            "when": {"type": "promptSubmit"},
            "then": {
                "type": "askAgent",
                "prompt": (
                    "## ⚠️ IDENTITY & TONE\n\n"
                    "- Role：你是首席工程师兼高级数据科学家\n"
                    "- Voice：Professional，Concise，Result-Oriented。No \"I hope this helps\"\n"
                    "- Authority：The user is the Lead Architect. Execute explicit commands immediately (not questions).\n\n"
                    "---\n\n"
                    "## ⚠️ 消息类型判断\n\n"
                    "收到用户消息后，严谨认真理解用户消息的意思然后判断消息类型，询问仅限闲聊，进度、讨论规则、简单确认不记录问题文档，其他所有情况必须需要记录问题文档，然后告诉用户方案，等用户确认后再执行\n\n"
                    "**⚠️ 回复时用自然语言说明判断结果**，例如：\n"
                    "- \"这是个询问，验证相应文件代码后回答\"\n"
                    "- \"这是个问题，方案如下...\"\n"
                    "- \"这个问题需要记录\"\n\n"
                    "**⚠️ 消息处理必须严格按流程执行，禁止跳步、省略、合并步骤。每个步骤完成后才能进入下一步，禁止自作主张跳过任何环节。**\n\n"
                    "---\n\n"
                    "## ⚠️ 核心原则\n\n"
                    "1. **任何操作前必须验证，不能假设，不能靠记忆**。\n"
                    "2. **遇到需要处理的问题时禁止盲目测试，必须查看问题对应的代码文件，必须找到问题的根本原因，必须与实际错误对应**。\n"
                    "3. **禁止口头承诺，口头答应，一切以测试通过为准**。\n"
                    "4. **任何文件修改前必须查看代码强制严谨思考**。\n"
                    "5. **开发、自测过程中禁止让用户手动操作，能自己执行的不要让用户做**。\n"
                    "6. **用户要求读取文件时，禁止以「已读过」「上下文已有」为由跳过，必须重新调用工具读取最新内容**。\n\n"
                    "---\n\n"
                    "## ⚠️ IDE 卡死防范\n\n"
                    "- **禁止** `$(...)` + 管道组合\n"
                    "- **禁止** MySQL `-e` 执行多条语句\n"
                    "- **禁止** `python3 -c \"...\"` 执行多行脚本（超过2行必须写成 .py 文件再执行）\n"
                    "- **禁止** `lsof -ti:端口` 不加 ignoreWarning（会被安全检查拦截）\n"
                    "- **正确做法**：SQL 写入 `.sql` 文件用 `< data/xxx.sql` 执行；Python 验证脚本写成 .py 文件用 `python3 xxx.py` 执行；端口检查用 `lsof -ti:端口` + ignoreWarning:true\n\n"
                    "---\n\n"
                    "## ⚠️ 自测要求\n\n"
                    "**禁止让用户手动操作** - 能自己执行的，不要让用户做\n\n"
                    "- Python：`python -m pytest` 或直接运行脚本验证\n"
                    "- MCP Server：通过 stdio 发送 JSON-RPC 消息验证\n"
                    "- Web 看板：Playwright 验证\n"
                    "- 自测通过后才能说\"等待验证\"\n\n"
                    "---\n\n"
                    "## ⚠️ 开发规则\n\n"
                    "> 禁止口头承诺，一切以测试通过为准。\n"
                    "> 任何文件修改前必须强制严谨思考。\n"
                    "> 遇到报错或异常时严禁盲目测试，必须分析问题根本原因。"
                ),
            },
        },
    },
    {
        "filename": "pre-tool-use-check.kiro.hook",
        "content": {
            "name": "代码修改前检查 track issue",
            "version": "1.0.0",
            "description": "Edit/Write 工具执行前，检查当前项目是否有活跃的 track issue，没有则拒绝执行",
            "when": {"type": "preToolUse", "toolTypes": ["write"]},
            "then": {
                "type": "runCommand",
                "command": "",  # 占位，install 时动态填充
            },
        },
    },
]


def _check_track_script_path() -> Path:
    """返回包内 check_track.sh 的路径"""
    return Path(__file__).parent / "hooks" / "check_track.sh"


CLAUDE_CODE_HOOKS_CONFIG = {
    "hooks": {
        "PreToolUse": [
            {
                "matcher": "Edit|Write",
                "hooks": [
                    {
                        "type": "command",
                        "command": "",  # 占位，install 时动态填充实际路径
                        "timeout": 10,
                    }
                ]
            }
        ]
    }
}

CURSOR_HOOKS_CONFIG = {
    "version": 1,
    "hooks": {}
}

WINDSURF_HOOKS_CONFIG = {
    "hooks": {}
}


OPENCODE_PLUGIN_CONTENT = """\
// AIVectorMemory plugin for OpenCode (@opencode-ai/plugin)
// - experimental.chat.system.transform: 注入开发规则到 system prompt（对应 Kiro promptSubmit hook）
// - tool.execute.before: 检查 Edit/Write 前是否有活跃 track issue
import { execSync } from "child_process";
import { homedir } from "os";
import { existsSync } from "fs";
import { join } from "path";

const DB_PATH = join(homedir(), ".aivectormemory", "memory.db");

const DEV_WORKFLOW_RULES = `<ADDITIONAL_INSTRUCTIONS>
## IDENTITY & TONE

- Role: 你是首席工程师兼高级数据科学家
- Voice: Professional, Concise, Result-Oriented. No "I hope this helps"
- Authority: The user is the Lead Architect. Execute explicit commands immediately (not questions).

---

## 消息类型判断

收到用户消息后，严谨认真理解用户消息的意思然后判断消息类型，询问仅限闲聊，进度、讨论规则、简单确认不记录问题文档，其他所有情况必须需要记录问题文档，然后告诉用户方案，等用户确认后再执行

回复时用自然语言说明判断结果，例如：
- "这是个询问，验证相应文件代码后回答"
- "这是个问题，方案如下..."
- "这个问题需要记录"

**⚠️ 消息处理必须严格按流程执行，禁止跳步、省略、合并步骤。每个步骤完成后才能进入下一步，禁止自作主张跳过任何环节。**

---

## 核心原则

1. 任何操作前必须验证，不能假设，不能靠记忆。
2. 遇到需要处理的问题时禁止盲目测试，必须查看问题对应的代码文件，必须找到问题的根本原因，必须与实际错误对应。
3. 禁止口头承诺，口头答应，一切以测试通过为准。
4. 任何文件修改前必须查看代码强制严谨思考。
5. 开发、自测过程中禁止让用户手动操作，能自己执行的不要让用户做。
6. 用户要求读取文件时，禁止以「已读过」「上下文已有」为由跳过，必须重新调用工具读取最新内容。

---

## IDE 卡死防范

- 禁止 $(...) + 管道组合
- 禁止 MySQL -e 执行多条语句
- 禁止 python3 -c "..." 执行多行脚本（超过2行必须写成 .py 文件再执行）
- 禁止 lsof -ti:端口 不加 ignoreWarning（会被安全检查拦截）
- 正确做法：SQL 写入 .sql 文件用 < data/xxx.sql 执行；Python 验证脚本写成 .py 文件用 python3 xxx.py 执行

---

## 自测要求

禁止让用户手动操作 - 能自己执行的，不要让用户做

- Python: python -m pytest 或直接运行脚本验证
- MCP Server: 通过 stdio 发送 JSON-RPC 消息验证
- Web 看板: Playwright 验证
- 自测通过后才能说"等待验证"

---

## 开发规则

> 禁止口头承诺，一切以测试通过为准。
> 任何文件修改前必须强制严谨思考。
> 遇到报错或异常时严禁盲目测试，必须分析问题根本原因。
</ADDITIONAL_INSTRUCTIONS>`;

function hasActiveIssues(projectDir) {
  if (!existsSync(DB_PATH)) return true;
  try {
    const result = execSync(
      `sqlite3 "${DB_PATH}" "SELECT COUNT(*) FROM issues WHERE project_dir='${projectDir}' AND status IN ('pending','in_progress');"`,
      { encoding: "utf-8", timeout: 5000 }
    ).trim();
    return parseInt(result, 10) > 0;
  } catch {
    return true;
  }
}

export default async ({ project }) => ({
  "experimental.chat.system.transform": async (_input, output) => {
    output.system.push(DEV_WORKFLOW_RULES);
  },
  "tool.execute.before": async ({ tool, sessionID }, output) => {
    if (tool !== "Edit" && tool !== "Write" && tool !== "edit" && tool !== "write") return;
    const projectDir = project?.path || process.cwd();
    if (!hasActiveIssues(projectDir)) {
      output.args = {
        ...output.args,
        __blocked: "当前项目没有活跃的 track issue。请先调用 track(action: create) 记录问题后再修改代码。",
      };
    }
  },
});
"""


def _copy_check_track_script(target_dir: Path) -> tuple[Path, bool]:
    """复制 check_track.sh 到目标目录，返回 (目标路径, 是否有变更)"""
    import shutil
    target_dir.mkdir(parents=True, exist_ok=True)
    src = _check_track_script_path()
    dst = target_dir / "check_track.sh"
    if not dst.exists() or dst.read_text("utf-8") != src.read_text("utf-8"):
        shutil.copy2(src, dst)
        dst.chmod(0o755)
        return dst, True
    return dst, False


def _build_claude_code_hooks(script_path: str) -> dict:
    """构建 Claude Code hooks 配置，填充实际脚本路径"""
    import copy
    cfg = copy.deepcopy(CLAUDE_CODE_HOOKS_CONFIG)
    cfg["hooks"]["PreToolUse"][0]["hooks"][0]["command"] = script_path
    return cfg


def _write_claude_code_hooks(hooks_dir: Path) -> list[str]:
    """写入 Claude Code hooks 到 .claude/settings.json"""
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    # 复制检查脚本
    script_dir = hooks_dir / "hooks"
    script_path, script_changed = _copy_check_track_script(script_dir)
    results.append(f"{'✓ 已同步' if script_changed else '- 无变更'}  Script: .claude/hooks/check_track.sh")
    # 构建配置
    new_hooks_cfg = _build_claude_code_hooks(str(script_path))
    filepath = hooks_dir / "settings.json"
    config = {}
    if filepath.exists():
        try:
            config = json.loads(filepath.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            config = {}
    existing = config.get("hooks", {})
    new_hooks = new_hooks_cfg["hooks"]
    changed = existing.get("PreToolUse") != new_hooks.get("PreToolUse")
    # 清理旧的 Stop hook（如果存在）
    has_old_stop = "Stop" in existing
    if changed or has_old_stop:
        config.setdefault("hooks", {})
        config["hooks"]["PreToolUse"] = new_hooks["PreToolUse"]
        config["hooks"].pop("Stop", None)
        filepath.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        results.append("✓ 已生成  Hook: .claude/settings.json (PreToolUse)")
    else:
        results.append("- 无变更  Hook: .claude/settings.json (PreToolUse)")
    return results


def _write_cursor_hooks(hooks_dir: Path) -> list[str]:
    """写入 Cursor hooks 到 .cursor/hooks.json"""
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    filepath = hooks_dir / "hooks.json"
    new_content = json.dumps(CURSOR_HOOKS_CONFIG, indent=2, ensure_ascii=False) + "\n"
    if filepath.exists():
        existing = filepath.read_text("utf-8")
        if existing.strip() == new_content.strip():
            results.append("- 无变更  Hook: .cursor/hooks.json")
            return results
    filepath.write_text(new_content, encoding="utf-8")
    results.append("✓ 已生成  Hook: .cursor/hooks.json")
    return results


def _write_windsurf_hooks(hooks_dir: Path) -> list[str]:
    """写入 Windsurf hooks 到 .windsurf/hooks.json"""
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    filepath = hooks_dir / "hooks.json"
    new_content = json.dumps(WINDSURF_HOOKS_CONFIG, indent=2, ensure_ascii=False) + "\n"
    if filepath.exists():
        existing = filepath.read_text("utf-8")
        if existing.strip() == new_content.strip():
            results.append("- 无变更  Hook: .windsurf/hooks.json")
            return results
    filepath.write_text(new_content, encoding="utf-8")
    results.append("✓ 已生成  Hook: .windsurf/hooks.json")
    return results


def _write_opencode_plugins(plugins_dir: Path) -> list[str]:
    """写入 OpenCode 插件文件，返回变更列表"""
    results = []
    plugins_dir.mkdir(parents=True, exist_ok=True)
    # 确保 .opencode/package.json 包含 "type": "module"（ESM 插件必需）
    opencode_dir = plugins_dir.parent
    pkg_path = opencode_dir / "package.json"
    pkg_expected = {"type": "module", "dependencies": {"@opencode-ai/plugin": "1.2.10"}}
    pkg_changed = False
    if pkg_path.exists():
        try:
            pkg = json.loads(pkg_path.read_text("utf-8"))
            if pkg.get("type") != "module":
                pkg["type"] = "module"
                pkg_changed = True
            if pkg.get("dependencies", {}).get("@opencode-ai/plugin") != "1.2.10":
                pkg.setdefault("dependencies", {})["@opencode-ai/plugin"] = "1.2.10"
                pkg_changed = True
            if pkg_changed:
                pkg_path.write_text(json.dumps(pkg, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        except (json.JSONDecodeError, OSError):
            pkg_path.write_text(json.dumps(pkg_expected, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            pkg_changed = True
    else:
        pkg_path.write_text(json.dumps(pkg_expected, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        pkg_changed = True
    results.append(f"{'✓ 已更新' if pkg_changed else '- 无变更'}  package.json (type: module)")
    # 写入插件文件
    filepath = plugins_dir / "aivectormemory-pre-tool-check.js"
    if filepath.exists() and filepath.read_text("utf-8").strip() == OPENCODE_PLUGIN_CONTENT.strip():
        results.append("- 无变更  Plugin: aivectormemory-pre-tool-check.js")
    else:
        filepath.write_text(OPENCODE_PLUGIN_CONTENT, encoding="utf-8")
        results.append("✓ 已生成  Plugin: aivectormemory-pre-tool-check.js")
    return results


def _write_hooks(hooks_dir: Path) -> list[str]:
    """写入 hook 文件到指定目录（Kiro），返回变更列表"""
    import copy
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    # 复制 check_track.sh 到 hooks 目录
    script_path, script_changed = _copy_check_track_script(hooks_dir)
    results.append(f"{'✓ 已同步' if script_changed else '- 无变更'}  Script: check_track.sh")
    for hook in HOOKS_CONFIGS:
        content = copy.deepcopy(hook["content"])
        # 动态填充 pre-tool-use-check 的 command 路径
        if hook["filename"] == "pre-tool-use-check.kiro.hook":
            content["then"]["command"] = str(script_path)
        filepath = hooks_dir / hook["filename"]
        new_content = json.dumps(content, indent=2, ensure_ascii=False) + "\n"
        if filepath.exists():
            existing = filepath.read_text("utf-8")
            if existing.strip() == new_content.strip():
                results.append(f"- 无变更  Hook: {hook['filename']}")
                continue
        filepath.write_text(new_content, encoding="utf-8")
        results.append(f"✓ 已生成  Hook: {hook['filename']}")
    return results


TASKS_PATH_MAP = {
    "Kiro": ".kiro/specs/{feature}/tasks.md",
    "Cursor": ".cursor/tasks.md",
    "Windsurf": ".windsurf/tasks.md",
    "Trae": ".trae/tasks.md",
}
TASKS_PATH_DEFAULT = "tasks.md（项目根目录）"

SPECS_PATH_MAP = {
    "Kiro": ".kiro/specs/{feature_id}/",
    "Cursor": ".cursor/specs/{feature_id}/",
    "Windsurf": ".windsurf/specs/{feature_id}/",
    "Trae": ".trae/specs/{feature_id}/",
}
SPECS_PATH_DEFAULT = "docs/specs/{feature_id}/（项目根目录）"


def _write_steering(filepath: Path, mode: str, ide_name: str = "") -> bool:
    raw = STEERING_CONTENT.strip()
    tasks_path = TASKS_PATH_MAP.get(ide_name, TASKS_PATH_DEFAULT)
    specs_path = SPECS_PATH_MAP.get(ide_name, SPECS_PATH_DEFAULT)
    content = raw.replace("{tasks_path}", tasks_path).replace("{specs_path}", specs_path)
    if mode == "file":
        final = content + "\n"
        if filepath.exists() and filepath.read_text("utf-8").strip() == content:
            return False
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(final, encoding="utf-8")
        return True
    if mode == "append":
        existing = filepath.read_text("utf-8") if filepath.exists() else ""
        block = f"\n{STEERING_MARKER}\n{content}\n"
        if STEERING_MARKER in existing:
            start = existing.index(STEERING_MARKER)
            next_marker = existing.find("\n<!-- ", start + len(STEERING_MARKER))
            end = next_marker if next_marker != -1 else len(existing)
            old_block = existing[start:end]
            new_block = f"{STEERING_MARKER}\n{content}\n"
            if old_block.strip() == new_block.strip():
                return False
            updated = existing[:start] + new_block + existing[end:]
            filepath.write_text(updated, encoding="utf-8")
            return True
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(block)
        return True
    return False


def _claude_desktop_path() -> Path | None:
    s = platform.system()
    if s == "Darwin":
        return Path.home() / "Library/Application Support/Claude/claude_desktop_config.json"
    if s == "Windows":
        import os
        return Path(os.environ.get("APPDATA", "")) / "Claude/claude_desktop_config.json"
    if s == "Linux":
        return Path.home() / ".config/Claude/claude_desktop_config.json"
    return None


def _build_config(cmd: str, args: list[str], fmt: str) -> dict:
    env_block = {}
    hf_endpoint = os.environ.get("HF_ENDPOINT", "")
    if hf_endpoint:
        env_block["HF_ENDPOINT"] = hf_endpoint
    if fmt == "opencode":
        cfg = {"type": "local", "command": [cmd] + args, "enabled": True}
        if env_block:
            cfg["env"] = env_block
        return cfg
    cfg = {
        "command": cmd,
        "args": args,
        "disabled": False,
        "autoApprove": ["remember", "recall", "forget", "status", "track", "task", "readme", "auto_save"],
    }
    if env_block:
        cfg["env"] = env_block
    return cfg


def _merge_config(filepath: Path, key: str, server_name: str, server_config: dict) -> bool:
    config = {}
    if filepath.exists():
        try:
            config = json.loads(filepath.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            config = {}
    config.setdefault(key, {})
    if server_name in config[key] and config[key][server_name] == server_config:
        return False
    config[key][server_name] = server_config
    old_key = "devmemory"
    if old_key in config[key] and old_key != server_name:
        del config[key][old_key]
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return True


def _choose(prompt: str, options: list[tuple], allow_all: bool = False) -> list | None:
    for i, (label, *_) in enumerate(options, 1):
        print(f"  {i}. {label}")
    if allow_all:
        print(f"  a. 全部安装")
    print()
    choice = input(f"{prompt}: ").strip().lower()
    if not choice:
        return None
    if allow_all and choice == "a":
        return list(range(len(options)))
    nums = {int(p.strip()) - 1 for p in choice.split(",") if p.strip().isdigit()}
    selected = [i for i in nums if 0 <= i < len(options)]
    return selected or None


def run_install(project_dir: str | None = None):
    pdir = str(Path(project_dir or ".").resolve()).replace("\\", "/")
    print(f"项目目录: {pdir}\n")

    # 1. 选择启动方式
    print("启动方式：")
    runner_indices = _choose("选择启动方式 [1]", RUNNERS)
    if runner_indices is None:
        runner_indices = [0]  # 默认 pip/pipx
    cmd, args = RUNNERS[runner_indices[0]][1](pdir)
    print(f"  → {cmd} {' '.join(args)}\n")

    # 2. 选择 IDE
    print("支持的 IDE：")
    valid_ides = []
    for name, path_fn, fmt, is_global, steering_fn, steering_mode, hooks_fn in IDES:
        p = path_fn(Path(pdir))
        if p is None:
            continue
        tag = " (全局)" if is_global else ""
        valid_ides.append((f"{name}{tag}", name, path_fn, fmt, steering_fn, steering_mode, hooks_fn))

    ide_indices = _choose("选择 IDE（编号，逗号分隔多选，a=全部）", valid_ides, allow_all=True)
    if ide_indices is None:
        print("未选择，退出")
        return

    # 3. 写入配置
    print()
    root = Path(pdir)
    for idx in ide_indices:
        label, ide_name, path_fn, fmt, steering_fn, steering_mode, hooks_fn = valid_ides[idx]
        filepath = path_fn(root)
        if filepath is None:
            continue
        server_config = _build_config(cmd, args, fmt)
        key = "mcp" if fmt == "opencode" else "mcpServers"
        changed = _merge_config(filepath, key, "aivectormemory", server_config)
        status = "✓ 已更新" if changed else "- 无变更"
        print(f"  {status}  {label} MCP 配置")

        # 4. 写入 Steering 规则
        if steering_fn and steering_mode:
            steering_path = steering_fn(root)
            s_changed = _write_steering(steering_path, steering_mode, ide_name)
            s_status = "✓ 已生成" if s_changed else "- 无变更"
            print(f"  {s_status}  {label} Steering 规则 → {steering_path.relative_to(root)}")

        # 5. 写入 Hooks / Plugins
        if hooks_fn:
            hooks_dir = hooks_fn(root)
            hooks_dir_str = str(hooks_dir)
            if hooks_dir_str.endswith(".opencode/plugins"):
                hook_results = _write_opencode_plugins(hooks_dir)
            elif hooks_dir_str.endswith(".claude"):
                hook_results = _write_claude_code_hooks(hooks_dir)
            elif hooks_dir_str.endswith(".cursor"):
                hook_results = _write_cursor_hooks(hooks_dir)
            elif hooks_dir_str.endswith(".windsurf"):
                hook_results = _write_windsurf_hooks(hooks_dir)
            else:
                hook_results = _write_hooks(hooks_dir)
            for r in hook_results:
                print(f"  {r}")

    print("\n安装完成，重启 IDE 即可使用")

    # 6. 注册项目到数据库（Web 看板可见）
    try:
        from aivectormemory.db.connection import ConnectionManager
        from aivectormemory.db.schema import init_db
        cm = ConnectionManager(pdir)
        init_db(cm.conn)
        from datetime import datetime
        now = datetime.now().astimezone().isoformat()
        cm.conn.execute(
            "INSERT OR IGNORE INTO session_state (project_dir, is_blocked, block_reason, next_step, current_task, progress, recent_changes, pending, updated_at) VALUES (?,0,'','','','[]','[]','[]',?)",
            (pdir, now)
        )
        cm.conn.commit()
        cm.close()
    except Exception as e:
        print(f"\n⚠️  项目注册到数据库失败（Web 看板可能不显示此项目）: {e}")
