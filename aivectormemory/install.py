"""aivectormemory install - 交互式为当前项目配置 MCP + Steering 规则"""
import json
import os
import sys
from pathlib import Path
from aivectormemory.i18n import get_steering, get_workflow_prompt

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
     lambda root: root / ".github/copilot-instructions.md", "append",
     lambda root: root / ".claude"),
    ("Trae",           lambda root: root / ".trae/mcp.json",           "standard", False,
     lambda root: root / ".trae/rules/aivectormemory.md", "file", None),
    ("OpenCode",       lambda root: root / "opencode.json",            "opencode", False,
     lambda root: root / "AGENTS.md", "append",
     lambda root: root / ".opencode/plugins"),
]

RUNNERS = [
    ("python -m aivectormemory（pip/pipx 安装）", lambda pdir: (sys.executable, ["-m", "aivectormemory", "--project-dir", pdir])),
    ("uvx aivectormemory（无需安装）", lambda pdir: ("uvx", ["aivectormemory@latest", "--project-dir", pdir])),
]


STEERING_MARKER = "<!-- aivectormemory-steering -->"


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
                "prompt": "",  # 占位，_write_hooks 时动态填充
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
                        "command": "",  # 占位，install 时动态填充 check_track.sh 路径
                        "timeout": 10,
                    }
                ]
            }
        ],
        "UserPromptSubmit": [
            {
                "hooks": [
                    {
                        "type": "command",
                        "command": "",  # 占位，install 时动态填充 inject-workflow-rules.sh 路径
                    }
                ]
            }
        ]
    }
}

CURSOR_HOOKS_CONFIG = {
    "version": 1,
    "hooks": {
        "preToolUse": [
            {
                "matcher": "edit|write",
                "command": "",  # 占位，install 时动态填充实际路径
                "timeout": 10,
            }
        ]
    }
}

WINDSURF_HOOKS_CONFIG = {
    "hooks": {
        "pre_write_code": [
            {
                "command": "",  # 占位，install 时动态填充实际路径
                "show_output": True,
            }
        ]
    }
}


OPENCODE_PLUGIN_TEMPLATE = """\
// AIVectorMemory plugin for OpenCode (@opencode-ai/plugin)
// - experimental.chat.system.transform: 注入开发规则到 system prompt
// - tool.execute.before: 检查 Edit/Write 前是否有活跃 track issue
import { execSync } from "child_process";
import { homedir } from "os";
import { existsSync } from "fs";
import { join } from "path";

const DB_PATH = join(homedir(), ".aivectormemory", "memory.db");

const DEV_WORKFLOW_RULES = `<ADDITIONAL_INSTRUCTIONS>
__DEV_WORKFLOW_RULES__</ADDITIONAL_INSTRUCTIONS>`;

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


def _build_opencode_plugin_content(lang: str = None) -> str:
    """动态生成 OpenCode 插件内容，从 i18n 模块加载"""
    js_rules = get_workflow_prompt(lang).replace('`', '\\`').replace('${', '\\${')
    return OPENCODE_PLUGIN_TEMPLATE.replace('__DEV_WORKFLOW_RULES__', js_rules)


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


def _build_claude_code_hooks(check_script_path: str, inject_script_path: str) -> dict:
    """构建 Claude Code hooks 配置，填充实际脚本路径"""
    import copy
    cfg = copy.deepcopy(CLAUDE_CODE_HOOKS_CONFIG)
    cfg["hooks"]["PreToolUse"][0]["hooks"][0]["command"] = check_script_path
    cfg["hooks"]["UserPromptSubmit"][0]["hooks"][0]["command"] = inject_script_path
    return cfg


def _write_inject_workflow_script(target_dir: Path, lang: str | None = None) -> tuple[Path, bool]:
    """生成 inject-workflow-rules.sh，只包含指定语言的 workflow prompt（直接 cat 输出）"""
    from aivectormemory.settings import get_language
    if lang is None:
        lang = get_language()
    target_dir.mkdir(parents=True, exist_ok=True)
    dst = target_dir / "inject-workflow-rules.sh"
    prompt = get_workflow_prompt(lang)
    parts = ["#!/bin/bash", "cat <<'AIVECTORMEMORY_EOF'", prompt, "AIVECTORMEMORY_EOF"]
    content = "\n".join(parts) + "\n"
    if dst.exists() and dst.read_text("utf-8") == content:
        return dst, False
    dst.write_text(content, encoding="utf-8")
    dst.chmod(0o755)
    return dst, True


def _write_claude_code_hooks(hooks_dir: Path, lang: str | None = None) -> list[str]:
    """写入 Claude Code hooks 到 .claude/settings.json"""
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    script_dir = hooks_dir / "hooks"
    # 复制 check_track.sh
    check_path, check_changed = _copy_check_track_script(script_dir)
    results.append(f"{'✓ 已同步' if check_changed else '- 无变更'}  Script: .claude/hooks/check_track.sh")
    # 生成 inject-workflow-rules.sh
    inject_path, inject_changed = _write_inject_workflow_script(script_dir, lang=lang)
    results.append(f"{'✓ 已同步' if inject_changed else '- 无变更'}  Script: .claude/hooks/inject-workflow-rules.sh")
    # 构建配置
    new_hooks_cfg = _build_claude_code_hooks(str(check_path), str(inject_path))
    filepath = hooks_dir / "settings.json"
    config = {}
    if filepath.exists():
        try:
            config = json.loads(filepath.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            config = {}
    existing = config.get("hooks", {})
    new_hooks = new_hooks_cfg["hooks"]
    changed = (existing.get("PreToolUse") != new_hooks.get("PreToolUse")
               or existing.get("UserPromptSubmit") != new_hooks.get("UserPromptSubmit"))
    # 清理旧的 Stop hook
    has_old_stop = "Stop" in existing
    if changed or has_old_stop:
        config.setdefault("hooks", {})
        config["hooks"]["PreToolUse"] = new_hooks["PreToolUse"]
        config["hooks"]["UserPromptSubmit"] = new_hooks["UserPromptSubmit"]
        config["hooks"].pop("Stop", None)
        filepath.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        results.append("✓ 已生成  Hook: .claude/settings.json (PreToolUse + UserPromptSubmit)")
    else:
        results.append("- 无变更  Hook: .claude/settings.json (PreToolUse + UserPromptSubmit)")
    return results


def _build_cursor_hooks(script_path: str) -> dict:
    """构建 Cursor hooks 配置，填充实际脚本路径"""
    import copy
    cfg = copy.deepcopy(CURSOR_HOOKS_CONFIG)
    cfg["hooks"]["preToolUse"][0]["command"] = script_path
    return cfg


def _write_cursor_hooks(hooks_dir: Path) -> list[str]:
    """写入 Cursor hooks 到 .cursor/hooks.json"""
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    # 复制检查脚本
    script_dir = hooks_dir / "hooks"
    script_path, script_changed = _copy_check_track_script(script_dir)
    results.append(f"{'✓ 已同步' if script_changed else '- 无变更'}  Script: .cursor/hooks/check_track.sh")
    # 构建配置
    new_cfg = _build_cursor_hooks(str(script_path))
    filepath = hooks_dir / "hooks.json"
    config = {}
    if filepath.exists():
        try:
            config = json.loads(filepath.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            config = {}
    existing_hooks = config.get("hooks", {})
    new_hooks = new_cfg["hooks"]
    changed = existing_hooks.get("preToolUse") != new_hooks.get("preToolUse")
    # 清理旧的 beforeSubmitPrompt hook（如果存在）
    has_old_hooks = "beforeSubmitPrompt" in existing_hooks
    if changed or has_old_hooks:
        config["version"] = new_cfg["version"]
        config.setdefault("hooks", {})
        config["hooks"]["preToolUse"] = new_hooks["preToolUse"]
        config["hooks"].pop("beforeSubmitPrompt", None)
        filepath.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        results.append("✓ 已生成  Hook: .cursor/hooks.json (preToolUse)")
    else:
        results.append("- 无变更  Hook: .cursor/hooks.json (preToolUse)")
    return results


def _build_windsurf_hooks(script_path: str) -> dict:
    """构建 Windsurf hooks 配置，填充实际脚本路径"""
    import copy
    cfg = copy.deepcopy(WINDSURF_HOOKS_CONFIG)
    cfg["hooks"]["pre_write_code"][0]["command"] = script_path
    return cfg


def _write_windsurf_hooks(hooks_dir: Path) -> list[str]:
    """写入 Windsurf hooks 到 .windsurf/hooks.json"""
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    # 复制检查脚本
    script_dir = hooks_dir / "hooks"
    script_path, script_changed = _copy_check_track_script(script_dir)
    results.append(f"{'✓ 已同步' if script_changed else '- 无变更'}  Script: .windsurf/hooks/check_track.sh")
    # 构建配置
    new_cfg = _build_windsurf_hooks(str(script_path))
    filepath = hooks_dir / "hooks.json"
    config = {}
    if filepath.exists():
        try:
            config = json.loads(filepath.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            config = {}
    existing_hooks = config.get("hooks", {})
    new_hooks = new_cfg["hooks"]
    changed = existing_hooks.get("pre_write_code") != new_hooks.get("pre_write_code")
    if changed:
        config.setdefault("hooks", {})
        config["hooks"]["pre_write_code"] = new_hooks["pre_write_code"]
        filepath.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        results.append("✓ 已生成  Hook: .windsurf/hooks.json (pre_write_code)")
    else:
        results.append("- 无变更  Hook: .windsurf/hooks.json (pre_write_code)")
    return results


def _write_opencode_plugins(plugins_dir: Path, lang: str = None) -> list[str]:
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
    plugin_content = _build_opencode_plugin_content(lang)
    filepath = plugins_dir / "aivectormemory-pre-tool-check.js"
    if filepath.exists() and filepath.read_text("utf-8").strip() == plugin_content.strip():
        results.append("- 无变更  Plugin: aivectormemory-pre-tool-check.js")
    else:
        filepath.write_text(plugin_content, encoding="utf-8")
        results.append("✓ 已生成  Plugin: aivectormemory-pre-tool-check.js")
    return results


def _write_hooks(hooks_dir: Path, lang: str = None) -> list[str]:
    """写入 hook 文件到指定目录（Kiro），返回变更列表"""
    import copy
    results = []
    hooks_dir.mkdir(parents=True, exist_ok=True)
    # 复制 check_track.sh 到 hooks 目录
    script_path, script_changed = _copy_check_track_script(hooks_dir)
    results.append(f"{'✓ 已同步' if script_changed else '- 无变更'}  Script: check_track.sh")
    for hook in HOOKS_CONFIGS:
        content = copy.deepcopy(hook["content"])
        # 动态填充
        if hook["filename"] == "dev-workflow-check.kiro.hook":
            content["then"]["prompt"] = get_workflow_prompt(lang)
        elif hook["filename"] == "pre-tool-use-check.kiro.hook":
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


SPECS_PATH_MAP = {
    "Kiro": ".kiro/specs/{feature_id}/",
    "Cursor": ".cursor/specs/{feature_id}/",
    "Windsurf": ".windsurf/specs/{feature_id}/",
    "Trae": ".trae/specs/{feature_id}/",
}
SPECS_PATH_DEFAULT = "docs/specs/{feature_id}/（项目根目录）"

# 有每消息注入机制的 IDE，steering 不写入 DEV_WORKFLOW_PROMPT（避免重复）
PER_MSG_INJECTION_IDES = {"Kiro", "OpenCode", "Claude Code"}


def _write_steering(filepath: Path, mode: str, ide_name: str = "", include_workflow: bool = True, lang: str = None) -> bool:
    raw = get_steering(lang).strip()
    specs_path = SPECS_PATH_MAP.get(ide_name, SPECS_PATH_DEFAULT)
    content = raw.replace("{specs_path}", specs_path)
    if include_workflow:
        # 语言无关：在 "## 1." 章节前插入 workflow prompt
        anchor = "---\n\n## 1."
        idx = content.find(anchor)
        if idx != -1:
            workflow = get_workflow_prompt(lang).strip()
            content = content[:idx] + f"---\n\n{workflow}\n\n---\n\n## 1." + content[idx + len(anchor):]
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

    # 2. 选择语言
    from aivectormemory.settings import SUPPORTED_LANGS, get_language, set_language
    lang_labels = {
        "zh-CN": "简体中文", "zh-TW": "繁體中文", "en": "English",
        "es": "Español", "de": "Deutsch", "fr": "Français", "ja": "日本語",
    }
    current_lang = get_language()
    lang_options = [(f"{lang_labels.get(l, l)}{' (当前)' if l == current_lang else ''}", l) for l in SUPPORTED_LANGS]
    print("AI 规则语言：")
    lang_indices = _choose("选择语言 [当前]", lang_options)
    if lang_indices is not None:
        selected_lang = lang_options[lang_indices[0]][1]
    else:
        selected_lang = current_lang
    if selected_lang != current_lang:
        set_language(selected_lang)
    print(f"  → {lang_labels.get(selected_lang, selected_lang)}\n")

    # 3. 选择 IDE
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
            include_workflow = ide_name not in PER_MSG_INJECTION_IDES
            s_changed = _write_steering(steering_path, steering_mode, ide_name, include_workflow, lang=selected_lang)
            s_status = "✓ 已生成" if s_changed else "- 无变更"
            print(f"  {s_status}  {label} Steering 规则 → {steering_path.relative_to(root)}")

        # 5. 写入 Hooks / Plugins
        if hooks_fn:
            hooks_dir = hooks_fn(root)
            hooks_dir_str = str(hooks_dir)
            if hooks_dir_str.endswith(".opencode/plugins"):
                hook_results = _write_opencode_plugins(hooks_dir, lang=selected_lang)
            elif hooks_dir_str.endswith(".claude"):
                hook_results = _write_claude_code_hooks(hooks_dir)
            elif hooks_dir_str.endswith(".cursor"):
                hook_results = _write_cursor_hooks(hooks_dir)
            elif hooks_dir_str.endswith(".windsurf"):
                hook_results = _write_windsurf_hooks(hooks_dir)
            else:
                hook_results = _write_hooks(hooks_dir, lang=selected_lang)
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
