"""aivectormemory regenerate - 切换语言并重新生成所有已注册项目的规则文件"""
import json
import sqlite3
from pathlib import Path

from aivectormemory.settings import set_language, SUPPORTED_LANGS
from aivectormemory.install import (
    IDES, STEERING_MARKER, PER_MSG_INJECTION_IDES, SPECS_PATH_MAP, SPECS_PATH_DEFAULT,
    _write_steering,
    _write_hooks, _write_claude_code_hooks, _write_cursor_hooks,
    _write_windsurf_hooks, _write_opencode_plugins,
)

DB_PATH = Path.home() / ".aivectormemory" / "memory.db"


def _get_registered_projects() -> list[str]:
    """从数据库读取所有已注册项目目录"""
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute("SELECT DISTINCT project_dir FROM session_state").fetchall()
    conn.close()
    return [r[0] for r in rows]


def _detect_installed_ides(root: Path) -> list[tuple]:
    """检测项目中已安装的 IDE 配置，返回匹配的 IDE 配置列表"""
    installed = []
    for name, path_fn, fmt, is_global, steering_fn, steering_mode, hooks_fn in IDES:
        mcp_path = path_fn(root)
        if mcp_path and mcp_path.exists():
            try:
                config = json.loads(mcp_path.read_text("utf-8"))
                key = "mcp" if fmt == "opencode" else "mcpServers"
                if "aivectormemory" in config.get(key, {}):
                    installed.append((name, steering_fn, steering_mode, hooks_fn))
            except (json.JSONDecodeError, OSError):
                pass
    return installed


def regenerate_project(project_dir: str, lang: str) -> list[str]:
    """重新生成单个项目的规则文件，返回变更日志"""
    root = Path(project_dir)
    if not root.exists():
        return [f"⚠ 跳过（目录不存在）: {project_dir}"]

    results = []
    installed = _detect_installed_ides(root)
    if not installed:
        return [f"⚠ 跳过（无已安装 IDE）: {project_dir}"]

    for ide_name, steering_fn, steering_mode, hooks_fn in installed:
        # 重新生成 Steering 规则
        if steering_fn and steering_mode:
            steering_path = steering_fn(root)
            include_workflow = ide_name not in PER_MSG_INJECTION_IDES
            changed = _write_steering(steering_path, steering_mode, ide_name, include_workflow, lang=lang)
            status = "✓ 已更新" if changed else "- 无变更"
            results.append(f"  {status}  {ide_name} Steering → {steering_path.relative_to(root)}")

        # 重新生成 Hooks / Plugins
        if hooks_fn:
            hooks_dir = hooks_fn(root)
            hooks_dir_str = str(hooks_dir)
            if hooks_dir_str.endswith(".opencode/plugins"):
                hook_results = _write_opencode_plugins(hooks_dir, lang=lang)
            elif hooks_dir_str.endswith(".claude"):
                hook_results = _write_claude_code_hooks(hooks_dir, lang=lang)
            elif hooks_dir_str.endswith(".cursor"):
                hook_results = _write_cursor_hooks(hooks_dir)
            elif hooks_dir_str.endswith(".windsurf"):
                hook_results = _write_windsurf_hooks(hooks_dir)
            else:
                hook_results = _write_hooks(hooks_dir, lang=lang)
            results.extend(f"  {r}" for r in hook_results)

    return results


def run_regenerate(lang: str):
    """CLI 入口：切换语言并重新生成所有项目的规则文件"""
    if lang not in SUPPORTED_LANGS:
        print(f"❌ 不支持的语言: {lang}")
        print(f"   支持: {', '.join(SUPPORTED_LANGS)}")
        return

    # 1. 写入全局语言设置
    set_language(lang)
    print(f"✓ 全局语言已切换为: {lang}\n")

    # 2. 遍历所有已注册项目
    projects = _get_registered_projects()
    if not projects:
        print("未找到已注册项目")
        return

    print(f"已注册项目: {len(projects)} 个\n")
    for pdir in projects:
        print(f"📁 {pdir}")
        results = regenerate_project(pdir, lang)
        for r in results:
            print(r)
        print()

    print("✓ 全部完成")
