import json
from urllib.parse import urlparse, parse_qs, unquote

from aivectormemory.db.state_repo import StateRepo
from aivectormemory.db.issue_repo import IssueRepo
from aivectormemory.web.routes import memories, issues, tasks, tags, projects, auth, maintenance


def _resolve_project(cm, params):
    """如果 URL 带 ?project=xxx 则覆盖 cm.project_dir，返回临时 project_dir"""
    override = params.get("project", [None])[0]
    return override if override is not None else cm.project_dir


def handle_api_request(handler, cm):
    parsed = urlparse(handler.path)
    path = parsed.path
    params = parse_qs(parsed.query)
    pdir = _resolve_project(cm, params)
    method = handler.command

    # --- Auth 路由（无需认证） ---
    auth_routes = {
        "/api/auth/register": lambda: auth.register(handler, cm, _read_body),
        "/api/auth/login": lambda: auth.login(handler, cm, _read_body),
        "/api/auth/logout": lambda: auth.logout(handler, _read_body),
        "/api/auth/change-password": lambda: auth.change_password(handler, cm, _read_body),
    }
    if method == "POST" and path in auth_routes:
        return _json_response(handler, auth_routes[path]())
    if method == "GET" and path == "/api/auth/me":
        return _json_response(handler, auth.get_current_user(params))

    # --- 资源 ID 路由 ---
    if path.startswith("/api/memories/") and len(path.split("/")) == 4:
        mid = path.split("/")[3]
        if method == "GET":
            return _json_response(handler, memories.get_memory_detail(cm, mid, pdir))
        elif method == "PUT":
            return _json_response(handler, memories.put_memory(handler, cm, mid, pdir))
        elif method == "DELETE":
            return _json_response(handler, memories.delete_memory(cm, mid, pdir))

    if path.startswith("/api/projects/") and method == "DELETE":
        proj_dir = unquote("/".join(path.split("/")[3:]))
        return _json_response(handler, projects.delete_project(cm, proj_dir))

    if path.startswith("/api/issues/") and len(path.split("/")) == 4:
        inum = int(path.split("/")[3])
        repo = IssueRepo(cm.conn, pdir)
        row = repo.get_by_number(inum)
        is_archived = row is None
        if is_archived:
            row = repo.get_archived_by_number(inum)
        if not row:
            return _json_response(handler, {"error": "not found"}, 404)
        iid = row["id"]
        if method == "GET":
            return _json_response(handler, dict(row))
        elif method == "PUT":
            return _json_response(handler, issues.put_issue(handler, cm, iid, pdir))
        elif method == "DELETE":
            return _json_response(handler, issues.delete_issue(handler, cm, iid, pdir, params, is_archived=is_archived))

    if path.startswith("/api/tasks/") and len(path.split("/")) == 4:
        seg = path.split("/")[3]
        if seg == "archived" and method == "GET":
            return _json_response(handler, tasks.get_archived_tasks(cm, params, pdir))
        tid = int(seg)
        if method == "PUT":
            return _json_response(handler, tasks.put_task(handler, cm, tid, pdir))
        elif method == "DELETE":
            return _json_response(handler, tasks.delete_task(cm, tid, pdir))

    if path == "/api/tasks" and method == "DELETE":
        return _json_response(handler, tasks.delete_tasks_by_feature(handler, cm, pdir, params))

    # --- 集合路由 ---
    route_map = {
        "GET": {
            "/api/memories": lambda: memories.get_memories(cm, params, pdir),
            "/api/status": lambda: _get_status(cm, pdir),
            "/api/issues": lambda: issues.get_issues(cm, params, pdir),
            "/api/tasks": lambda: tasks.get_tasks(cm, params, pdir),
            "/api/stats": lambda: projects.get_stats(cm, pdir),
            "/api/tags": lambda: tags.get_tags(cm, params, pdir),
            "/api/projects": lambda: projects.get_projects(cm),
            "/api/export": lambda: memories.export_memories(cm, params, pdir),
            "/api/browse": lambda: projects.browse_directory(params),
            "/api/maintenance/health": lambda: maintenance.health_check(cm),
            "/api/maintenance/stats": lambda: maintenance.db_stats(cm),
            "/api/maintenance/backups": lambda: maintenance.list_backups(cm),
            "/api/settings/language": lambda: _get_language(),
        },
        "POST": {
            "/api/import": lambda: memories.import_memories(handler, cm, pdir),
            "/api/search": lambda: memories.search_memories(handler, cm, pdir),
            "/api/projects": lambda: projects.add_project(handler, cm),
            "/api/issues": lambda: issues.post_issue(handler, cm, pdir),
            "/api/tasks": lambda: tasks.post_tasks(handler, cm, pdir),
            "/api/settings/language": lambda: _set_language(handler),
            "/api/maintenance/repair": lambda: maintenance.repair_missing(cm),
            "/api/maintenance/backup": lambda: maintenance.backup_db(cm),
        },
        "PUT": {
            "/api/status": lambda: _put_status(handler, cm, pdir),
            "/api/tags/rename": lambda: tags.rename_tag(handler, cm, pdir),
            "/api/tags/merge": lambda: tags.merge_tags(handler, cm, pdir),
        },
        "DELETE": {
            "/api/memories": lambda: memories.delete_memories_batch(handler, cm, pdir),
            "/api/tags/delete": lambda: tags.delete_tags(handler, cm, pdir),
        },
    }

    route_fn = route_map.get(method, {}).get(path)
    if route_fn:
        _json_response(handler, route_fn())
    else:
        handler.send_error(404, "API not found")


# --- 公共工具 ---

def _read_body(handler) -> dict:
    try:
        length = int(handler.headers.get("Content-Length", 0))
    except (ValueError, TypeError):
        return {}
    if length <= 0:
        return {}
    if length > 10 * 1024 * 1024:
        return {}
    try:
        return json.loads(handler.rfile.read(length))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _json_response(handler, data, status=200):
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", len(body))
    handler.end_headers()
    handler.wfile.write(body)


# --- Status (保留在 api.py，太小不值得拆分) ---

def _get_status(cm, pdir):
    repo = StateRepo(cm.conn, pdir)
    state = repo.get()
    return state or {"empty": True}


def _put_status(handler, cm, pdir):
    body = _read_body(handler)
    repo = StateRepo(cm.conn, pdir)
    return repo.upsert(**body)

# --- Language Settings ---

def _get_language():
    from aivectormemory.settings import get_language
    return {"language": get_language()}


def _set_language(handler):
    body = _read_body(handler)
    lang = body.get("language", "")
    if not lang:
        return {"error": "language is required"}
    from aivectormemory.settings import set_language, SUPPORTED_LANGS
    if lang not in SUPPORTED_LANGS:
        return {"error": f"Unsupported language: {lang}. Supported: {', '.join(SUPPORTED_LANGS)}"}
    set_language(lang)
    from aivectormemory.regenerate import run_regenerate
    run_regenerate(lang)
    return {"success": True, "language": lang}
