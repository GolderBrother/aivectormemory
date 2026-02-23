import json
from urllib.parse import urlparse, parse_qs
from aivectormemory.db.memory_repo import MemoryRepo
from aivectormemory.db.user_memory_repo import UserMemoryRepo
from aivectormemory.db.state_repo import StateRepo
from aivectormemory.db.issue_repo import IssueRepo
from aivectormemory.db.task_repo import TaskRepo


def _resolve_project(cm, params):
    """如果 URL 带 ?project=xxx 则覆盖 cm.project_dir，返回临时 project_dir"""
    override = params.get("project", [None])[0]
    return override if override is not None else cm.project_dir


def handle_api_request(handler, cm):
    parsed = urlparse(handler.path)
    path = parsed.path
    params = parse_qs(parsed.query)
    pdir = _resolve_project(cm, params)

    routes = {
        "GET": {
            "/api/memories": lambda: get_memories(cm, params, pdir),
            "/api/status": lambda: get_status(cm, pdir),
            "/api/issues": lambda: get_issues(cm, params, pdir),
            "/api/tasks": lambda: get_tasks(cm, params, pdir),
            "/api/stats": lambda: get_stats(cm, pdir),
            "/api/tags": lambda: get_tags(cm, params, pdir),
            "/api/projects": lambda: get_projects(cm),
            "/api/export": lambda: export_memories(cm, params, pdir),
            "/api/browse": lambda: browse_directory(params),
        },
        "POST": {
            "/api/import": lambda: import_memories(handler, cm, pdir),
            "/api/search": lambda: search_memories(handler, cm, pdir),
            "/api/projects": lambda: add_project(handler, cm),
            "/api/issues": lambda: post_issue(handler, cm, pdir),
            "/api/tasks": lambda: post_tasks(handler, cm, pdir),
        },
        "PUT": {
            "/api/status": lambda: put_status(handler, cm, pdir),
            "/api/tags/rename": lambda: rename_tag(handler, cm, pdir),
            "/api/tags/merge": lambda: merge_tags(handler, cm, pdir),
        },
        "DELETE": {
            "/api/memories": lambda: delete_memories_batch(handler, cm, pdir),
            "/api/tags/delete": lambda: delete_tags(handler, cm, pdir),
        },
    }

    method = handler.command
    if path.startswith("/api/memories/") and len(path.split("/")) == 4:
        mid = path.split("/")[3]
        if method == "GET":
            return _json_response(handler, get_memory_detail(cm, mid, pdir))
        elif method == "PUT":
            return _json_response(handler, put_memory(handler, cm, mid, pdir))
        elif method == "DELETE":
            return _json_response(handler, delete_memory(cm, mid, pdir))

    if path.startswith("/api/projects/") and method == "DELETE":
        proj_dir = "/".join(path.split("/")[3:])
        from urllib.parse import unquote
        proj_dir = unquote(proj_dir)
        return _json_response(handler, delete_project(cm, proj_dir))

    if path.startswith("/api/issues/") and len(path.split("/")) == 4:
        iid = int(path.split("/")[3])
        if method == "PUT":
            return _json_response(handler, put_issue(handler, cm, iid, pdir))
        elif method == "DELETE":
            return _json_response(handler, delete_issue(handler, cm, iid, pdir, params))

    if path.startswith("/api/tasks/") and len(path.split("/")) == 4:
        tid = int(path.split("/")[3])
        if method == "PUT":
            return _json_response(handler, put_task(handler, cm, tid, pdir))
        elif method == "DELETE":
            return _json_response(handler, delete_task(cm, tid, pdir))

    if path == "/api/tasks" and method == "DELETE":
        return _json_response(handler, delete_tasks_by_feature(handler, cm, pdir, params))

    route_fn = routes.get(method, {}).get(path)
    if route_fn:
        _json_response(handler, route_fn())
    else:
        handler.send_error(404, "API not found")


def _read_body(handler) -> dict:
    try:
        length = int(handler.headers.get("Content-Length", 0))
    except (ValueError, TypeError):
        return {}
    if length <= 0:
        return {}
    if length > 10 * 1024 * 1024:  # 10MB 上限
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


def get_memories(cm, params, pdir):
    scope = params.get("scope", ["all"])[0]
    query = params.get("query", [None])[0]
    tag = params.get("tag", [None])[0]
    source = params.get("source", [None])[0]
    exclude_tags = params.get("exclude_tags", [None])[0]
    limit = int(params.get("limit", [100])[0])
    offset = int(params.get("offset", [0])[0])

    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)

    if tag:
        if scope == "user":
            all_rows = user_repo.list_by_tags([tag], limit=9999, source=source)
        elif scope == "project":
            all_rows = repo.list_by_tags([tag], scope="project", project_dir=pdir, limit=9999, source=source)
        else:
            proj_rows = repo.list_by_tags([tag], scope="project", project_dir=pdir, limit=9999, source=source)
            user_rows = user_repo.list_by_tags([tag], limit=9999, source=source)
            all_rows = proj_rows + user_rows
        if query:
            q = query.lower()
            all_rows = [r for r in all_rows if q in r.get("content", "").lower()]
        total = len(all_rows)
        results = all_rows[offset:offset + limit]
    elif exclude_tags:
        ex_set = set(exclude_tags.split(","))
        if scope == "user":
            all_rows = user_repo.get_all(limit=9999)
        elif scope == "project":
            all_rows = repo.get_all(limit=9999, offset=0, project_dir=pdir)
        else:
            all_rows = repo.get_all(limit=9999, offset=0) + user_repo.get_all(limit=9999)
        all_rows = [r for r in all_rows if not ex_set.intersection(json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or []))]
        if source:
            all_rows = [r for r in all_rows if r.get("source", "manual") == source]
        if query:
            q = query.lower()
            all_rows = [r for r in all_rows if q in r.get("content", "").lower()]
        total = len(all_rows)
        results = all_rows[offset:offset + limit]
    else:
        if scope == "user":
            rows = user_repo.get_all(limit=limit, offset=offset)
            total = user_repo.count()
        elif scope == "project":
            rows = repo.get_all(limit=limit, offset=offset, project_dir=pdir)
            total = repo.count(project_dir=pdir)
        else:
            rows = repo.get_all(limit=limit, offset=offset)
            total = repo.count() + user_repo.count()
            if len(rows) < limit:
                user_rows = user_repo.get_all(limit=limit - len(rows))
                rows = rows + user_rows
        if source:
            rows = [r for r in rows if r.get("source", "manual") == source]
        results = [r for r in rows if not query or query.lower() in r.get("content", "").lower()] if query else rows

    return {"memories": results, "total": total}


def get_memory_detail(cm, mid, pdir):
    repo = MemoryRepo(cm.conn, pdir)
    mem = repo.get_by_id(mid)
    if mem:
        return mem
    user_repo = UserMemoryRepo(cm.conn)
    mem = user_repo.get_by_id(mid)
    return mem or {"error": "not found"}


def put_memory(handler, cm, mid, pdir):
    body = _read_body(handler)
    repo = MemoryRepo(cm.conn, pdir)
    mem = repo.get_by_id(mid)
    table = "memories"
    if not mem:
        user_repo = UserMemoryRepo(cm.conn)
        mem = user_repo.get_by_id(mid)
        table = "user_memories"
    if not mem:
        return {"error": "not found"}
    now = repo._now()
    updates = {}
    if "content" in body:
        updates["content"] = body["content"]
    if "tags" in body:
        updates["tags"] = json.dumps(body["tags"])
    if updates:
        updates["updated_at"] = now
        set_clause = ",".join(f"{k}=?" for k in updates)
        cm.conn.execute(f"UPDATE {table} SET {set_clause} WHERE id=?", [*updates.values(), mid])
        cm.conn.commit()
    if table == "user_memories":
        return UserMemoryRepo(cm.conn).get_by_id(mid)
    return repo.get_by_id(mid)


def delete_memory(cm, mid, pdir):
    repo = MemoryRepo(cm.conn, pdir)
    if repo.delete(mid):
        return {"deleted": True, "id": mid}
    user_repo = UserMemoryRepo(cm.conn)
    if user_repo.delete(mid):
        return {"deleted": True, "id": mid}
    return {"error": "not found"}


def delete_memories_batch(handler, cm, pdir):
    body = _read_body(handler)
    ids = body.get("ids", [])
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    deleted = []
    for mid in ids:
        if repo.delete(mid):
            deleted.append(mid)
        elif user_repo.delete(mid):
            deleted.append(mid)
    return {"deleted_count": len(deleted), "ids": deleted}


def get_status(cm, pdir):
    repo = StateRepo(cm.conn, pdir)
    state = repo.get()
    return state or {"empty": True}


def put_status(handler, cm, pdir):
    body = _read_body(handler)
    repo = StateRepo(cm.conn, pdir)
    return repo.upsert(**body)


def get_issues(cm, params, pdir):
    date = params.get("date", [None])[0]
    status = params.get("status", [None])[0]
    include_archived = params.get("include_archived", ["false"])[0] == "true"
    repo = IssueRepo(cm.conn, pdir)
    if status == "archived":
        issues = repo.list_archived(date=date)
    elif status:
        issues = repo.list_by_date(date=date, status=status)
    else:
        issues = repo.list_by_date(date=date) + repo.list_archived(date=date)
    return {"issues": issues}


def put_issue(handler, cm, iid, pdir):
    body = _read_body(handler)
    repo = IssueRepo(cm.conn, pdir)
    old = repo.get_by_id(iid)
    if not old:
        return {"error": "not found"}
    fields = {k: body[k] for k in ("title", "status", "content",
              "description", "investigation", "root_cause", "solution",
              "files_changed", "test_result", "notes", "feature_id") if k in body}
    result = repo.update(iid, **fields)
    if not result:
        return {"error": "not found"}
    memory_id = result.get("memory_id", "")
    if memory_id:
        mem_repo = MemoryRepo(cm.conn, pdir)
        mem = mem_repo.get_by_id(memory_id)
        if mem:
            tags = body.get("tags", [])
            content = f"[问题追踪] #{result['issue_number']} {result['title']}\n{result.get('content', '')}"
            now = mem_repo._now()
            cm.conn.execute("UPDATE memories SET content=?, tags=?, updated_at=? WHERE id=?",
                            (content, json.dumps(tags, ensure_ascii=False), now, memory_id))
            cm.conn.commit()
    return result

def post_issue(handler, cm, pdir):
    body = _read_body(handler)
    title = body.get("title", "").strip()
    if not title:
        return {"error": "title required"}
    content = body.get("content", "")
    tags = body.get("tags", [])
    from datetime import date
    d = body.get("date", date.today().isoformat())

    repo = IssueRepo(cm.conn, pdir)
    parent_id = body.get("parent_id", 0)
    result = repo.create(d, title, content, parent_id=parent_id)
    if result.get("deduplicated"):
        return result

    mem_repo = MemoryRepo(cm.conn, pdir)
    engine = getattr(cm, "_embedding_engine", None)
    memory_id = ""
    if engine:
        mem_content = f"[问题追踪] #{result['issue_number']} {title}\n{content}"
        embedding = engine.encode(mem_content)
        session_id = getattr(cm, "session_id", 0)
        mem_result = mem_repo.insert(mem_content, tags, "project", session_id, embedding, dedup_threshold=0.99)
        memory_id = mem_result.get("id", "")
    repo.update(result["id"], memory_id=memory_id)
    result["memory_id"] = memory_id
    return result


def delete_issue(handler, cm, iid, pdir, params):
    action = params.get("action", ["delete"])[0]
    repo = IssueRepo(cm.conn, pdir)
    mem_repo = MemoryRepo(cm.conn, pdir)

    if action == "archive":
        result = repo.archive(iid)
        if not result:
            return {"error": "not found"}
        return result

    is_archived = params.get("archived", ["false"])[0] == "true"
    if is_archived:
        result = repo.delete_archived(iid)
    else:
        result = repo.delete(iid)
    if not result:
        return {"error": "not found"}
    memory_id = result.get("memory_id", "")
    if memory_id:
        mem_repo.delete(memory_id)
    return result


def get_tasks(cm, params, pdir):
    repo = TaskRepo(cm.conn, pdir)
    feature_id = params.get("feature_id", [None])[0]
    status = params.get("status", [None])[0]
    tasks = repo.list_by_feature(feature_id=feature_id, status=status)
    return {"tasks": tasks}


def post_tasks(handler, cm, pdir):
    body = _read_body(handler)
    repo = TaskRepo(cm.conn, pdir)
    feature_id = body.get("feature_id", "").strip()
    if not feature_id:
        return {"error": "feature_id is required"}
    tasks = body.get("tasks", [])
    if not tasks:
        return {"error": "tasks array is required"}
    result = repo.batch_create(feature_id, tasks, task_type=body.get("task_type", "manual"))
    return result


def put_task(handler, cm, tid, pdir):
    body = _read_body(handler)
    repo = TaskRepo(cm.conn, pdir)
    fields = {k: body[k] for k in ("status", "title") if k in body}
    result = repo.update(tid, **fields)
    if not result:
        return {"error": "not found"}
    return {"task": result}


def _merged_tag_counts(mem_repo, user_repo, pdir):
    proj = mem_repo.get_tag_counts(project_dir=pdir)
    user = user_repo.get_tag_counts()
    merged = dict(proj)
    for k, v in user.items():
        merged[k] = merged.get(k, 0) + v
    return merged


def _merged_ids_with_tag(mem_repo, user_repo, tag, pdir):
    proj = mem_repo.get_ids_with_tag(tag, project_dir=pdir)
    user = user_repo.get_ids_with_tag(tag)
    seen = {m["id"] for m in proj}
    return proj + [m for m in user if m["id"] not in seen]


def get_stats(cm, pdir):
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    issue_repo = IssueRepo(cm.conn, pdir)

    proj_count = repo.count(project_dir=pdir)
    user_count = user_repo.count()
    total_count = repo.count() + user_count

    all_issues = issue_repo.list_by_date()
    status_counts = {}
    for i in all_issues:
        s = i["status"]
        status_counts[s] = status_counts.get(s, 0) + 1
    archived_issues = issue_repo.list_archived()
    status_counts["archived"] = len(archived_issues)

    tag_counts = _merged_tag_counts(repo, user_repo, pdir)

    return {
        "memories": {"project": proj_count, "user": user_count, "total": total_count},
        "issues": status_counts,
        "tags": tag_counts,
    }


def get_tags(cm, params, pdir):
    query = params.get("query", [None])[0]
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    proj = repo.get_tag_counts(project_dir=pdir)
    user = user_repo.get_tag_counts()
    all_names = sorted(set(proj) | set(user), key=lambda k: -(proj.get(k, 0) + user.get(k, 0)))
    tags = [{"name": k, "count": proj.get(k, 0) + user.get(k, 0),
             "project_count": proj.get(k, 0), "user_count": user.get(k, 0)} for k in all_names]
    if query:
        q = query.lower()
        tags = [t for t in tags if q in t["name"].lower()]
    return {"tags": tags, "total": len(tags)}


def rename_tag(handler, cm, pdir):
    body = _read_body(handler)
    old_name = body.get("old_name", "")
    new_name = body.get("new_name", "").strip()
    if not old_name or not new_name:
        return {"error": "old_name and new_name required"}
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    updated = 0
    for m in _merged_ids_with_tag(repo, user_repo, old_name, pdir):
        tags = json.loads(m["tags"]) if isinstance(m.get("tags"), str) else m.get("tags", [])
        tags = [new_name if t == old_name else t for t in tags]
        tags = list(dict.fromkeys(tags))
        table = "user_memories" if user_repo.get_by_id(m["id"]) else "memories"
        cm.conn.execute(f"UPDATE {table} SET tags=?, updated_at=? WHERE id=?",
                        (json.dumps(tags, ensure_ascii=False), repo._now(), m["id"]))
        updated += 1
    cm.conn.commit()
    return {"updated": updated, "old_name": old_name, "new_name": new_name}


def merge_tags(handler, cm, pdir):
    body = _read_body(handler)
    source_tags = body.get("source_tags", [])
    target_name = body.get("target_name", "").strip()
    if not source_tags or not target_name:
        return {"error": "source_tags and target_name required"}
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    updated = 0
    seen = set()
    for src in source_tags:
        for m in _merged_ids_with_tag(repo, user_repo, src, pdir):
            if m["id"] in seen:
                continue
            seen.add(m["id"])
            tags = json.loads(m["tags"]) if isinstance(m.get("tags"), str) else m.get("tags", [])
            tags = [target_name if t in source_tags else t for t in tags]
            tags = list(dict.fromkeys(tags))
            table = "user_memories" if user_repo.get_by_id(m["id"]) else "memories"
            cm.conn.execute(f"UPDATE {table} SET tags=?, updated_at=? WHERE id=?",
                            (json.dumps(tags, ensure_ascii=False), repo._now(), m["id"]))
            updated += 1
    cm.conn.commit()
    return {"updated": updated, "target_name": target_name}


def delete_tags(handler, cm, pdir):
    body = _read_body(handler)
    tag_names = body.get("tags", [])
    if not tag_names:
        return {"error": "tags required"}
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    updated = 0
    seen = set()
    for tn in tag_names:
        for m in _merged_ids_with_tag(repo, user_repo, tn, pdir):
            if m["id"] in seen:
                continue
            seen.add(m["id"])
            tags = json.loads(m["tags"]) if isinstance(m.get("tags"), str) else m.get("tags", [])
            new_tags = [t for t in tags if t not in tag_names]
            if len(new_tags) != len(tags):
                table = "user_memories" if user_repo.get_by_id(m["id"]) else "memories"
                cm.conn.execute(f"UPDATE {table} SET tags=?, updated_at=? WHERE id=?",
                                (json.dumps(new_tags, ensure_ascii=False), repo._now(), m["id"]))
                updated += 1
    cm.conn.commit()
    return {"deleted_tags": tag_names, "updated_memories": updated}


def get_projects(cm):
    conn = cm.conn
    rows = conn.execute(
        "SELECT project_dir, COUNT(*) as mem_count FROM memories GROUP BY project_dir"
    ).fetchall()
    projects = {}
    for r in rows:
        pd = r["project_dir"]
        projects.setdefault(pd, {"project_dir": pd, "memories": 0, "issues": 0, "tags": set()})
        projects[pd]["memories"] = r["mem_count"]

    issue_rows = conn.execute(
        "SELECT project_dir, COUNT(*) as cnt FROM issues GROUP BY project_dir"
    ).fetchall()
    archive_rows = conn.execute(
        "SELECT project_dir, COUNT(*) as cnt FROM issues_archive GROUP BY project_dir"
    ).fetchall()
    for r in issue_rows:
        pd = r["project_dir"]
        projects.setdefault(pd, {"project_dir": pd, "memories": 0, "issues": 0, "tags": set()})
        projects[pd]["issues"] += r["cnt"]
    for r in archive_rows:
        pd = r["project_dir"]
        projects.setdefault(pd, {"project_dir": pd, "memories": 0, "issues": 0, "tags": set()})
        projects[pd]["issues"] += r["cnt"]

    state_rows = conn.execute("SELECT project_dir FROM session_state").fetchall()
    for r in state_rows:
        pd = r["project_dir"]
        projects.setdefault(pd, {"project_dir": pd, "memories": 0, "issues": 0, "tags": set()})

    tag_rows = conn.execute("SELECT project_dir, tags FROM memories").fetchall()
    for r in tag_rows:
        pd = r["project_dir"]
        if pd in projects:
            tags = json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or [])
            projects[pd]["tags"].update(tags)

    user_repo = UserMemoryRepo(conn)
    user_tag_counts = user_repo.get_tag_counts()
    user_tags = set(user_tag_counts.keys())
    user_count = user_repo.count()

    result = []
    for pd, info in sorted(projects.items(), key=lambda x: -x[1]["memories"]):
        if not pd:
            continue
        result.append({
            "project_dir": pd,
            "name": pd.replace("\\", "/").rsplit("/", 1)[-1] if pd else "unknown",
            "memories": info["memories"],
            "user_memories": user_count,
            "issues": info["issues"],
            "tags": len(info["tags"] | user_tags),
        })
    return {"projects": result}


def add_project(handler, cm):
    body = _read_body(handler)
    project_dir = (body.get("project_dir") or "").strip()
    if not project_dir:
        return {"error": "project_dir is required"}
    project_dir = project_dir.replace("\\", "/")
    conn = cm.conn
    now = __import__("datetime").datetime.now().astimezone().isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO session_state (project_dir, is_blocked, block_reason, next_step, current_task, progress, recent_changes, pending, updated_at) VALUES (?,0,'','','','[]','[]','[]',?)",
        (project_dir, now)
    )
    conn.commit()
    return {"success": True, "project_dir": project_dir}


def browse_directory(params):
    import os
    path = (params.get("path", [None])[0] or "").strip()
    if not path:
        path = os.path.expanduser("~")
    path = os.path.expanduser(path)
    if not os.path.isdir(path):
        return {"error": "not a directory", "path": path}
    dirs = []
    try:
        for entry in sorted(os.scandir(path), key=lambda e: e.name.lower()):
            if entry.is_dir() and not entry.name.startswith("."):
                dirs.append(entry.name)
    except PermissionError:
        return {"error": "permission denied", "path": path}
    return {"path": path.replace("\\", "/"), "dirs": dirs}

def delete_project(cm, project_dir):
    if not project_dir:
        return {"success": False, "error": "Cannot delete empty project"}
    conn = cm.conn
    mem_ids = [r["id"] for r in conn.execute("SELECT id FROM memories WHERE project_dir = ?", (project_dir,)).fetchall()]
    if mem_ids:
        placeholders = ",".join("?" * len(mem_ids))
        conn.execute(f"DELETE FROM vec_memories WHERE id IN ({placeholders})", mem_ids)
        conn.execute(f"DELETE FROM memories WHERE id IN ({placeholders})", mem_ids)
    conn.execute("DELETE FROM issues WHERE project_dir = ?", (project_dir,))
    conn.execute("DELETE FROM issues_archive WHERE project_dir = ?", (project_dir,))
    conn.execute("DELETE FROM session_state WHERE project_dir = ?", (project_dir,))
    conn.commit()
    return {"success": True, "deleted_memories": len(mem_ids)}



def export_memories(cm, params, pdir):
    scope = params.get("scope", ["all"])[0]
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)

    if scope == "user":
        memories = user_repo.get_all(limit=999999)
        vec_table = "vec_user_memories"
    elif scope == "project":
        memories = repo.get_all(limit=999999, project_dir=pdir)
        vec_table = "vec_memories"
    else:
        memories = repo.get_all(limit=999999) + user_repo.get_all(limit=999999)
        vec_table = None  # 需要按记忆判断

    result = []
    for m in memories:
        entry = dict(m)
        tbl = vec_table
        if tbl is None:
            tbl = "vec_user_memories" if user_repo.get_by_id(m["id"]) else "vec_memories"
        row = cm.conn.execute(f"SELECT embedding FROM {tbl} WHERE id=?", (m["id"],)).fetchone()
        if row:
            raw = row["embedding"]
            if isinstance(raw, (bytes, memoryview)):
                import struct
                if len(raw) >= 4 and len(raw) % 4 == 0:
                    entry["embedding"] = list(struct.unpack(f'{len(raw)//4}f', raw))
                else:
                    entry["embedding"] = None
            else:
                try:
                    entry["embedding"] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    entry["embedding"] = None
        else:
            entry["embedding"] = None
        result.append(entry)
    return {"memories": result, "count": len(result), "project_dir": pdir}


def import_memories(handler, cm, pdir):
    body = _read_body(handler)
    items = body.get("memories", [])
    if not items:
        return {"error": "no memories to import"}
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    imported, skipped = 0, 0
    for item in items:
        mid = item.get("id", "")
        if not mid or repo.get_by_id(mid) or user_repo.get_by_id(mid):
            skipped += 1
            continue
        now = repo._now()
        tags = item.get("tags", "[]")
        tags_str = json.dumps(tags, ensure_ascii=False) if isinstance(tags, list) else tags
        scope = item.get("scope", "project")
        embedding = item.get("embedding")
        if scope == "user":
            cm.conn.execute(
                "INSERT INTO user_memories (id, content, tags, source, session_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (mid, item.get("content", ""), tags_str, item.get("source", "manual"),
                 item.get("session_id", 0), item.get("created_at", now), now)
            )
            if embedding:
                cm.conn.execute("INSERT INTO vec_user_memories (id, embedding) VALUES (?,?)", (mid, json.dumps(embedding)))
        else:
            cm.conn.execute(
                "INSERT INTO memories (id, content, tags, scope, project_dir, session_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                (mid, item.get("content", ""), tags_str, scope,
                 item.get("project_dir", pdir), item.get("session_id", 0), item.get("created_at", now), now)
            )
            if embedding:
                cm.conn.execute("INSERT INTO vec_memories (id, embedding) VALUES (?,?)", (mid, json.dumps(embedding)))
        imported += 1
    cm.conn.commit()
    return {"imported": imported, "skipped": skipped}


def search_memories(handler, cm, pdir):
    body = _read_body(handler)
    query = body.get("query", "").strip()
    if not query:
        return {"error": "query required"}
    top_k = body.get("top_k", 20)
    scope = body.get("scope", "all")
    tags = body.get("tags", [])

    engine = getattr(cm, "_embedding_engine", None)
    if not engine:
        return {"error": "embedding engine not loaded"}

    embedding = engine.encode(query)
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)

    if scope == "user":
        if tags:
            results = user_repo.search_by_vector_with_tags(embedding, tags, top_k=top_k)
        else:
            results = user_repo.search_by_vector(embedding, top_k=top_k)
    elif scope == "project":
        if tags:
            results = repo.search_by_vector_with_tags(embedding, tags, top_k=top_k, scope="project", project_dir=pdir)
        else:
            results = repo.search_by_vector(embedding, top_k=top_k, scope="project", project_dir=pdir)
    else:
        if tags:
            proj_results = repo.search_by_vector_with_tags(embedding, tags, top_k=top_k, scope="project", project_dir=pdir)
            user_results = user_repo.search_by_vector_with_tags(embedding, tags, top_k=top_k)
        else:
            proj_results = repo.search_by_vector(embedding, top_k=top_k, scope="project", project_dir=pdir)
            user_results = user_repo.search_by_vector(embedding, top_k=top_k)
        results = sorted(proj_results + user_results, key=lambda x: x.get("distance", 0))[:top_k]

    for r in results:
        r["similarity"] = round(1 - (r.get("distance", 0) ** 2) / 2, 4)
    return {"results": results, "count": len(results), "query": query}


def delete_task(cm, tid, pdir):
    repo = TaskRepo(cm.conn, pdir)
    result = repo.delete(tid)
    return result if result else {"error": "not found"}


def delete_tasks_by_feature(handler, cm, pdir, params):
    feature_id = params.get("feature_id", [None])[0]
    if not feature_id:
        return {"error": "feature_id is required"}
    repo = TaskRepo(cm.conn, pdir)
    count = repo.delete_by_feature(feature_id)
    return {"deleted": count, "feature_id": feature_id}
