import json

from aivectormemory.db.memory_repo import MemoryRepo
from aivectormemory.db.user_memory_repo import UserMemoryRepo
from aivectormemory.db.issue_repo import IssueRepo


def get_stats(cm, pdir):
    repo = MemoryRepo(cm.conn, pdir)
    user_repo = UserMemoryRepo(cm.conn)
    issue_repo = IssueRepo(cm.conn, pdir)

    proj_count = repo.count(project_dir=pdir)
    user_count = user_repo.count()
    total_count = repo.count() + user_count

    all_issues, _ = issue_repo.list_by_date()
    status_counts = {}
    for i in all_issues:
        s = i["status"]
        status_counts[s] = status_counts.get(s, 0) + 1
    _, archived_total = issue_repo.list_archived()
    status_counts["archived"] = archived_total

    tag_counts = _merged_tag_counts(repo, user_repo, pdir)

    return {
        "memories": {"project": proj_count, "user": user_count, "total": total_count},
        "issues": status_counts,
        "tags": tag_counts,
    }


def _merged_tag_counts(mem_repo, user_repo, pdir):
    proj = mem_repo.get_tag_counts(project_dir=pdir)
    user = user_repo.get_tag_counts()
    merged = dict(proj)
    for k, v in user.items():
        merged[k] = merged.get(k, 0) + v
    return merged


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
    from aivectormemory.web.api import _read_body
    body = _read_body(handler)
    project_dir = (body.get("project_dir") or "").strip()
    if not project_dir:
        return {"error": "project_dir is required"}
    project_dir = project_dir.replace("\\", "/")
    conn = cm.conn
    now = __import__("datetime").datetime.now().astimezone().isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO session_state (project_dir, is_blocked, block_reason, next_step, current_task, progress, recent_changes, pending, updated_at) VALUES (?,0,'','','','[]','[]','[]',?)",
        (project_dir, now))
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
