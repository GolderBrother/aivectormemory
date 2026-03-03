import json

from aivectormemory.utils import now_iso, safe_table
from aivectormemory.db.memory_repo import MemoryRepo
from aivectormemory.db.user_memory_repo import UserMemoryRepo


def _merged_ids_with_tag(mem_repo, user_repo, tag, pdir):
    proj = mem_repo.get_ids_with_tag(tag, project_dir=pdir)
    user = user_repo.get_ids_with_tag(tag)
    seen = {m["id"] for m in proj}
    return proj + [m for m in user if m["id"] not in seen]


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
    from aivectormemory.web.api import _read_body
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
        cm.conn.execute(f"UPDATE {safe_table(table)} SET tags=?, updated_at=? WHERE id=?",
                        (json.dumps(tags, ensure_ascii=False), now_iso(), m["id"]))
        updated += 1
    cm.conn.commit()
    return {"updated": updated, "old_name": old_name, "new_name": new_name}


def merge_tags(handler, cm, pdir):
    from aivectormemory.web.api import _read_body
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
            cm.conn.execute(f"UPDATE {safe_table(table)} SET tags=?, updated_at=? WHERE id=?",
                            (json.dumps(tags, ensure_ascii=False), now_iso(), m["id"]))
            updated += 1
    cm.conn.commit()
    return {"updated": updated, "target_name": target_name}


def delete_tags(handler, cm, pdir):
    from aivectormemory.web.api import _read_body
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
                cm.conn.execute(f"UPDATE {safe_table(table)} SET tags=?, updated_at=? WHERE id=?",
                                (json.dumps(new_tags, ensure_ascii=False), now_iso(), m["id"]))
                updated += 1
    cm.conn.commit()
    return {"deleted_tags": tag_names, "updated_memories": updated}
