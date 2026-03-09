from aivectormemory.db.memory_repo import MemoryRepo
from aivectormemory.db.user_memory_repo import UserMemoryRepo
from aivectormemory.i18n.responses import fmt


def handle_forget(args, *, cm, **_):
    mid = args.get("memory_id")
    mids = args.get("memory_ids", [])
    tags = args.get("tags")
    scope = args.get("scope", "all")

    mem_repo = MemoryRepo(cm.conn, cm.project_dir)
    user_repo = UserMemoryRepo(cm.conn)

    if tags:
        ids = []
        if scope in ("project", "all"):
            ids += [r["id"] for r in mem_repo.list_by_tags(tags, scope="project", project_dir=cm.project_dir, limit=10000)]
        if scope in ("user", "all"):
            ids += [r["id"] for r in user_repo.list_by_tags(tags, limit=10000)]
    else:
        ids = [mid] if mid else mids

    if not ids:
        raise ValueError("memory_id, memory_ids, or tags is required")

    deleted = []
    not_found = []
    for i in ids:
        if scope == "user":
            ok = user_repo.delete(i)
        elif scope == "project":
            ok = mem_repo.delete(i)
        else:
            ok = mem_repo.delete(i) or user_repo.delete(i)
        (deleted if ok else not_found).append(i)

    text = fmt("forget", deleted_count=len(deleted))
    if not_found:
        text += fmt("forget.not_found", not_found_count=len(not_found))
    return text
