from aivectormemory.config import DEFAULT_TOP_K
from aivectormemory.db.memory_repo import MemoryRepo
from aivectormemory.db.user_memory_repo import UserMemoryRepo
from aivectormemory.db.issue_repo import IssueRepo
from aivectormemory.errors import success_response
from aivectormemory.i18n.responses import to_json


BRIEF_KEYS = {"content", "tags"}


def _to_brief(rows):
    return [{k: r[k] for k in BRIEF_KEYS if k in r} for r in rows]


def _add_similarity(rows, has_tags=False):
    results = []
    for r in rows:
        distance = r.pop("distance", 0)
        r["similarity"] = round(1 - distance, 4) if has_tags else round(1 - (distance ** 2) / 2, 4)
        results.append(r)
    results.sort(key=lambda x: x["similarity"], reverse=True)
    return results


def handle_recall(args, *, cm, engine, **_):
    source = args.get("source")

    if source == "experience":
        return _recall_experience(args, cm=cm, engine=engine)

    scope = args.get("scope", "all")
    query = args.get("query")
    tags = args.get("tags")
    top_k = args.get("top_k", DEFAULT_TOP_K)
    brief = args.get("brief", False)
    tags_mode = args.get("tags_mode", "any" if query and tags else "all")

    if scope == "user":
        rows = _query_user(cm, engine, query, tags, top_k, source, tags_mode)
    elif scope == "project":
        rows = _query_project(cm, engine, query, tags, top_k, source, tags_mode)
    else:
        rows = _query_all(cm, engine, query, tags, top_k, source, tags_mode)

    return to_json(success_response(memories=_to_brief(rows) if brief else rows))


def _query_user(cm, engine, query, tags, top_k, source, tags_mode="all"):
    repo = UserMemoryRepo(cm.conn)
    if not query:
        if not tags:
            raise ValueError("query or tags is required")
        rows = repo.list_by_tags(tags, limit=top_k, source=source, tags_mode=tags_mode)
        for r in rows:
            r["similarity"] = 1.0
        return rows
    embedding = engine.encode(query)
    if tags:
        return _add_similarity(repo.search_by_vector_with_tags(embedding, tags, top_k=top_k), has_tags=True)
    return _add_similarity(repo.search_by_vector(embedding, top_k=top_k))


def _query_project(cm, engine, query, tags, top_k, source, tags_mode="all"):
    repo = MemoryRepo(cm.conn, cm.project_dir)
    if not query:
        if not tags:
            raise ValueError("query or tags is required")
        rows = repo.list_by_tags(tags, scope="project", project_dir=cm.project_dir, limit=top_k, source=source, tags_mode=tags_mode)
        for r in rows:
            r["similarity"] = 1.0
        return rows
    embedding = engine.encode(query)
    if tags:
        return _add_similarity(repo.search_by_vector_with_tags(embedding, tags, top_k=top_k, scope="project", project_dir=cm.project_dir, source=source), has_tags=True)
    return _add_similarity(repo.search_by_vector(embedding, top_k=top_k, scope="project", project_dir=cm.project_dir, source=source))


def _query_all(cm, engine, query, tags, top_k, source, tags_mode="all"):
    proj = _query_project(cm, engine, query, tags, top_k, source, tags_mode)
    user = _query_user(cm, engine, query, tags, top_k, source, tags_mode)
    merged = proj + user
    merged.sort(key=lambda x: x.get("similarity", 0), reverse=True)
    return merged[:top_k]


def _recall_experience(args, *, cm, engine):
    query = args.get("query")
    if not query:
        raise ValueError("query is required for source=experience")
    top_k = args.get("top_k", DEFAULT_TOP_K)
    brief = args.get("brief", False)

    embedding = engine.encode(query)
    issue_repo = IssueRepo(cm.conn, cm.project_dir, engine=engine)
    rows = issue_repo.search_archive_by_vector(embedding, top_k=top_k)

    results = []
    for r in rows:
        content = f"{r['title']}\n根因：{r['root_cause'] or ''}\n方案：{r['solution'] or ''}"
        results.append({
            "id": r["id"],
            "content": content,
            "tags": ["经验"],
            "similarity": r["similarity"],
        })
    return to_json(success_response(memories=_to_brief(results) if brief else results))
