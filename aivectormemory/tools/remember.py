import json
from aivectormemory.config import DEDUP_THRESHOLD
from aivectormemory.db.memory_repo import MemoryRepo
from aivectormemory.db.user_memory_repo import UserMemoryRepo
from aivectormemory.errors import success_response
from aivectormemory.tools.keywords import extract_keywords
from aivectormemory.utils import validate_content, validate_tags


def handle_remember(args, *, cm, engine, session_id, **_):
    content = validate_content(args.get("content", ""))
    tags = validate_tags(args.get("tags", []))
    scope = args.get("scope", "project")

    if len(content) > 5000:
        content = content[:5000]

    # 自动从 content 提取关键词补充到 tags
    existing = {t.lower() for t in tags}
    for kw in extract_keywords(content):
        if kw.lower() not in existing:
            tags.append(kw)
            existing.add(kw.lower())

    embedding = engine.encode(content)

    if scope == "user":
        repo = UserMemoryRepo(cm.conn)
        result = repo.insert(content, tags, session_id, embedding, DEDUP_THRESHOLD)
    else:
        repo = MemoryRepo(cm.conn, cm.project_dir)
        result = repo.insert(content, tags, scope, session_id, embedding, DEDUP_THRESHOLD)

    return json.dumps(success_response(
        id=result["id"], action=result["action"],
        tags=tags, scope=scope
    ))
