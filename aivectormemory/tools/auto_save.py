from aivectormemory.config import DEDUP_THRESHOLD
from aivectormemory.db.user_memory_repo import UserMemoryRepo
from aivectormemory.i18n.responses import fmt
from aivectormemory.tools.keywords import extract_keywords


def handle_auto_save(args, *, cm, engine, session_id, **_):
    items = args.get("preferences", [])
    if not items or not isinstance(items, list):
        return fmt("auto_save.empty")

    repo = UserMemoryRepo(cm.conn)
    saved = []
    with cm.transaction():
        for item in items:
            if not item or not isinstance(item, str):
                continue
            item = item[:5000] if len(item) > 5000 else item
            embedding = engine.encode(item)
            tags = ["preference"] + args.get("extra_tags", [])
            # 自动从 item 提取关键词补充到 tags
            existing = {t.lower() for t in tags}
            for kw in extract_keywords(item):
                if kw.lower() not in existing:
                    tags.append(kw)
                    existing.add(kw.lower())
            result = repo.insert(item, tags, session_id, embedding, DEDUP_THRESHOLD, source="auto_save")
            saved.append({"id": result["id"], "action": result["action"], "category": "preferences"})

    return fmt("auto_save", count=len(saved))
