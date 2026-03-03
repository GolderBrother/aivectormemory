"""v10: 创建 memory_tags / user_memory_tags 关联表，从 JSON tags 填充"""
import json
from aivectormemory.db.schema import MEMORY_TAGS_TABLE, USER_MEMORY_TAGS_TABLE


def upgrade(conn, **_):
    conn.execute(MEMORY_TAGS_TABLE)
    conn.execute(USER_MEMORY_TAGS_TABLE)
    for row in conn.execute("SELECT id, tags FROM memories").fetchall():
        tags_raw = row["tags"]
        tags_list = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
        for t in tags_list:
            conn.execute("INSERT OR IGNORE INTO memory_tags (memory_id, tag) VALUES (?,?)", (row["id"], t))
    for row in conn.execute("SELECT id, tags FROM user_memories").fetchall():
        tags_raw = row["tags"]
        tags_list = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
        for t in tags_list:
            conn.execute("INSERT OR IGNORE INTO user_memory_tags (memory_id, tag) VALUES (?,?)", (row["id"], t))
