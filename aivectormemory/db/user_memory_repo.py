import json

from .base import BaseMemoryRepo


class UserMemoryRepo(BaseMemoryRepo):
    """管理 user_memories + vec_user_memories 表（跨项目用户偏好）"""
    TABLE = "user_memories"
    VEC_TABLE = "vec_user_memories"
    TAG_TABLE = "user_memory_tags"

    def __init__(self, conn):
        super().__init__(conn, project_dir="")

    def _build_insert(self, mid, content, tags, source, session_id, now, extra):
        cols = "id, content, tags, source, session_id, created_at, updated_at"
        vals = [mid, content, json.dumps(tags, ensure_ascii=False), source, session_id, now, now]
        return cols, vals

    def list_by_tags(self, tags: list[str], limit: int = 100, source: str | None = None, **_) -> list[dict]:
        sql, params = "SELECT * FROM user_memories WHERE 1=1", []
        if source:
            sql += " AND source=?"
            params.append(source)
        for tag in tags:
            sql += " AND id IN (SELECT memory_id FROM user_memory_tags WHERE tag=?)"
            params.append(tag)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def get_all(self, limit: int = 100, offset: int = 0) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM user_memories ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset)).fetchall()
        return [dict(r) for r in rows]

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM user_memories").fetchone()[0]

    def get_tag_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT tag, COUNT(*) as cnt FROM user_memory_tags GROUP BY tag").fetchall()
        return {r["tag"]: r["cnt"] for r in rows}

    def get_ids_with_tag(self, tag: str) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT um.id, um.tags FROM user_memories um JOIN user_memory_tags umt ON um.id = umt.memory_id WHERE umt.tag=?",
            (tag,)
        ).fetchall()]
