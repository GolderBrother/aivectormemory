import json

from .base import BaseMemoryRepo


class MemoryRepo(BaseMemoryRepo):
    TABLE = "memories"
    VEC_TABLE = "vec_memories"
    TAG_TABLE = "memory_tags"

    def insert(self, content: str, tags: list[str], scope: str, session_id: int,
               embedding: list[float], dedup_threshold: float = 0.95,
               source: str = "manual") -> dict:
        return super().insert(content, tags, session_id, embedding, dedup_threshold, source, scope=scope)

    def _build_insert(self, mid, content, tags, source, session_id, now, extra):
        scope = extra.get("scope", "project")
        cols = "id, content, tags, scope, source, project_dir, session_id, created_at, updated_at"
        vals = [mid, content, json.dumps(tags, ensure_ascii=False), scope, source, self.project_dir, session_id, now, now]
        return cols, vals

    def _is_same_scope(self, mid: str) -> bool:
        mem = self.conn.execute("SELECT project_dir FROM memories WHERE id=?", (mid,)).fetchone()
        return mem and mem["project_dir"] == self.project_dir

    def _match_filters(self, mem, filters) -> bool:
        scope = filters.get("scope", "all")
        if scope == "project" and mem["project_dir"] != filters.get("project_dir", self.project_dir):
            return False
        source = filters.get("source")
        return not source or mem.get("source", "manual") == source

    def list_by_tags(self, tags: list[str], scope: str = "all", project_dir: str = "",
                     limit: int = 100, source: str | None = None, **_) -> list[dict]:
        sql, params = "SELECT * FROM memories WHERE 1=1", []
        if scope == "project":
            sql += " AND project_dir=?"
            params.append(project_dir or self.project_dir)
        if source:
            sql += " AND source=?"
            params.append(source)
        for tag in tags:
            sql += " AND id IN (SELECT memory_id FROM memory_tags WHERE tag=?)"
            params.append(tag)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def get_all(self, limit: int = 100, offset: int = 0, project_dir: str | None = None) -> list[dict]:
        if project_dir is not None:
            rows = self.conn.execute(
                "SELECT * FROM memories WHERE project_dir = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (project_dir, limit, offset)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM memories ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset)).fetchall()
        return [dict(r) for r in rows]

    def count(self, project_dir: str | None = None) -> int:
        if project_dir is not None:
            return self.conn.execute("SELECT COUNT(*) FROM memories WHERE project_dir=?", (project_dir,)).fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]

    def get_tag_counts(self, project_dir: str | None = None) -> dict[str, int]:
        if project_dir is not None:
            rows = self.conn.execute(
                "SELECT mt.tag, COUNT(*) as cnt FROM memory_tags mt "
                "JOIN memories m ON mt.memory_id = m.id WHERE m.project_dir=? GROUP BY mt.tag",
                (project_dir,)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT tag, COUNT(*) as cnt FROM memory_tags GROUP BY tag").fetchall()
        return {r["tag"]: r["cnt"] for r in rows}

    def get_ids_with_tag(self, tag: str, project_dir: str | None = None) -> list[dict]:
        sql = "SELECT m.id, m.tags FROM memories m JOIN memory_tags mt ON m.id = mt.memory_id WHERE mt.tag=?"
        params: list = [tag]
        if project_dir is not None:
            sql += " AND m.project_dir=?"
            params.append(project_dir)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
