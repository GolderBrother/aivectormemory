import json
import sqlite3
import uuid
from datetime import datetime


class MemoryRepo:
    def __init__(self, conn: sqlite3.Connection, project_dir: str = ""):
        self.conn = conn
        self.project_dir = project_dir

    def _now(self) -> str:
        return datetime.now().astimezone().isoformat()

    def insert(self, content: str, tags: list[str], scope: str, session_id: int,
               embedding: list[float], dedup_threshold: float = 0.95,
               source: str = "manual") -> dict:
        pdir = self.project_dir
        dup = self.find_duplicate(embedding, dedup_threshold, pdir)
        if dup:
            return self.update(dup["id"], content, tags, session_id, embedding)

        now = self._now()
        mid = uuid.uuid4().hex[:12]
        self.conn.execute(
            "INSERT INTO memories (id, content, tags, scope, source, project_dir, session_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (mid, content, json.dumps(tags, ensure_ascii=False), scope, source, pdir, session_id, now, now)
        )
        self.conn.execute(
            "INSERT INTO vec_memories (id, embedding) VALUES (?, ?)",
            (mid, json.dumps(embedding))
        )
        self.conn.commit()
        return {"id": mid, "action": "created"}

    def update(self, mid: str, content: str, tags: list[str], session_id: int,
               embedding: list[float]) -> dict:
        now = self._now()
        self.conn.execute(
            "UPDATE memories SET content=?, tags=?, session_id=?, updated_at=? WHERE id=?",
            (content, json.dumps(tags, ensure_ascii=False), session_id, now, mid)
        )
        self.conn.execute("DELETE FROM vec_memories WHERE id=?", (mid,))
        self.conn.execute(
            "INSERT INTO vec_memories (id, embedding) VALUES (?, ?)",
            (mid, json.dumps(embedding))
        )
        self.conn.commit()
        return {"id": mid, "action": "updated"}

    def find_duplicate(self, embedding: list[float], threshold: float, project_dir: str = "") -> dict | None:
        rows = self.conn.execute(
            "SELECT id, distance FROM vec_memories WHERE embedding MATCH ? AND k = 5",
            (json.dumps(embedding),)
        ).fetchall()
        for r in rows:
            mem = self.conn.execute("SELECT project_dir FROM memories WHERE id=?", (r["id"],)).fetchone()
            if mem and mem["project_dir"] == project_dir:
                similarity = 1 - (r["distance"] ** 2) / 2
                if similarity >= threshold:
                    return dict(r)
        return None

    def search_by_vector(self, embedding: list[float], top_k: int = 5,
                         scope: str = "all", project_dir: str = "",
                         source: str | None = None) -> list[dict]:
        k = top_k * 3
        rows = self.conn.execute(
            "SELECT id, distance FROM vec_memories WHERE embedding MATCH ? AND k = ?",
            (json.dumps(embedding), k)
        ).fetchall()
        results = []
        for r in rows:
            mem = self.conn.execute("SELECT * FROM memories WHERE id=?", (r["id"],)).fetchone()
            if not mem:
                continue
            if scope == "project" and mem["project_dir"] != project_dir:
                continue
            if source and mem.get("source", "manual") != source:
                continue
            d = dict(mem)
            d["distance"] = r["distance"]
            results.append(d)
            if len(results) >= top_k:
                break
        return results

    def search_by_vector_with_tags(self, embedding: list[float], tags: list[str],
                                    top_k: int = 5, scope: str = "all",
                                    project_dir: str = "",
                                    source: str | None = None) -> list[dict]:
        import numpy as np
        candidates = self.list_by_tags(tags, scope=scope, project_dir=project_dir, limit=1000, source=source)
        if not candidates:
            return []
        query_vec = np.array(embedding, dtype=np.float32)
        final = []
        for mem in candidates:
            row = self.conn.execute("SELECT embedding FROM vec_memories WHERE id=?", (mem["id"],)).fetchone()
            if not row:
                continue
            raw = row["embedding"]
            vec = np.frombuffer(raw, dtype=np.float32) if isinstance(raw, (bytes, memoryview)) else np.array(json.loads(raw), dtype=np.float32)
            cos_sim = float(np.dot(query_vec, vec) / (np.linalg.norm(query_vec) * np.linalg.norm(vec) + 1e-9))
            d = dict(mem)
            d["distance"] = 1 - cos_sim
            final.append(d)
        final.sort(key=lambda x: x["distance"])
        return final[:top_k]

    def delete(self, mid: str) -> bool:
        cur = self.conn.execute("DELETE FROM memories WHERE id=?", (mid,))
        self.conn.execute("DELETE FROM vec_memories WHERE id=?", (mid,))
        self.conn.commit()
        return cur.rowcount > 0

    def get_all(self, limit: int = 100, offset: int = 0, project_dir: str | None = None) -> list[dict]:
        if project_dir is not None:
            rows = self.conn.execute(
                "SELECT * FROM memories WHERE project_dir = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (project_dir, limit, offset)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM memories ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_by_id(self, mid: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM memories WHERE id=?", (mid,)).fetchone()
        return dict(row) if row else None

    def count(self, project_dir: str | None = None) -> int:
        if project_dir is not None:
            return self.conn.execute("SELECT COUNT(*) FROM memories WHERE project_dir=?", (project_dir,)).fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]
    def list_by_tags(self, tags: list[str], scope: str = "all", project_dir: str = "",
                     limit: int = 100, source: str | None = None) -> list[dict]:
        sql, params = "SELECT * FROM memories WHERE 1=1", []
        if scope == "project":
            sql += " AND project_dir=?"
            params.append(project_dir)
        if source:
            sql += " AND source=?"
            params.append(source)
        for tag in tags:
            sql += " AND tags LIKE ?"
            params.append(f'%"{tag}"%')
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def get_tag_counts(self, project_dir: str | None = None) -> dict[str, int]:
        if project_dir is not None:
            rows = self.conn.execute("SELECT tags FROM memories WHERE project_dir=?", (project_dir,)).fetchall()
        else:
            rows = self.conn.execute("SELECT tags FROM memories").fetchall()
        counts = {}
        for r in rows:
            tags = json.loads(r["tags"]) if isinstance(r["tags"], str) else (r["tags"] or [])
            for t in tags:
                counts[t] = counts.get(t, 0) + 1
        return counts

    def get_ids_with_tag(self, tag: str, project_dir: str | None = None) -> list[dict]:
        sql = "SELECT id, tags FROM memories WHERE tags LIKE ?"
        params = [f'%"{tag}"%']
        if project_dir is not None:
            sql += " AND project_dir=?"
            params.append(project_dir)
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
