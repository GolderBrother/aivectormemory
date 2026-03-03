from .base import BaseRepo


class TaskRepo(BaseRepo):

    def batch_create(self, feature_id: str, tasks: list[dict], task_type: str = "manual") -> dict:
        created, skipped = 0, 0
        now = self._now()
        # 一次查询加载所有已有记录到内存
        existing_rows = self.conn.execute(
            "SELECT id, title, parent_id, sort_order FROM tasks WHERE project_dir=? AND feature_id=?",
            (self.project_dir, feature_id)
        ).fetchall()
        title_key_set = {(r["title"], r["parent_id"]) for r in existing_rows}
        sort_key_set = {r["sort_order"] for r in existing_rows if r["parent_id"] == 0}

        for t in tasks:
            title = t.get("title", "").strip()
            if not title:
                skipped += 1
                continue
            parent_id = t.get("parent_id", 0)
            sort_order = t.get("sort_order", 0)
            if (title, parent_id) in title_key_set:
                skipped += 1
                continue
            if parent_id == 0 and sort_order > 0 and sort_order in sort_key_set:
                skipped += 1
                continue
            cur = self.conn.execute(
                "INSERT INTO tasks (project_dir,feature_id,title,status,sort_order,parent_id,task_type,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (self.project_dir, feature_id, title, "pending", sort_order, parent_id, task_type, t.get("metadata", "{}"), now, now)
            )
            title_key_set.add((title, parent_id))
            if parent_id == 0:
                sort_key_set.add(sort_order)
            created += 1
            node_id = cur.lastrowid
            for child in t.get("children", []):
                child_title = child.get("title", "").strip()
                if not child_title:
                    skipped += 1
                    continue
                if (child_title, node_id) in title_key_set:
                    skipped += 1
                    continue
                self.conn.execute(
                    "INSERT INTO tasks (project_dir,feature_id,title,status,sort_order,parent_id,task_type,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (self.project_dir, feature_id, child_title, "pending", child.get("sort_order", 0), node_id, task_type, child.get("metadata", "{}"), now, now)
                )
                title_key_set.add((child_title, node_id))
                created += 1
        self._commit()
        return {"created": created, "skipped": skipped, "feature_id": feature_id}

    def update(self, task_id: int, **fields) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE id=? AND project_dir=?",
            (task_id, self.project_dir)
        ).fetchone()
        if not row:
            return None
        allowed = {"status", "title"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return dict(row)
        updates["updated_at"] = self._now()
        set_clause = ",".join(f"{k}=?" for k in updates)
        self.conn.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", [*updates.values(), task_id])
        self._commit()
        return dict(self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())

    def list_by_feature(self, feature_id: str | None = None, status: str | None = None) -> list[dict]:
        sql, params = "SELECT * FROM tasks WHERE project_dir=?", [self.project_dir]
        if feature_id:
            sql += " AND feature_id=?"
            params.append(feature_id)
        sql += " ORDER BY feature_id, sort_order, id"
        rows = [dict(r) for r in self.conn.execute(sql, params).fetchall()]

        top_level = [r for r in rows if r.get("parent_id", 0) == 0]
        children_map: dict[int, list[dict]] = {}
        for r in rows:
            pid = r.get("parent_id", 0)
            if pid != 0:
                children_map.setdefault(pid, []).append(r)

        result = []
        for node in top_level:
            all_kids = children_map.get(node["id"], [])
            if all_kids:
                # 有子任务的节点：过滤子任务，动态计算节点状态
                kids = [k for k in all_kids if k["status"] == status] if status else all_kids
                if status and not kids:
                    continue
                node["children"] = kids
                node["status"] = self._compute_status(kids)
                result.append(node)
            else:
                # 扁平任务（无子任务）：直接按 status 过滤
                node["children"] = []
                if status and node["status"] != status:
                    continue
                result.append(node)
        return result

    def _compute_status(self, children: list[dict]) -> str:
        statuses = {c["status"] for c in children}
        if statuses == {"completed"}:
            return "completed"
        if statuses == {"pending"}:
            return "pending"
        return "in_progress"

    def delete(self, task_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE id=? AND project_dir=?",
            (task_id, self.project_dir)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        self.conn.execute("DELETE FROM tasks WHERE parent_id=? AND project_dir=?", (task_id, self.project_dir))
        self.conn.execute("DELETE FROM tasks WHERE id=? AND project_dir=?", (task_id, self.project_dir))
        self._commit()
        return result

    def delete_by_feature(self, feature_id: str) -> int:
        count = self.conn.execute(
            "SELECT COUNT(*) as c FROM tasks WHERE project_dir=? AND feature_id=?",
            (self.project_dir, feature_id)
        ).fetchone()["c"]
        self.conn.execute(
            "DELETE FROM tasks WHERE project_dir=? AND feature_id=?",
            (self.project_dir, feature_id)
        )
        self._commit()
        return count

    def complete_by_feature(self, feature_id: str):
        now = self._now()
        self.conn.execute(
            "UPDATE tasks SET status='completed', updated_at=? WHERE project_dir=? AND feature_id=? AND status!='completed'",
            (now, self.project_dir, feature_id)
        )
        self._commit()

    def archive_by_feature(self, feature_id: str) -> dict:
        now = self._now()
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE project_dir=? AND feature_id=?",
            (self.project_dir, feature_id)
        ).fetchall()
        count = 0
        for r in rows:
            self.conn.execute(
                """INSERT INTO tasks_archive
                   (project_dir, feature_id, title, status, sort_order, parent_id,
                    task_type, metadata, original_task_id, archived_at, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r["project_dir"], r["feature_id"], r["title"], r["status"],
                 r["sort_order"], r["parent_id"], r["task_type"], r["metadata"],
                 r["id"], now, r["created_at"], r["updated_at"])
            )
            count += 1
        self.conn.execute(
            "DELETE FROM tasks WHERE project_dir=? AND feature_id=?",
            (self.project_dir, feature_id)
        )
        self._commit()
        return {"archived": count, "feature_id": feature_id}

    def list_archived(self, feature_id: str | None = None) -> list[dict]:
        sql, params = "SELECT * FROM tasks_archive WHERE project_dir=?", [self.project_dir]
        if feature_id:
            sql += " AND feature_id=?"
            params.append(feature_id)
        sql += " ORDER BY feature_id, sort_order, id"
        rows = [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        id_map = {r["original_task_id"]: r for r in rows}
        top_level = [r for r in rows if r["parent_id"] == 0]
        for node in top_level:
            node["children"] = [r for r in rows if r["parent_id"] == node["original_task_id"]]
        return top_level

    def get_task_progress_batch(self, feature_ids: list[str]) -> dict[str, dict]:
        """批量获取任务进度 {feature_id: {total: n, done: n}}"""
        if not feature_ids:
            return {}
        placeholders = ",".join("?" * len(feature_ids))
        rows = self.conn.execute(
            f"SELECT id, feature_id, status, parent_id FROM tasks WHERE project_dir=? AND feature_id IN ({placeholders})",
            [self.project_dir] + feature_ids
        ).fetchall()
        by_fid: dict[str, list[dict]] = {}
        for r in rows:
            by_fid.setdefault(r["feature_id"], []).append(dict(r))
        result = {}
        for fid, tasks in by_fid.items():
            children = [t for t in tasks if t["parent_id"] != 0]
            parent_ids_with_children = {c["parent_id"] for c in children}
            flat_tops = [t for t in tasks if t["parent_id"] == 0 and t["id"] not in parent_ids_with_children]
            total = len(children) + len(flat_tops)
            done = sum(1 for c in children if c["status"] == "completed") + \
                   sum(1 for t in flat_tops if t["status"] == "completed")
            result[fid] = {"total": total, "done": done}
        return result

    def get_feature_status(self, feature_id: str) -> str:
        rows = self.conn.execute(
            "SELECT status FROM tasks WHERE project_dir=? AND feature_id=? AND parent_id!=0",
            (self.project_dir, feature_id)
        ).fetchall()
        if not rows:
            rows = self.conn.execute(
                "SELECT status FROM tasks WHERE project_dir=? AND feature_id=?",
                (self.project_dir, feature_id)
            ).fetchall()
        if not rows:
            return "pending"
        statuses = {r["status"] for r in rows}
        if statuses == {"completed"}:
            return "completed"
        if statuses == {"pending"}:
            return "pending"
        return "in_progress"
