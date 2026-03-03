import json

from .base import BaseRepo


class StateRepo(BaseRepo):

    def get(self) -> dict | None:
        row = self.conn.execute("SELECT * FROM session_state WHERE project_dir=?", (self.project_dir,)).fetchone()
        if not row:
            return None
        d = dict(row)
        for key in ("progress", "recent_changes", "pending"):
            d[key] = json.loads(d[key])
        d["is_blocked"] = bool(d["is_blocked"])
        return d

    def upsert(self, **fields) -> dict:
        now = self._now()
        current = self.get()

        for key in ("progress", "recent_changes", "pending"):
            if key in fields and isinstance(fields[key], list):
                fields[key] = json.dumps(fields[key])

        if "is_blocked" in fields:
            fields["is_blocked"] = int(fields["is_blocked"])

        if not current:
            cols = {
                "project_dir": self.project_dir, "is_blocked": 0, "block_reason": "",
                "next_step": "", "current_task": "", "progress": "[]",
                "recent_changes": "[]", "pending": "[]", "updated_at": now
            }
            cols.update(fields)
            cols["updated_at"] = now
            placeholders = ",".join("?" for _ in cols)
            col_names = ",".join(cols.keys())
            self.conn.execute(f"INSERT INTO session_state ({col_names}) VALUES ({placeholders})", list(cols.values()))
        else:
            if not fields:
                return self.get()
            fields["updated_at"] = now
            set_clause = ",".join(f"{k}=?" for k in fields)
            self.conn.execute(f"UPDATE session_state SET {set_clause} WHERE project_dir=?",
                              [*fields.values(), self.project_dir])

        self._commit()
        return self.get()
