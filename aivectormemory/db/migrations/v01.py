"""v1: memories 加 project_dir；旧版 issues archived 记录迁移到 issues_archive"""


def upgrade(conn, **_):
    cols = {row[1] for row in conn.execute("PRAGMA table_info(memories)").fetchall()}
    if "project_dir" not in cols:
        conn.execute("ALTER TABLE memories ADD COLUMN project_dir TEXT NOT NULL DEFAULT ''")
    issue_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues)").fetchall()}
    if "archive_content" in issue_cols:
        rows = conn.execute("SELECT * FROM issues WHERE status IN ('archived', 'migrated')").fetchall()
        for r in rows:
            conn.execute(
                "INSERT INTO issues_archive (project_dir, issue_number, date, title, content, archived_at, created_at) VALUES (?,?,?,?,?,?,?)",
                (r["project_dir"], r["issue_number"], r["date"], r["title"], r["content"], r["updated_at"], r["created_at"])
            )
            conn.execute("DELETE FROM issues WHERE id=?", (r["id"],))
        conn.execute("CREATE TABLE IF NOT EXISTS issues_new (id INTEGER PRIMARY KEY AUTOINCREMENT, project_dir TEXT NOT NULL DEFAULT '', issue_number INTEGER NOT NULL, date TEXT NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', content TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
        conn.execute("INSERT INTO issues_new SELECT id, project_dir, issue_number, date, title, status, content, created_at, updated_at FROM issues")
        conn.execute("DROP TABLE issues")
        conn.execute("ALTER TABLE issues_new RENAME TO issues")
