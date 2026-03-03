"""v8: issues_archive 加 original_issue_id 字段"""


def upgrade(conn, **_):
    archive_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues_archive)").fetchall()}
    if "original_issue_id" not in archive_cols:
        conn.execute("ALTER TABLE issues_archive ADD COLUMN original_issue_id INTEGER NOT NULL DEFAULT 0")
