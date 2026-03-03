"""v4: issues/issues_archive 新增结构化字段；创建 tasks 表"""
from aivectormemory.db.schema import TASKS_TABLE


def upgrade(conn, **_):
    issue_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues)").fetchall()}
    for col, typ, default in [
        ("description", "TEXT", "''"),
        ("investigation", "TEXT", "''"),
        ("root_cause", "TEXT", "''"),
        ("solution", "TEXT", "''"),
        ("files_changed", "TEXT", "'[]'"),
        ("test_result", "TEXT", "''"),
        ("notes", "TEXT", "''"),
        ("feature_id", "TEXT", "''"),
        ("parent_id", "INTEGER", "0"),
    ]:
        if col not in issue_cols:
            conn.execute(f"ALTER TABLE issues ADD COLUMN {col} {typ} NOT NULL DEFAULT {default}")

    archive_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues_archive)").fetchall()}
    for col, typ, default in [
        ("description", "TEXT", "''"),
        ("investigation", "TEXT", "''"),
        ("root_cause", "TEXT", "''"),
        ("solution", "TEXT", "''"),
        ("files_changed", "TEXT", "'[]'"),
        ("test_result", "TEXT", "''"),
        ("notes", "TEXT", "''"),
        ("feature_id", "TEXT", "''"),
        ("parent_id", "INTEGER", "0"),
        ("status", "TEXT", "''"),
    ]:
        if col not in archive_cols:
            conn.execute(f"ALTER TABLE issues_archive ADD COLUMN {col} {typ} NOT NULL DEFAULT {default}")

    conn.execute(TASKS_TABLE)
