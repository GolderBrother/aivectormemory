"""v6: tasks 表新增 parent_id/task_type/metadata 字段"""


def upgrade(conn, **_):
    task_cols = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    for col, typ, default in [
        ("parent_id", "INTEGER", "0"),
        ("task_type", "TEXT", "'manual'"),
        ("metadata", "TEXT", "'{}'"),
    ]:
        if col not in task_cols:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {typ} NOT NULL DEFAULT {default}")
