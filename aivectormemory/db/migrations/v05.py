"""v5: memories 加 source 字段，回填 auto_save 标记"""


def upgrade(conn, **_):
    mem_cols = {row[1] for row in conn.execute("PRAGMA table_info(memories)").fetchall()}
    if "source" not in mem_cols:
        conn.execute("ALTER TABLE memories ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'")
    conn.execute(
        "UPDATE memories SET source='auto_save' WHERE source='manual' AND ("
        "tags LIKE '%\"modification\"%' OR tags LIKE '%\"todo\"%' OR "
        "tags LIKE '%\"decision\"%' OR tags LIKE '%\"pitfall\"%' OR "
        "tags LIKE '%\"preference\"%')"
    )
