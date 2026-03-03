"""v2: session_state 加 last_session_id；issues/issues_archive 加 memory_id"""


def upgrade(conn, **_):
    state_cols = {row[1] for row in conn.execute("PRAGMA table_info(session_state)").fetchall()}
    if "last_session_id" not in state_cols:
        conn.execute("ALTER TABLE session_state ADD COLUMN last_session_id INTEGER NOT NULL DEFAULT 0")
    issue_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues)").fetchall()}
    if "memory_id" not in issue_cols:
        conn.execute("ALTER TABLE issues ADD COLUMN memory_id TEXT NOT NULL DEFAULT ''")
    archive_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues_archive)").fetchall()}
    if "memory_id" not in archive_cols:
        conn.execute("ALTER TABLE issues_archive ADD COLUMN memory_id TEXT NOT NULL DEFAULT ''")
