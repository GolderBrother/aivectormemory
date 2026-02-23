import json
from datetime import datetime
from aivectormemory.config import USER_SCOPE_DIR

SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL DEFAULT 0
)"""

MEMORIES_TABLE = """
CREATE TABLE IF NOT EXISTS memories (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    scope TEXT NOT NULL DEFAULT 'project',
    source TEXT NOT NULL DEFAULT 'manual',
    project_dir TEXT NOT NULL DEFAULT '',
    session_id INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

VEC_MEMORIES_TABLE = """
CREATE VIRTUAL TABLE IF NOT EXISTS vec_memories USING vec0(
    id TEXT PRIMARY KEY,
    embedding FLOAT[384]
)"""

SESSION_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS session_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_dir TEXT NOT NULL DEFAULT '',
    is_blocked INTEGER NOT NULL DEFAULT 0,
    block_reason TEXT NOT NULL DEFAULT '',
    next_step TEXT NOT NULL DEFAULT '',
    current_task TEXT NOT NULL DEFAULT '',
    progress TEXT NOT NULL DEFAULT '[]',
    recent_changes TEXT NOT NULL DEFAULT '[]',
    pending TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL,
    UNIQUE(project_dir)
)"""

ISSUES_TABLE = """
CREATE TABLE IF NOT EXISTS issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_dir TEXT NOT NULL DEFAULT '',
    issue_number INTEGER NOT NULL,
    date TEXT NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    content TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    investigation TEXT NOT NULL DEFAULT '',
    root_cause TEXT NOT NULL DEFAULT '',
    solution TEXT NOT NULL DEFAULT '',
    files_changed TEXT NOT NULL DEFAULT '[]',
    test_result TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    feature_id TEXT NOT NULL DEFAULT '',
    parent_id INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

ISSUES_ARCHIVE_TABLE = """
CREATE TABLE IF NOT EXISTS issues_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_dir TEXT NOT NULL DEFAULT '',
    issue_number INTEGER NOT NULL,
    date TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    investigation TEXT NOT NULL DEFAULT '',
    root_cause TEXT NOT NULL DEFAULT '',
    solution TEXT NOT NULL DEFAULT '',
    files_changed TEXT NOT NULL DEFAULT '[]',
    test_result TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    feature_id TEXT NOT NULL DEFAULT '',
    parent_id INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT '',
    original_issue_id INTEGER NOT NULL DEFAULT 0,
    archived_at TEXT NOT NULL,
    created_at TEXT NOT NULL
)"""

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_memories_project ON memories(project_dir)",
    "CREATE INDEX IF NOT EXISTS idx_memories_scope ON memories(scope)",
    "CREATE INDEX IF NOT EXISTS idx_memories_tags ON memories(tags)",
    "CREATE INDEX IF NOT EXISTS idx_issues_date ON issues(date)",
    "CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status)",
    "CREATE INDEX IF NOT EXISTS idx_issues_project ON issues(project_dir)",
    "CREATE INDEX IF NOT EXISTS idx_issues_archive_project ON issues_archive(project_dir)",
    "CREATE INDEX IF NOT EXISTS idx_issues_archive_date ON issues_archive(date)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(project_dir)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_feature ON tasks(feature_id)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)",
    "CREATE INDEX IF NOT EXISTS idx_memories_source ON memories(source)",
    "CREATE INDEX IF NOT EXISTS idx_user_memories_tags ON user_memories(tags)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_archive_project ON tasks_archive(project_dir)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_archive_feature ON tasks_archive(feature_id)",
]

TASKS_TABLE = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_dir TEXT NOT NULL DEFAULT '',
    feature_id TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    sort_order INTEGER NOT NULL DEFAULT 0,
    parent_id INTEGER NOT NULL DEFAULT 0,
    task_type TEXT NOT NULL DEFAULT 'manual',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

USER_MEMORIES_TABLE = """
CREATE TABLE IF NOT EXISTS user_memories (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    source TEXT NOT NULL DEFAULT 'manual',
    session_id INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

VEC_USER_MEMORIES_TABLE = """
CREATE VIRTUAL TABLE IF NOT EXISTS vec_user_memories USING vec0(
    id TEXT PRIMARY KEY,
    embedding FLOAT[384]
)"""

VEC_ISSUES_ARCHIVE_TABLE = """
CREATE VIRTUAL TABLE IF NOT EXISTS vec_issues_archive USING vec0(
    id INTEGER PRIMARY KEY,
    embedding FLOAT[384]
)"""

TASKS_ARCHIVE_TABLE = """
CREATE TABLE IF NOT EXISTS tasks_archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_dir TEXT NOT NULL DEFAULT '',
    feature_id TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    sort_order INTEGER NOT NULL DEFAULT 0,
    parent_id INTEGER NOT NULL DEFAULT 0,
    task_type TEXT NOT NULL DEFAULT 'manual',
    metadata TEXT NOT NULL DEFAULT '{}',
    original_task_id INTEGER NOT NULL DEFAULT 0,
    archived_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

ALL_TABLES = [SCHEMA_VERSION_TABLE, MEMORIES_TABLE, VEC_MEMORIES_TABLE, SESSION_STATE_TABLE, ISSUES_TABLE, ISSUES_ARCHIVE_TABLE, TASKS_TABLE, USER_MEMORIES_TABLE, VEC_USER_MEMORIES_TABLE, VEC_ISSUES_ARCHIVE_TABLE, TASKS_ARCHIVE_TABLE]

CURRENT_SCHEMA_VERSION = 9


def _get_schema_version(conn) -> int:
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    if row:
        return row[0] if isinstance(row, tuple) else row["version"]
    conn.execute("INSERT INTO schema_version (version) VALUES (0)")
    conn.commit()
    return 0


def _set_schema_version(conn, version: int):
    conn.execute("UPDATE schema_version SET version=?", (version,))


def init_db(conn, engine=None):
    for sql in ALL_TABLES:
        conn.execute(sql)

    ver = _get_schema_version(conn)

    if ver < 1:
        # v1: 确保 memories 有 project_dir 列
        cols = {row[1] for row in conn.execute("PRAGMA table_info(memories)").fetchall()}
        if "project_dir" not in cols:
            conn.execute("ALTER TABLE memories ADD COLUMN project_dir TEXT NOT NULL DEFAULT ''")
        # v1: 旧版 issues 表中 archived 记录移到 issues_archive
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

    if ver < 2:
        # v2: session_state 加 last_session_id；issues/issues_archive 加 memory_id
        state_cols = {row[1] for row in conn.execute("PRAGMA table_info(session_state)").fetchall()}
        if "last_session_id" not in state_cols:
            conn.execute("ALTER TABLE session_state ADD COLUMN last_session_id INTEGER NOT NULL DEFAULT 0")
        issue_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues)").fetchall()}
        if "memory_id" not in issue_cols:
            conn.execute("ALTER TABLE issues ADD COLUMN memory_id TEXT NOT NULL DEFAULT ''")
        archive_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues_archive)").fetchall()}
        if "memory_id" not in archive_cols:
            conn.execute("ALTER TABLE issues_archive ADD COLUMN memory_id TEXT NOT NULL DEFAULT ''")

    if ver < 3:
        # v3: user scope 记忆的 project_dir 从空字符串改为 @user@
        conn.execute(
            "UPDATE memories SET project_dir=? WHERE project_dir='' AND scope='user'",
            (USER_SCOPE_DIR,)
        )

    if ver < 4:
        # v4: issues 表新增结构化字段
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

        # v4: issues_archive 表新增结构化字段 + status
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

        # v4: 创建 tasks 表（批次 B 的表，合并在 v4 迁移）
        conn.execute(TASKS_TABLE)

    if ver < 5:
        # v5: memories 表加 source 字段，区分手动记忆和 auto_save 碎片
        mem_cols = {row[1] for row in conn.execute("PRAGMA table_info(memories)").fetchall()}
        if "source" not in mem_cols:
            conn.execute("ALTER TABLE memories ADD COLUMN source TEXT NOT NULL DEFAULT 'manual'")
        # 回填：auto_save 产生的 5 个标签全部标记为 auto_save
        conn.execute(
            "UPDATE memories SET source='auto_save' WHERE source='manual' AND ("
            "tags LIKE '%\"modification\"%' OR tags LIKE '%\"todo\"%' OR "
            "tags LIKE '%\"decision\"%' OR tags LIKE '%\"pitfall\"%' OR "
            "tags LIKE '%\"preference\"%')"
        )

    if ver < 6:
        # v6: tasks 表新增 parent_id/task_type/metadata 字段（树形结构+任务分类）
        task_cols = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        for col, typ, default in [
            ("parent_id", "INTEGER", "0"),
            ("task_type", "TEXT", "'manual'"),
            ("metadata", "TEXT", "'{}'"),
        ]:
            if col not in task_cols:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {typ} NOT NULL DEFAULT {default}")

    if ver < 7:
        import sys
        # 1.5: 创建 3 张新表
        conn.execute(USER_MEMORIES_TABLE)
        conn.execute(VEC_USER_MEMORIES_TABLE)
        conn.execute(VEC_ISSUES_ARCHIVE_TABLE)

        # 1.6: scope=user 记录从 memories 迁移到 user_memories
        user_rows = conn.execute(
            "SELECT * FROM memories WHERE project_dir=?", (USER_SCOPE_DIR,)
        ).fetchall()
        for r in user_rows:
            conn.execute(
                "INSERT INTO user_memories (id, content, tags, source, session_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (r["id"], r["content"], r["tags"], r["source"] or "manual", r["session_id"], r["created_at"], r["updated_at"])
            )
            vec_row = conn.execute("SELECT embedding FROM vec_memories WHERE id=?", (r["id"],)).fetchone()
            if vec_row:
                conn.execute("INSERT INTO vec_user_memories (id, embedding) VALUES (?,?)", (r["id"], vec_row["embedding"]))
                conn.execute("DELETE FROM vec_memories WHERE id=?", (r["id"],))
            conn.execute("DELETE FROM memories WHERE id=?", (r["id"],))

        # 1.7: 删除 auto_save 碎片（source=auto_save 且标签非 preference）
        conn.execute(
            "DELETE FROM vec_memories WHERE id IN ("
            "  SELECT id FROM memories WHERE source='auto_save' AND tags NOT LIKE '%\"preference\"%'"
            ")"
        )
        conn.execute(
            "DELETE FROM memories WHERE source='auto_save' AND tags NOT LIKE '%\"preference\"%'"
        )

        # 1.8: 踩坑记录迁移到 issues_archive（含双标签处理）
        pitfall_rows = conn.execute(
            "SELECT * FROM memories WHERE tags LIKE '%\"踩坑\"%'"
        ).fetchall()
        now_ts = datetime.now().astimezone().isoformat()
        for r in pitfall_rows:
            content = r["content"]
            first_line = content.split("\n")[0].lstrip("# ").strip()[:100]
            created_date = r["created_at"][:10]
            tags_raw = r["tags"]
            tags_list = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
            has_project_knowledge = "项目知识" in tags_list

            cur = conn.execute(
                """INSERT INTO issues_archive (project_dir, issue_number, date, title, content, description,
                   root_cause, solution, status, archived_at, created_at)
                   VALUES (?,0,?,?,?,?,?,?,?,?,?)""",
                (r["project_dir"], created_date, first_line, "", content, "", "", "completed", now_ts, r["created_at"])
            )

            if has_project_knowledge:
                new_tags = [t for t in tags_list if t != "踩坑"]
                conn.execute("UPDATE memories SET tags=? WHERE id=?",
                             (json.dumps(new_tags, ensure_ascii=False), r["id"]))
            else:
                conn.execute("DELETE FROM vec_memories WHERE id=?", (r["id"],))
                conn.execute("DELETE FROM memories WHERE id=?", (r["id"],))

        # 1.9: 删除 ISSUE_STEPS 产生的系统任务
        conn.execute(
            "DELETE FROM tasks WHERE task_type='system' AND feature_id LIKE 'issue/%'"
        )

        # 1.10: issues_archive 批量生成 embedding（超时防护）
        archive_count = conn.execute("SELECT COUNT(*) FROM issues_archive").fetchone()[0]
        if engine and archive_count <= 50:
            archives = conn.execute(
                "SELECT id, title, description, root_cause, solution FROM issues_archive"
            ).fetchall()
            gen_count = 0
            for a in archives:
                existing = conn.execute(
                    "SELECT id FROM vec_issues_archive WHERE id=?", (a["id"],)
                ).fetchone()
                if existing:
                    continue
                text = f"{a['title']} {a['description'] or ''} {a['root_cause'] or ''} {a['solution'] or ''}"
                emb = engine.encode(text)
                conn.execute(
                    "INSERT INTO vec_issues_archive (id, embedding) VALUES (?,?)",
                    (a["id"], json.dumps(emb))
                )
                gen_count += 1
            print(f"[aivectormemory] v7 migration: generated embeddings for {gen_count} archived issues", file=sys.stderr)
        elif archive_count > 50:
            print(f"[aivectormemory] v7 migration: skipped embedding generation for {archive_count} archived issues (>50, lazy loading)", file=sys.stderr)

    if ver < 8:
        # v8: issues_archive 加 original_issue_id 字段
        archive_cols = {row[1] for row in conn.execute("PRAGMA table_info(issues_archive)").fetchall()}
        if "original_issue_id" not in archive_cols:
            conn.execute("ALTER TABLE issues_archive ADD COLUMN original_issue_id INTEGER NOT NULL DEFAULT 0")

    if ver < 9:
        # v9: 创建 tasks_archive 表
        conn.execute(TASKS_ARCHIVE_TABLE)

    for sql in INDEXES:
        conn.execute(sql)

    if ver < CURRENT_SCHEMA_VERSION:
        _set_schema_version(conn, CURRENT_SCHEMA_VERSION)
    conn.commit()
